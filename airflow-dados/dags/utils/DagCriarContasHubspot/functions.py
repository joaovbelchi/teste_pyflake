import random
from utils.OneGraph.drive import OneGraphDrive
from utils.bucket_names import get_bucket_name
from utils.secrets_manager import get_secret_value
from hubspot.crm.contacts import SimplePublicObjectInput, PublicObjectSearchRequest
from hubspot.crm.companies import ApiException
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import unicodedata
import requests
import hubspot
import time
import json
import re
from deltalake import DeltaTable
import os


def define_mes_path(data: datetime) -> str:
    """Esta função converte uma data para um formato de string específico que é usada para nomear as pastas que guardam dados mês a mês no Drive.

    Args:
        data (datetime): Data.

    Returns:
        str: Mês da data no formato do nome das pastas do Drive.
    """
    # Solução com locale: só funciona se o pacote de português do Brasil estiver instalado no sistema operacional
    # Por hora usaremos a alternativa reescrita com um dict de mapeamentos
    # locale.setlocale(locale.LC_TIME, 'pt_BR.utf-8')
    # return f"{data.month:02d} - {data.strftime('%B').capitalize()}"

    dict_meses = {
        1: "01 - Janeiro",
        2: "02 - Fevereiro",
        3: "03 - Março",
        4: "04 - Abril",
        5: "05 - Maio",
        6: "06 - Junho",
        7: "07 - Julho",
        8: "08 - Agosto",
        9: "09 - Setembro",
        10: "10 - Outubro",
        11: "11 - Novembro",
        12: "12 - Dezembro",
    }
    return dict_meses[data.month]


def normaliza_caracteres_unicode(string: str) -> str:
    """Normaliza caracteres unicode para garantir que operações de junção por campos de texto não falhem.
    Uma boa referência para o tema pode ser encontrada aqui: https://learn.microsoft.com/pt-br/windows/win32/intl/using-unicode-normalization-to-represent-strings

    Args:
        string (str): String a ser normalizada.

    Returns:
        str: String após normalização.
    """
    return (
        unicodedata.normalize("NFKD", string).encode("ASCII", "ignore").decode().strip()
    )


def carrega_base_contas_btg(bucket: str, hoje: pd.Timestamp) -> pd.DataFrame:
    """Carrega base de contas dos clientes da One oriunda do BTG.
    A base está no bucket do S3 (em DeltaLake) e contém informações cadastrais de cada um dos clientes,
    bem como algumas informações como data do primeiro investimento
    e PL declarado pelo cliente ao criar a conta.

    Args:
        bucket (str): Bucket no S3 onde está a base (atualmente carregamos da bronze).
        hoje (pd.Timestamp): Data que será usada para filtrar a versão mais recente dessa base em Delta.

    Returns:
        df_base_contas_btg (pd.DataFrame): Dataframe pandas com o resultado já tratado dessa base.
    """
    table_base_contas_btg = DeltaTable(
        f"s3://{bucket}/btg/webhooks/downloads/dados_cadastrais/"
    )
    df_base_contas_btg = table_base_contas_btg.to_pandas(
        partitions=[
            ("ano_particao", "=", str(hoje.year)),
            ("mes_particao", "=", str(hoje.month)),
        ]
    )

    # tratamentos
    df_base_contas_btg = df_base_contas_btg.sort_values(
        "timestamp_escrita_bronze", ascending=False
    )
    df_base_contas_btg = df_base_contas_btg.drop_duplicates(
        subset=["nr_conta"], keep="first"
    )
    df_base_contas_btg = df_base_contas_btg.rename(
        columns={
            "nr_conta": "carteira",
            "dt_nascimento": "data_nascimento",
            "dt_abertura": "data_abertura",
            "dt_encerramento": "data_encerramento",
            "vl_pl_declarado": "pl_declarado",
            "dt_primeiro_investimento": "data_primeiro_investimento",
        }
    )
    df_base_contas_btg = df_base_contas_btg.dropna(subset=["carteira"])
    df_base_contas_btg = df_base_contas_btg.astype({"carteira": "int"})
    df_base_contas_btg[
        [
            "data_nascimento",
            "data_abertura",
            "data_encerramento",
            "data_primeiro_investimento",
        ]
    ] = df_base_contas_btg[
        [
            "data_nascimento",
            "data_abertura",
            "data_encerramento",
            "data_primeiro_investimento",
        ]
    ].apply(
        lambda x: x.dt.strftime("%Y-%m-%d")
    )

    df_base_contas_btg["carteira"] = df_base_contas_btg["carteira"].astype(str)
    df_base_contas_btg["suitability"] = df_base_contas_btg["suitability"].fillna(
        "nao_respondeu"
    )

    filtro = df_base_contas_btg["suitability"].isin(
        ["CONSERVADOR", "MODERADO", "SOFISTICADO", "nao_respondeu"]
    )
    df_base_contas_btg.loc[~filtro, "suitability"] = ""

    df_base_contas_btg["celular"] = (
        df_base_contas_btg["celular"]
        .fillna(0)
        .astype("str")
        .str.replace(".0", "", regex=False)
    )
    df_base_contas_btg["status"] = df_base_contas_btg["status"].replace(
        {
            "ATIVA": "ativa",
            "BLOQUEIO TOTAL": "bloqueio_total",
            "BLOQUEIO PARCIAL": "bloqueio_parcial",
            "ENCERRADA": "encerrada",
            "EM PROCESSO DE ENCERRAMENTO": "em_processo_de_encerramento",
        }
    )

    df_base_contas_btg["email"] = (
        df_base_contas_btg["email"].fillna("").str.upper().str.split(",").str[0]
    )
    df_base_contas_btg["pl_declarado"] = df_base_contas_btg["pl_declarado"].fillna(0)

    df_base_contas_btg[
        ["endereco_cidade", "endereco_estado", "endereco_cep", "suitability"]
    ] = df_base_contas_btg[
        ["endereco_cidade", "endereco_estado", "endereco_cep", "suitability"]
    ].apply(
        lambda x: x.str.upper()
    )

    df_base_contas_btg = df_base_contas_btg[
        [
            "carteira",
            "nome_completo",
            "data_nascimento",
            "celular",
            "email",
            "endereco_cidade",
            "endereco_estado",
            "endereco_cep",
            "suitability",
            "tipo_cliente",
            "status",
            "data_abertura",
            "data_encerramento",
            "pl_declarado",
            "data_primeiro_investimento",
        ]
    ]

    df_base_contas_btg = df_base_contas_btg.drop_duplicates(["carteira"])

    return df_base_contas_btg


def carrega_base_assessores(bucket: str, hoje: pd.Timestamp) -> pd.DataFrame:
    """Carrega a base do time de assessores de investimento da One que está em DeltaLake no S3 na camada bronze.
    Essa base contém os nomes e códigos desses assessores, associados às contas que cada um assessora.

    Args:
        bucket (str): Bucket no S3 onde está a base (atualmente carregamos da bronze).
        hoje (pd.Timestamp): Data que será usada para filtrar a versão mais recente dessa base em Delta.

    Returns:
        df_contas_por_assessor (pd.DataFrame): Dataframe pandas com resultado já tratado dessa base.
    """
    table_contas_por_assessor = DeltaTable(
        f"s3://{bucket}/btg/webhooks/downloads/contas_por_assessor/"
    )
    df_contas_por_assessor = table_contas_por_assessor.to_pandas()

    df_contas_por_assessor = table_contas_por_assessor.to_pandas(
        partitions=[
            ("ano_particao", "=", str(hoje.year)),
            ("mes_particao", "=", str(hoje.month)),
        ]
    )

    df_contas_por_assessor = df_contas_por_assessor.loc[
        :, ["account", "sg_cge", "username", "timestamp_escrita_bronze", "bond_date"]
    ]

    # Remove UTC da coluna de data
    df_contas_por_assessor["bond_date"] = pd.to_datetime(
        df_contas_por_assessor["bond_date"], utc=True, format="mixed"
    )
    df_contas_por_assessor["bond_date"] = df_contas_por_assessor[
        "bond_date"
    ].dt.tz_localize(None)

    df_contas_por_assessor = df_contas_por_assessor.astype(
        {"account": "str", "sg_cge": "str", "username": "str"}
    )

    # São selecionadas as informações mais recentes de cada conta da base.
    df_contas_por_assessor = df_contas_por_assessor.sort_values(
        "timestamp_escrita_bronze", ascending=False
    )
    df_contas_por_assessor = df_contas_por_assessor.drop_duplicates(
        subset=["account"], keep="first"
    )

    # Realiza o tratamento da coluna username para permir o cruzamento desta base com a base sócios.
    # O tratamento coloca todas os caracteres em caixa alta e remove acentos e caracteres especiais.
    df_contas_por_assessor["username"] = df_contas_por_assessor["username"].str.upper()

    df_contas_por_assessor["username"] = (
        df_contas_por_assessor["username"]
        .apply(normaliza_caracteres_unicode)
        .replace("Ç", "C")
    )

    # Carregar base de-para dos nomes dos assessores
    # Como a padronização dos nomes na base do BTG é diferente da feita nas bases da One
    # foi criado um arquivo de-para com a relação do nome BTG para o nome One Investimentos.
    df_de_para_nomes = pd.read_csv(
        "s3://prd-dados-lake-gold-oneinvest/api/one/de_para_criar_contas/de_para_nomes_socios.csv",
        sep=";",
    )
    dict_de_para_nomes = df_de_para_nomes.set_index("nome_btg")[
        "nome_one_investimentos"
    ].to_dict()

    df_contas_por_assessor["username"] = df_contas_por_assessor["username"].replace(
        dict_de_para_nomes
    )

    data_agora = datetime.now().strftime("%Y-%m-%d")
    df_contas_por_assessor.to_csv(
        f"s3://one-teste-logs/criar_contas/assessor_contas_{data_agora}.csv", sep=";"
    )

    return df_contas_por_assessor


def carrega_base_contatos_hubspot(bucket: str, hoje: pd.Timestamp) -> pd.DataFrame:
    """Carrega base com os contatos da One cadastrados no Hubspot (atualmente na camada bronze e não está em Delta).
    'Contatos' é um domínio de negócios da One engloba dados de pessoas que foram de alguma forma abordados pela empresa,
    sejam elas clientes ou não, registrando informações delas ao longo do ciclo de contatos.

    Args:
        bucket (str): Bucket no S3 onde está a base (atualmente carregamos da bronze).
        hoje (pd.Timestamp): Data que será usada para filtrar a versão mais recente dessa base.

    Returns:
        df_contatos_hubspot (pd.DataFrame): Dataframe pandas com o resultado bruto dessa base.
    """
    df_contatos_hubspot = pd.read_parquet(
        f's3://{bucket}/api/hubspot/tb_contatos/{hoje.strftime("%Y-%m-%d")}/contatos.parquet'
    )

    return df_contatos_hubspot


def carrega_base_socios(bucket: str) -> pd.DataFrame:
    """Carrega base com dados cadastrais de todos os sócios da One Investimentos (não está em Delta).


    Args:
        bucket (str): Bucket no S3 onde está a base (atualmente carregamos da bronze).

    Returns:
        df_socios (pd.DataFrame): Dataframe pandas já tratado para uso com o resultado dessa base.
    """
    SOCIOS_COLUMNS = [
        "Nome",
        "Id",
        "Departamento",
        "Área",
        "Team Leader",
        "Gestor / Head",
        "business_leader",
        "E-mail",
        "Nome_bd",
    ]

    df_socios = pd.read_parquet(
        f"s3://{bucket}/api/one/socios/socios.parquet", columns=SOCIOS_COLUMNS
    )
    df_socios["E-mail"] = df_socios["E-mail"].str.split(pat="@").str[0]

    # Existem alguns casos de duplicatas na base, estes serão removidos.
    df_socios = df_socios.drop_duplicates("Nome_bd")

    # Realiza o tratamento de strings na coluna Nome para permitir o cruzamento
    # com a base contas por assessor.
    df_socios["Departamento"] = df_socios["Departamento"].str.strip(" ")
    df_socios["Área"] = df_socios["Área"].str.strip(" ")

    df_socios["Nome"] = (
        df_socios["Nome"].apply(normaliza_caracteres_unicode).replace("Ç", "C")
    )

    return df_socios


def carrega_dados_receita_offshore(
    drive_microsoft: OneGraphDrive, mes_passado: pd.Timestamp
) -> tuple:
    """Carrega os dados de receita das contas contas offshore de clientes da One Investimentos que são consolidados ainda no OneDrive.

    Args:
        drive_microsoft (OneGraphDrive): Cliente Microsoft Graph usado para interagir com o Drive baixando arquivos e enviando e-mails.
        mes_passado (pd.Timestamp): Data relativo ao mês anterior ao da criação das contas, que é a periodicidade de geração desses dados (M-1).

    Returns:
        (sucesso, df_receita_offshore) (tuple): A função retorna uma tupla com dois elementos. O primeiro indica o sucesso em carregar os dados do OneDrive, e o segundo o resultado dessa tentativa, que pode ser um Dataframe pandas com os dados ou um valor None em caso de insucesso.
    """
    lista_pastas_receita_offshore = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "Receita",
        f"{mes_passado.year}",
        f"{define_mes_path(mes_passado)}",
        "Final",
        "Relatório Comissão Offshore - Eins.xlsx",
    ]
    try:
        df_receita_offshore = drive_microsoft.download_file(
            lista_pastas_receita_offshore,
            to_pandas=True,
            **dict(sheet_name="AuC", skiprows=4),
        )

        if isinstance(df_receita_offshore, pd.DataFrame):
            df_receita_offshore[["Cliente", "Referral"]] = df_receita_offshore[
                ["Cliente", "Referral"]
            ].astype(str)
            df_receita_offshore.rename(columns={"Referral": "Nome"}, inplace=True)
            df_receita_offshore["Cliente"] = df_receita_offshore["Cliente"].str.upper()
            df_receita_offshore["ClienteAuxiliar"] = (
                df_receita_offshore["Cliente"]
                .replace("[-./]", "", regex=True)
                .str.replace(" ", "")
            )
            df_receita_offshore[["ClienteAuxiliar", "Nome"]] = df_receita_offshore[
                ["ClienteAuxiliar", "Nome"]
            ].apply(lambda col: col.apply(normaliza_caracteres_unicode))

            return True, df_receita_offshore
        else:
            return False, pd.DataFrame()

    except Exception as e:
        print(e)
        return False, pd.DataFrame()


def carrega_dados_auc_offshore(
    drive_microsoft: OneGraphDrive,
) -> tuple:
    """Carrega os dados de AuC das contas contas offshore de clientes da One Investimentos que são consolidados ainda no OneDrive.

    Args:
        drive_microsoft (OneGraphDrive): Cliente Microsoft Graph usado para interagir com o Drive baixando arquivos e enviando e-mails.

    Returns:
        (sucesso, df_receita_offshore) (tuple): A função retorna uma tupla com dois elementos. O primeiro indica o sucesso em carregar os dados do OneDrive, e o segundo o resultado dessa tentativa, que pode ser um Dataframe com os dados ou um valor None em caso de insucesso.
    """
    lista_pastas_auc_offshore = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "Base_OffShore.xlsx",
    ]
    try:
        df_auc_offshore = drive_microsoft.download_file(
            lista_pastas_auc_offshore, to_pandas=True
        )

        if isinstance(df_auc_offshore, pd.DataFrame):
            df_auc_offshore[["Acc Number", "Office"]] = df_auc_offshore[
                ["Acc Number", "Office"]
            ].astype(str)
            df_auc_offshore.rename(columns={"Office": "Cliente"}, inplace=True)
            df_auc_offshore["Cliente"] = df_auc_offshore["Cliente"].str.upper()
            df_auc_offshore["ClienteAuxiliar"] = (
                df_auc_offshore["Cliente"]
                .replace("[-./]", "", regex=True)
                .str.replace(" ", "")
            )
            df_auc_offshore["ClienteAuxiliar"] = df_auc_offshore[
                "ClienteAuxiliar"
            ].apply(normaliza_caracteres_unicode)
            return True, df_auc_offshore
        else:
            return False, pd.DataFrame()

    except Exception as e:
        print(e)
        return False, pd.DataFrame()


def carrega_dados_contas_hubspot(
    cliente: hubspot.Client, df_base_contas_btg: pd.DataFrame
) -> tuple:
    """Usa a base de cadastral de contas dos clientes da One oriunda do BTG para acessar as informações desses mesmos clientes registradas no Hubspot.

    Args:
        cliente (hubspot.Client): Cliente que interage com a API do Hubspot (CRM).
        df_base_contas_btg (pd.DataFrame): Dataframe com dados de clientes e suas contas na One.

    Returns:
        (df_contas_hubspot, df_contas_hubspot_cp) (tuple): Dois Dataframes com os dados extraídos do Hubspot, com dados brutos e outro com dados tratados, que serão usados de maneiras distintas nos processos de criação de contas.
    """
    propriedades_hubspot = [
        "account_conta_btg",
        "account_name",
        "data_de_nascimento",
        "account_phone",
        "account_email",
        "cidade",
        "estado",
        "cep",
        "account_profile",
        "account_type",
        "account_status",
        "account_open_date",
        "data_de_encerramento",
        "account_pl_declared",
        "data_do_primeiro_investimento",
        "account_comercial",
        "data_de_vinculacao",
    ]

    retorno_api_hubspot = cliente.crm.objects.get_all(
        "2-7418328", properties=propriedades_hubspot, associations=["0-1", "0-2"]
    )
    lista_contas_api_hubspot = [
        dict_conta_hubspot.to_dict() for dict_conta_hubspot in retorno_api_hubspot
    ]
    df_contas_hubspot = pd.DataFrame(lista_contas_api_hubspot)

    # tratementos
    df_properties_contas_hubspot = pd.DataFrame(
        df_contas_hubspot["properties"].tolist()
    )
    colunas_properties_hubspot = df_properties_contas_hubspot.columns
    df_contas_hubspot[colunas_properties_hubspot] = pd.DataFrame(
        df_contas_hubspot["properties"].tolist()
    )
    df_contas_hubspot["Link"] = (
        "https://app.hubspot.com/contacts/21512517/record/2-7418328/"
        + df_contas_hubspot["id"]
    )
    df_contas_hubspot_cp = df_contas_hubspot.copy()

    df_contas_hubspot = df_contas_hubspot[
        [
            "account_conta_btg",
            "account_name",
            "data_de_nascimento",
            "account_phone",
            "account_email",
            "cidade",
            "estado",
            "cep",
            "account_profile",
            "account_type",
            "account_status",
            "account_open_date",
            "data_de_encerramento",
            "account_pl_declared",
            "data_do_primeiro_investimento",
            "id",
            "Link",
            "data_de_vinculacao",
        ]
    ]

    # tentar reescrever isto como um dicionario de_para btg > hubspot pode ser melhor. o risco é alguem mexer no nome da coluna em algum lugar e o dicionaria quebrar por isso
    df_contas_hubspot.columns = np.append(
        df_base_contas_btg.columns, ["id", "Link", "data_de_vinculacao"]
    )

    df_contas_hubspot[
        ["endereco_cidade", "endereco_estado", "endereco_cep", "suitability", "email"]
    ] = df_contas_hubspot[
        ["endereco_cidade", "endereco_estado", "endereco_cep", "suitability", "email"]
    ].apply(
        lambda x: x.str.upper()
    )
    df_contas_hubspot["email"] = df_contas_hubspot["email"].fillna("")
    df_contas_hubspot["data_de_vinculacao"] = df_contas_hubspot[
        "data_de_vinculacao"
    ].fillna(np.nan)

    return df_contas_hubspot, df_contas_hubspot_cp


def estabelece_contas_sem_data_vinculacao(
    df_contas_hubspot: pd.DataFrame, df_base_contas_btg: pd.DataFrame
) -> pd.DataFrame:
    """Usa os dados do Hubspot e do BTG para encontrar as contas que possuem dados no Hubspot criadas por usuários (sem data de vinculação no CRM).

    Args:
        df_contas_hubspot (pd.DataFrame): Dataframe com dados dos clientes na One registrados no Hubspot.
        df_base_contas_btg (pd.DataFrame): Dataframe com dados das contas dos clientes trazidos do BTG.

    Returns:
        df_contas_criadas_por_usuarios (pd.DataFrame): _description_
    """
    filtro_contas_btg = df_contas_hubspot["carteira"].isin(
        df_base_contas_btg["carteira"].unique()
    )
    filtro_contas_s_data = df_contas_hubspot["data_de_vinculacao"].isna()
    df_contas_criadas_por_usuarios = df_contas_hubspot[
        filtro_contas_btg & filtro_contas_s_data
    ]
    df_contas_criadas_por_usuarios = df_contas_criadas_por_usuarios["carteira"]

    return df_contas_criadas_por_usuarios


def estabelece_mudancas_contas_novas_entre_btg_hubspot(
    df_contas_hubspot: pd.DataFrame,
    df_base_contas_btg: pd.DataFrame,
    df_contas_criadas_por_usuarios: pd.DataFrame,
) -> pd.DataFrame:
    """Estabelece quais contas de clientes da One possuem dados divergentes entre o Hubspot e o BTG.

    Args:
        df_contas_hubspot (pd.DataFrame): Dataframe com dados das contas no Hubspot.
        df_base_contas_btg (pd.DataFrame): Dataframe com dados das contas no BTG.
        df_contas_criadas_por_usuarios (pd.DataFrame): Dataframe com as contas criadas por usuários (sem data de vinculação.)

    Returns:
        df_mudancas_contas_novas (pd.DataFrame): Dataframe com contas novas e contas que precisam ter dados atualizados no Hubspot.
    """
    df_mudancas_contas_novas = pd.concat(
        [df_contas_hubspot, df_base_contas_btg], axis=0
    )
    df_mudancas_contas_novas = df_mudancas_contas_novas[
        df_mudancas_contas_novas["carteira"].isin(
            df_base_contas_btg["carteira"].unique()
        )
    ]
    df_mudancas_contas_novas["suitability"] = df_mudancas_contas_novas[
        "suitability"
    ].str.lower()
    df_mudancas_contas_novas["tipo_cliente"] = df_mudancas_contas_novas[
        "tipo_cliente"
    ].str.lower()
    df_mudancas_contas_novas["pl_declarado"] = df_mudancas_contas_novas[
        "pl_declarado"
    ].astype(float)
    df_mudancas_contas_novas["data_primeiro_investimento"] = pd.to_datetime(
        df_mudancas_contas_novas["data_primeiro_investimento"]
    ).dt.strftime("%Y-%m-%d")
    df_mudancas_contas_novas["data_abertura"] = pd.to_datetime(
        df_mudancas_contas_novas["data_abertura"]
    ).dt.strftime("%Y-%m-%d")
    df_mudancas_contas_novas["data_encerramento"] = pd.to_datetime(
        df_mudancas_contas_novas["data_encerramento"]
    ).dt.strftime("%Y-%m-%d")
    df_mudancas_contas_novas["data_nascimento"] = pd.to_datetime(
        df_mudancas_contas_novas["data_nascimento"]
    ).dt.strftime("%Y-%m-%d")
    df_mudancas_contas_novas["email"] = df_mudancas_contas_novas["email"].replace(
        "", np.nan
    )
    df_mudancas_contas_novas["pl_declarado"] = df_mudancas_contas_novas[
        "pl_declarado"
    ].round(2)
    df_mudancas_contas_novas["email"] = df_mudancas_contas_novas["email"].str.lower()
    df_mudancas_contas_novas = df_mudancas_contas_novas.fillna("")
    colunas_criterio_duplicatas = df_base_contas_btg.columns
    colunas_criterio_duplicatas = colunas_criterio_duplicatas.drop("data_nascimento")
    df_mudancas_contas_novas = df_mudancas_contas_novas[
        ~df_mudancas_contas_novas.duplicated(
            subset=colunas_criterio_duplicatas, keep=False
        )
    ].drop_duplicates(["carteira"], keep="last")

    # Garante que as contas que não possuem data_de_vinculacao serão reavaliadas
    filtro = df_base_contas_btg["carteira"].isin(df_contas_criadas_por_usuarios)
    df_data_vinculacao = df_base_contas_btg[filtro]
    df_mudancas_contas_novas = pd.concat(
        [df_mudancas_contas_novas, df_data_vinculacao], axis=0
    )
    df_mudancas_contas_novas = df_mudancas_contas_novas.drop_duplicates(
        "carteira", keep="first"
    )
    df_mudancas_contas_novas = df_mudancas_contas_novas.drop(
        "data_de_vinculacao", axis=1
    )
    df_mudancas_contas_novas = df_mudancas_contas_novas.fillna("")

    return df_mudancas_contas_novas


def estabelece_df_nomes_assessores(
    df_contas_por_assessor: pd.DataFrame,
) -> pd.DataFrame:
    """Filtra os nomes dos assessores e seus respectivos código a partir da base de contas assessoradas da One.

    Args:
        df_contas_por_assessor (pd.DataFrame): Dataframe com dados de contas assessoradas por cada assessor da One.

    Returns:
        df_nomes_assessores (pd.DataFrame): Dataframe com nomes e códigos dos assessores.
    """
    df_nomes_assessores = df_contas_por_assessor[["sg_cge", "username"]]
    df_nomes_assessores = df_nomes_assessores.drop_duplicates()
    df_nomes_assessores = df_nomes_assessores.rename(columns={"sg_cge": "cge"})
    return df_nomes_assessores


def relaciona_id_btg_id_hubspot_base_socios(
    df_socios: pd.DataFrame, df_nomes_assessores: pd.DataFrame
) -> pd.DataFrame:
    """Usa a base de sócios e os nomes dos assessores para estabelecer o ID do Hubspot de cada um dos assessores da One.

    Args:
        df_socios (pd.DataFrame): Dataframe com dados dos sócios da One
        df_nomes_assessores (pd.DataFrame): Dataframe com nomes e códigos dos assessores da One.

    Returns:
        df_socios_com_ids (pd.DataFrame): Dataframe com dos assessores com seus IDs no Hubspot.
    """
    df_socios = df_socios.merge(
        df_nomes_assessores, left_on="Nome", right_on="username", how="left"
    ).drop(columns=["username"])

    df_socios.loc[
        ~(
            (df_socios["Departamento"].str.strip(" ") == "GROWTH")
            & (df_socios["Área"].str.strip(" ") == "COMERCIAL")
        ),
        "cge",
    ] = np.nan

    return df_socios


def relaciona_id_btg_id_hubspot_contas(
    df_socios_com_ids: pd.DataFrame,
    df_contas_por_assessor: pd.DataFrame,
    df_mudancas_contas_novas: pd.DataFrame,
) -> pd.DataFrame:
    """Relaciona as contas dos clientes no BTG com os IDs desses clientes no Hubspot, bem como os assessores de cada uma dessas contas.

    Args:
        df_socios_com_ids (pd.DataFrame): Dataframe com os IDs dos assessores no Hubspot.
        df_contas_por_assessor (pd.DataFrame): Dataframe com as contas assessoradas por cada assessor.
        df_mudancas_contas_novas (pd.DataFrame): Dataframe com as contas que precisam ser criadas ou alteradas.

    Returns:
        df_contas_por_assessor_ids_emails (pd.DataFrame): Dataframe com resultado descrito na função, que será usado para disparar e-mails.
    """
    df_id_email = df_socios_com_ids[["cge", "Id"]].dropna()
    df_id_email["cge"] = df_id_email["cge"].astype(int).astype(str)
    df_contas_por_assessor = df_contas_por_assessor.merge(
        df_id_email, how="left", left_on="sg_cge", right_on="cge"
    )
    df_contas_por_assessor["account"] = df_contas_por_assessor["account"].astype(str)

    data_agora = datetime.now().strftime("%Y-%m-%d")
    df_contas_por_assessor[
        df_contas_por_assessor["account"].isin(df_mudancas_contas_novas["carteira"])
    ].to_csv(
        f"s3://one-teste-logs/criar_contas/assessor_contas_novas_e_modificadas_com_id_hub_{data_agora}.csv",
        sep=";",
    )

    return df_contas_por_assessor


def estabelece_contas_sem_comercial(df_contas_hubspot_cp: pd.DataFrame) -> pd.DataFrame:
    """Usa os dados brutos de contas no Hubspot para estabelecer quais contas ainda estão sem um assessor.

    Args:
        df_contas_hubspot_cp (pd.DataFrame): Dataframe com dados brutos de contas no Hubspot.

    Returns:
        df_contas_sem_comercial (pd.DataFrame): Dataframe das contas sem assessor.
    """
    df_contas_sem_comercial = df_contas_hubspot_cp[
        df_contas_hubspot_cp["account_comercial"].isnull()
    ][["account_conta_btg", "Link"]]
    df_contas_sem_comercial.columns = ["Conta BTG", "Link"]

    return df_contas_sem_comercial


def estabelece_contas_sem_associacao(
    df_contas_hubspot_cp: pd.DataFrame,
) -> pd.DataFrame:
    """Usa os dados brutos de contas no Hubspot para estabelecer quais contas ainda estão sem associoção.

    Args:
        df_contas_hubspot_cp (pd.DataFrame): Dataframe com dados brutos de contas no Hubspot.

    Returns:
        df_contas_sem_associacao (pd.DataFrame): Dataframe das contas sem associação.
    """
    df_contas_sem_associacao = df_contas_hubspot_cp[
        (df_contas_hubspot_cp["associations"].isnull())
        & ~(df_contas_hubspot_cp["account_conta_btg"].str.contains("ONE"))
        & (df_contas_hubspot_cp["account_comercial"] != "225957674")
    ][["account_conta_btg", "Link", "account_comercial"]]

    df_contas_sem_associacao.columns = ["Conta BTG", "Link", "id_assessor_hubspot"]

    return df_contas_sem_associacao


def limpa_numero_celular(numero_celular: str) -> str:
    """Filtra caracteres alfabéticos e especiais e retorna os 11 últimos
    dígitos do número de telefone recebido. Caso o número original possua
    menos que 11 caracteres retorna np.nan.

    Args:
        numero_celular (str): String com o número de celular a ser tratado.

    Returns:
        numero_celular_limpo (str): Número de celular tratado.
    """

    numero_celular_limpo = re.sub("[^0-9]", "", numero_celular)
    if len(numero_celular_limpo) > 11:
        numero_celular_limpo = numero_celular_limpo[-11:]
    else:
        numero_celular_limpo = np.nan
    return numero_celular_limpo


def get_data_vinculacao(
    df_contas_por_assessor_ids_emails: pd.DataFrame, account: str, hoje: pd.Timestamp
) -> str:
    """Esta função busca na base assessores a data em que a conta foi vinculada à One e retorna
    esta data como uma string no formato %Y-%m-%d.

    Args:
        df_contas_por_assessor_ids_emails (pd.DataFrame): Data frame com a relação de contas e as datas de associação das mesmas.
        nr_conta (str): número da conta que será criada.

    Returns:
        data_string (str): Data de vinculação da conta à One.
    """
    if df_contas_por_assessor_ids_emails[
        df_contas_por_assessor_ids_emails["account"] == account
    ].empty:
        return hoje.strftime("%Y-%m-%d")
    else:
        data_string = df_contas_por_assessor_ids_emails[
            df_contas_por_assessor_ids_emails["account"] == account
        ]["timestamp_escrita_bronze"].values[0]

        data_datetime64 = data_string.astype("datetime64[ns]")
        data_timestamp = pd.to_datetime(data_datetime64)
        data_string = data_timestamp.strftime("%Y-%m-%d")

        return data_string


def associa_conta_contato(
    nome_conta: str,
    id_conta: str,
    comercial_conta: str,
    celular_conta: str,
    email_conta: str,
    df_contatos_hubspot: pd.DataFrame,
) -> None:
    """Associa contas criadas no hubspot com contatos existentes do hubspot. A associação é feita levando em consideração
    o comercial associado a conta, email e celular.

    Args:
        nome_conta (str): Nome do cliente da conta que será associada.
        id_conta (str): ID hubspot da conta que será associada.
        comercial_conta (str): Assessor da conta que será associada.
        celular_conta (str): Celular da conta que será associada.
        email_conta (str): E-mail da conta que será associada.
        df_contatos_hubspot (pd.DataFrame): DataFrame com os contatos que estão cadastrados no Hubspot.

    Returns:
        (None)
    """

    celular_conta = celular_conta[-11:]

    df_contatos_hubspot = df_contatos_hubspot.loc[
        :, ["id", "comercial", "phone", "email", "updated_at", "Conta", "Cliente"]
    ]
    df_contatos_hubspot["Cliente"] = df_contatos_hubspot["Cliente"].str.lower()
    df_contatos_hubspot["phone"] = df_contatos_hubspot["phone"].astype(str)
    df_contatos_hubspot["phone"] = df_contatos_hubspot["phone"].apply(
        limpa_numero_celular
    )
    df_contatos_hubspot = (
        df_contatos_hubspot.sort_values(
            by=[
                "Conta",
                "comercial",
                "Cliente",
                "email",
                "phone",
                "updated_at",
            ]
        )
        .drop_duplicates("id")
        .drop_duplicates(["email"], keep="first")
    )

    df_checagem_associacao = df_contatos_hubspot.query(
        "comercial == @comercial_conta & Cliente == @nome_conta & ((email == @email_conta) | (phone == @celular_conta))"
    )

    if not df_checagem_associacao.empty:
        id_contato = df_checagem_associacao.iat[0, 0]
        try:
            token = json.loads(get_secret_value("prd/apis/hubspot"))["token"]

            headers = {"accept": "application/json", "authorization": f"Bearer {token}"}
            url = f"https://api.hubapi.com/crm/v4/objects/0-1/{id_contato}/associations/default/2-7418328/{id_conta}"
            response = requests.request("PUT", url, headers=headers)

            print(response)
            print(response.content)
            print(f"conta associada {id_contato}")
            print(f"NOME: {nome_conta} -> {df_checagem_associacao['Cliente'].values}")
            print(
                f"COMERCIAL: {comercial_conta} -> {df_checagem_associacao['comercial'].values}"
            )
            print(f"EMAIL: {email_conta} -> {df_checagem_associacao['email'].values}")
            print(
                f"CELULAR: {celular_conta} -> {df_checagem_associacao['phone'].values}",
                end="\n\n",
            )

            return True
        except:
            return False

    else:
        print("conta não associada")
        df_checagem_associacao = df_contatos_hubspot.query(
            "comercial == @comercial_conta & ((email == @email_conta) | (phone == @celular_conta))"
        )
        print(f"NOME: {nome_conta} -> {df_checagem_associacao['Cliente'].values}")
        print(
            f"COMERCIAL: {comercial_conta} -> {df_checagem_associacao['comercial'].values}"
        )
        print(f"EMAIL: {email_conta} -> {df_checagem_associacao['email'].values}")
        print(
            f"CELULAR: {celular_conta} -> {df_checagem_associacao['phone'].values}",
            end="\n\n",
        )
        return False


def estabelece_df_criacao_contas(
    df_mudancas_contas_novas: pd.DataFrame, df_contas_por_assessor: pd.DataFrame
) -> pd.DataFrame:
    """Usa o Dataframe das contas que precisam ser criados ou modificadas e o Dataframe com as contas sob responsabilidade de cada assessor para estabelecer a lista das contas a setem criadas.

    Args:
        df_mudancas_contas_novas (pd.DataFrame): Dataframe das contas que precisam ser criados ou modificadas.
        df_contas_por_assessor (pd.DataFrame): Dataframe com as contas sob responsabilidade de cada assessor

    Returns:
        df_criacao_contas (pd.DataFrame): Dataframe com a lista das contas a serem criadas.
    """
    df_mudancas_contas_novas = df_mudancas_contas_novas.merge(
        df_contas_por_assessor[["account", "Id"]],
        left_on="carteira",
        right_on="account",
        how="left",
    ).drop("account", axis=1)
    df_mudancas_contas_novas = df_mudancas_contas_novas.rename(
        {"Id": "id_assessor_hubspot"}, axis=1
    )
    df_mudancas_contas_novas["id_assessor_hubspot"] = df_mudancas_contas_novas[
        "id_assessor_hubspot"
    ].fillna("213100581")

    data_agora = datetime.now().strftime("%Y-%m-%d")
    df_mudancas_contas_novas.to_csv(
        f"s3://one-teste-logs/criar_contas/contas_final_{data_agora}.csv", sep=";"
    )

    return df_mudancas_contas_novas


def estabelece_df_criacao_contas_offshore(
    df_auc_offshore: pd.DataFrame,
    df_receita_offshore: pd.DataFrame,
    mes_passado: pd.Timestamp,
    df_socios: pd.DataFrame,
    df_contas_hubspot: pd.DataFrame,
) -> pd.DataFrame:
    """Retorna quais contas offshore precisam ser criadas no Hubspot.

    Args:
        df_auc_offshore (pd.DataFrame): Dataframe com o AuC das contas offshore de clientes da One.
        df_receita_offshore (pd.DataFrame): Dataframe com a receita das contas offshore de clientes da One.
        mes_passado (pd.Timestamp): Mês de referência dos dados offshore.
        df_socios (pd.DataFrame): Dataframe com dados de todos os sócios da One.
        df_contas_hubspot (pd.DataFrame): Dataframe com dados das contas de clietes da One registrados no Hubspot.

    Returns:
        df_contas_fora_hub_filtrado (pd.DataFrame): Dataframe com as contas offshore de clientes a One a serem criadas no Hubspot.
    """
    # Pega último mês do relatório de AUC
    df_auc_offshore_atual = df_auc_offshore[
        (df_auc_offshore["Data"] == f"{mes_passado.year}-{mes_passado.month}-01")
    ]

    # Merge pela coluna em comum 'ClienteAuxiliar'
    df_offshore_combinado = df_auc_offshore_atual[
        ["Acc Number", "Cliente", "ClienteAuxiliar"]
    ].merge(
        df_receita_offshore[["Nome", "ClienteAuxiliar"]],
        on="ClienteAuxiliar",
        how="outer",
    )

    # Contas que não estão criadas no Hubspot
    df_contas_fora_hub = df_offshore_combinado[
        ~df_offshore_combinado["Acc Number"].isin(df_contas_hubspot["carteira"])
    ]

    # Junta com base sócios para pegar e-mail e alterar o proprietário para One ou algum comercial
    df_socios["Nome_bd"] = (
        df_socios["Nome_bd"]
        .replace("[-./]", "", regex=True)
        .apply(normaliza_caracteres_unicode)
    )
    df_contas_fora_hub = df_contas_fora_hub.merge(
        df_socios[["Nome", "Id"]], on="Nome", how="left"
    )
    df_contas_fora_hub.loc[
        ~df_contas_fora_hub["Nome"].isin(
            df_socios[df_socios["Área"] == "COMERCIAL"]["Nome"]
        ),
        ["Nome", "Id"],
    ] = 213100581
    df_contas_fora_hub_filtrado = df_contas_fora_hub.dropna(
        subset=["Acc Number"]
    )  # retira tudo que tem Conta vazia
    df_contas_fora_hub_filtrado = df_contas_fora_hub_filtrado[
        ["Acc Number", "Cliente", "Nome", "Id"]
    ]
    df_contas_fora_hub_filtrado.columns = ["conta", "cliente", "comercial", "id"]

    return df_contas_fora_hub_filtrado


def cria_contas_onshore(
    public_object_search_request: PublicObjectSearchRequest,
    cliente_hubspot: hubspot.Client,
    df_criacao_contas: pd.DataFrame,
    df_contas_por_assessor_ids_emails: pd.DataFrame,
    hoje: pd.Timestamp,
    df_contatos_hubspot: pd.DataFrame,
    df_contas_criadas_por_usuarios: pd.DataFrame,
) -> tuple:
    """Cria as contas onshore no Hubspot.

    Args:
        public_object_search_request (PublicObjectSearchRequest): Cliente do Hubspot que verifica se uma conta já existe.
        cliente_hubspot (hubspot.Client): Cliente do Hubspot que interage com API para criar ou modificar contas.
        df_criacao_contas (pd.DataFrame): Dataframe com a lista das contas a serem atualizadas ou modificadas.
        df_contas_por_assessor_ids_emails (pd.DataFrame): Dataframe com as contas assessoradas por cada assessor, seus IDs Hubspot e e-mails dos assessores.
        hoje (pd.Timestamp): Data de hoje.
        df_contatos_hubspot (pd.DataFrame): Dataframe com os contatos da One no Hubspot.
        df_contas_criadas_por_usuarios (pd.DataFrame): Dataframe com as contas criadas por usuários.

    Returns:
        (contas_criadas, contas_modificadas, contas_associadas, df_contas_criadas_autamoticamente) (tuple): Retorna um tupla com 3 listas e um Dataframe: lista com contas onshore que foram criadas, lista com contas onshore modificadas e lista com contas onshore associadas. O Dataframe contém as contas criadas automaticamente no Hubspot.
    """
    contas_criadas, contas_modificadas, contas_associadas = [], [], []

    for row in df_criacao_contas.itertuples():
        time.sleep(random.uniform(0.4, 0.8))
        # Verificação se a conta já existe
        public_object_search_request.filter_groups[0]["filters"][0][
            "value"
        ] = row.carteira
        try:
            api_response = cliente_hubspot.crm.objects.search_api.do_search(
                object_type="2-7418328",
                public_object_search_request=public_object_search_request,
            )
        except ApiException as e:
            print("Exception when calling search_api->do_search: %s\n" % e)

        celular = 0 if row.celular == "" else int(row.celular)

        if len(api_response.results) == 0:  # Cria a conta
            data_vinculacao = get_data_vinculacao(
                df_contas_por_assessor_ids_emails, row.carteira, hoje
            )
            properties = {
                "account_conta_btg": row.carteira,
                "account_profile": row.suitability.lower(),
                "account_type": row.tipo_cliente.lower(),
                "account_email": row.email.lower(),
                "account_phone": int(celular),
                "localizacao": "Brasil",
                "account_open_date": row.data_abertura,
                "account_irtrade": "nao",
                "account_leroy": "nao",
                "account_name": row.nome_completo,
                "account_status": row.status.lower(),
                "account_pl_declared": row.pl_declarado,
                "cidade": row.endereco_cidade,
                "estado": row.endereco_estado,
                "cep": row.endereco_cep,
                "account_comercial": row.id_assessor_hubspot,
                "data_de_vinculacao": data_vinculacao,
            }
            simple_public_object_input = SimplePublicObjectInput(properties=properties)
            try:
                api_response = cliente_hubspot.crm.objects.basic_api.create(
                    object_type="2-7418328",
                    simple_public_object_input_for_create=simple_public_object_input,
                )
                contas_criadas.append(row.carteira)
                print(f"OK! Conta {row.carteira} Criada.")
            except ApiException as e:
                time.sleep(random.uniform(10, 12))
                api_response = cliente_hubspot.crm.objects.basic_api.create(
                    object_type="2-7418328",
                    simple_public_object_input_for_create=simple_public_object_input,
                )
                contas_criadas.append(row.carteira)
                print(f"OK! Conta {row.carteira} Criada.")

            df_criacao_contas.loc[
                df_criacao_contas["carteira"] == row.carteira, "Link"
            ] = (
                "https://app.hubspot.com/contacts/21512517/record/2-7418328/"
                + api_response.id
            )

            # Associa conta ao contato
            associada = associa_conta_contato(
                nome_conta=row.nome_completo.lower(),
                id_conta=api_response.id,
                comercial_conta=row.id_assessor_hubspot,
                celular_conta=str(celular),
                email_conta=row.email.lower(),
                df_contatos_hubspot=df_contatos_hubspot,
            )
            contas_associadas.append(associada)

        else:  # Modifica a conta
            account_id = api_response.results[0].id

            # Caso a conta tenha sido criada por usuário e não possua data de vinculação o fluxo de atualização é diferente.
            if row.carteira in df_contas_criadas_por_usuarios.to_list():
                data_vinculacao = get_data_vinculacao(
                    df_contas_por_assessor_ids_emails, row.carteira, hoje
                )
                print(data_vinculacao)
                properties = {
                    "account_profile": row.suitability.lower(),
                    "account_type": row.tipo_cliente.lower(),
                    "account_email": row.email.lower(),
                    "account_phone": int(celular),
                    "localizacao": "Brasil",
                    "account_name": row.nome_completo,
                    "account_status": row.status.lower(),
                    "account_pl_declared": row.pl_declarado,
                    "cidade": row.endereco_cidade,
                    "estado": row.endereco_estado,
                    "cep": row.endereco_cep,
                    "data_de_encerramento": row.data_encerramento,
                    "data_do_primeiro_investimento": row.data_primeiro_investimento,
                    "account_open_date": row.data_abertura,
                    "data_de_vinculacao": data_vinculacao,
                }

            else:
                properties = {
                    "account_profile": row.suitability.lower(),
                    "account_type": row.tipo_cliente.lower(),
                    "account_email": row.email.lower(),
                    "account_phone": int(celular),
                    "localizacao": "Brasil",
                    "account_name": row.nome_completo,
                    "account_status": row.status.lower(),
                    "account_pl_declared": row.pl_declarado,
                    "cidade": row.endereco_cidade,
                    "estado": row.endereco_estado,
                    "cep": row.endereco_cep,
                    "data_de_encerramento": row.data_encerramento,
                    "data_do_primeiro_investimento": row.data_primeiro_investimento,
                    "account_open_date": row.data_abertura,
                }

            simple_public_object_input = SimplePublicObjectInput(properties=properties)
            try:
                api_response = cliente_hubspot.crm.objects.basic_api.update(
                    object_type="2-7418328",
                    object_id=account_id,
                    simple_public_object_input=simple_public_object_input,
                )
                contas_modificadas.append(row.carteira)
                print(f"OK! Conta {row.carteira} Modificada.")
            except ApiException as e:
                time.sleep(random.uniform(10, 12))
                api_response = cliente_hubspot.crm.objects.basic_api.update(
                    object_type="2-7418328",
                    object_id=account_id,
                    simple_public_object_input=simple_public_object_input,
                )
                contas_modificadas.append(row.carteira)
                print(f"OK! Conta {row.carteira} Modificada.")

    df_contas_criadas_autamoticamente = df_criacao_contas[
        df_criacao_contas["carteira"].isin(contas_criadas)
    ][["carteira", "Link"]]
    df_contas_criadas_autamoticamente.columns = ["Conta BTG", "Link"]

    return (
        contas_criadas,
        contas_modificadas,
        contas_associadas,
        df_contas_criadas_autamoticamente,
    )


def cria_contas_offshore(
    public_object_search_request: PublicObjectSearchRequest,
    cliente_hubspot: hubspot.Client,
    drive_microsoft: OneGraphDrive,
    df_contas_fora_hub_filtrado: pd.DataFrame,
    hoje: pd.Timestamp,
    df_socios: pd.DataFrame,
    df_contas_criadas_autamoticamente: pd.DataFrame,
    df_criacao_contas: pd.DataFrame,
    df_contas_sem_associacao: pd.DataFrame,
    df_contas_sem_comercial: pd.DataFrame,
    contas_associadas: list,
    id_usuario_envio_email: str,
    bucket_lake_gold: str,
    path_htmls: str,
) -> None:
    """Cria as contas offshore no Hubspot e dispara e-mails com todas as contas criadas para os diretamente interessados.

    Args:
        public_object_search_request (PublicObjectSearchRequest): Cliente do Hubspot que verifica se uma conta já existe.
        cliente_hubspot (hubspot.Client): Cliente do Hubspot que interage com API para criar ou modificar contas.
        drive_microsoft (OneGraphDrive): Cliente do Microsoft Graph usado para ler arquivos do OneDrive e enviar e-mails.
        df_contas_fora_hub_filtrado (pd.DataFrame): Dataframe com contas que não estão no Hubspot.
        hoje (pd.Timestamp): Data de hoje.
        df_socios (pd.DataFrame): Dataframe com dados dos sócios da One.
        df_contas_criadas_autamoticamente (pd.DataFrame): Dataframe das contas onshore criadas automaticamente.
        df_criacao_contas (pd.DataFrame): Dataframe com lista das contas onshore a serem criadas.
        df_contas_sem_associacao (pd.DataFrame): Dataframe com as contas de clientes que não estão associadas.
        df_contas_sem_comercial (pd.DataFrame): Dataframe das contas que não possuem assessor.
        contas_associadas (list): Lista das contas que foram associadas no Hubspot.
        id_usuario_envio_email (str): ID do usuário Microsoft que envia os e-mails.
        path_htmls (str): Diretório base dos HTMLs dos e-mails.

    Returns:
        None
    """
    contas_offshore_criadas = []

    for row in df_contas_fora_hub_filtrado.itertuples():
        localizacao = "Miami" if "TL" in row.conta else "Cayman"

        # Verificação se a conta já existe
        public_object_search_request.filter_groups[0]["filters"][0]["value"] = row.conta
        try:
            api_response = cliente_hubspot.crm.objects.search_api.do_search(
                object_type="2-7418328",
                public_object_search_request=public_object_search_request,
            )
        except ApiException as e:
            print("Exception when calling search_api->do_search: %s\n" % e)

        if len(api_response.results) == 0:
            properties = {
                "account_conta_btg": row.conta,
                "account_name": row.cliente,
                "localizacao": localizacao,
                "account_comercial": row.id,
                "data_de_vinculacao": datetime.strftime(
                    hoje.replace(day=1) - timedelta(1), "%Y-%m-%d"
                ),
            }
            simple_public_object_input = SimplePublicObjectInput(properties=properties)
            try:
                api_response = cliente_hubspot.crm.objects.basic_api.create(
                    object_type="2-7418328",
                    simple_public_object_input_for_create=simple_public_object_input,
                )
                contas_offshore_criadas.append(row.conta)
                print(f"OK! Conta {row.conta} Criada.")
            except ApiException as e:
                time.sleep(random.uniform(10, 12))
                api_response = cliente_hubspot.crm.objects.basic_api.create(
                    object_type="2-7418328",
                    simple_public_object_input_for_create=simple_public_object_input,
                )
                contas_offshore_criadas.append(row.conta)
                print(f"OK! Conta {row.conta} Criada.")

    if "Link" not in df_contas_fora_hub_filtrado.columns:
        df_contas_fora_hub_filtrado["Link"] = ""
    df_criacao_offshore = df_contas_fora_hub_filtrado[
        df_contas_fora_hub_filtrado["conta"].isin(contas_offshore_criadas)
    ][["comercial", "conta", "Link"]]
    df_criacao_offshore.columns = ["Comercial", "Conta BTG", "Link"]
    df_contas_novas_offshore = df_contas_fora_hub_filtrado.loc[
        df_contas_fora_hub_filtrado["conta"].isin(contas_offshore_criadas), :
    ]

    emails_automaticos_novas_contas(
        df_contas_criadas_autamoticamente,
        df_socios,
        df_criacao_contas,
        df_contas_sem_associacao,
        df_contas_sem_comercial,
        contas_associadas,
        drive_microsoft=drive_microsoft,
        df_contas_novas_offshore=df_contas_novas_offshore,
        id_usuario_envio_email=id_usuario_envio_email,
        bucket_lake_gold=bucket_lake_gold,
        path_htmls=path_htmls,
    )

    df_contas_sem_associacao = df_contas_sem_associacao.loc[:, ["Conta BTG", "Link"]]
    df_contas_criadas_autamoticamente = df_contas_criadas_autamoticamente.loc[
        :, ["Conta BTG", "Link"]
    ]

    # Template de e-mail para envio dos três dataframes criados para o time de BackOffice
    with open(
        os.path.join(path_htmls, "criar_contas_email_base_completo_offshore.html"),
        encoding="UTF-8",
        mode="r",
    ) as email_base_completo_offshore:
        template_email_base_completo_offshore = email_base_completo_offshore.read()

    template_email_base_completo_offshore = (
        template_email_base_completo_offshore.format(
            df_contas_criadas_autamoticamente.to_html(index=False),
            df_contas_sem_associacao.to_html(index=False),
            df_contas_sem_comercial.to_html(index=False),
            df_criacao_offshore.to_html(index=False),
        )
    )

    # Corpo da requisição para envio de e-mail para o back office
    json_base_completo_offshore = {
        "message": {
            "subject": "Contas criadas no Hubspot - "
            + datetime.now().strftime("%d/%m/%Y"),
            "body": {
                "contentType": "HTML",
                "content": template_email_base_completo_offshore,
            },
            "toRecipients": [
                {"emailAddress": {"address": "back.office@investimentos.one"}},
                {"emailAddress": {"address": "guilherme.isaac@investimentos.one"}},
                {"emailAddress": {"address": "rodrigo.alvarenga@investimentos.one"}},
                {"emailAddress": {"address": "eduardo.queiroz@investimentos.one"}},
                {"emailAddress": {"address": "victor.miranda@investimentos.one"}},
                {"emailAddress": {"address": "gustavo.sousa@investimentos.one"}},
            ],
        }
    }

    drive_microsoft.send_email_endpoint(
        id_usuario_envio_email, json_base_completo_offshore
    )

    return None


def emails_automaticos_novas_contas(
    df_contas_criadas_autamoticamente: pd.DataFrame,
    df_socios: pd.DataFrame,
    df_criacao_contas: pd.DataFrame,
    df_contas_sem_associacao: pd.DataFrame,
    df_contas_sem_comercial: pd.DataFrame,
    contas_associadas: list,
    drive_microsoft: OneGraphDrive,
    id_usuario_envio_email: str,
    bucket_lake_gold: str,
    path_htmls: str,
    df_contas_novas_offshore: pd.DataFrame = pd.DataFrame(),
) -> None:
    """Envia e-mails das novas contas criadas no Hubspot para os assessores.

    Args:
        df_contas_criadas_autamoticamente (pd.DataFrame): Dataframe com as contas que foram criadas.
        df_socios (pd.DataFrame): Dataframe com dados dos sócios da One.
        df_criacao_contas (pd.DataFrame): Dataframe com as contas onshore a serem criadas no Hubspot.
        df_contas_sem_associacao (pd.DataFrame): Dataframe com as contas sem associação.
        df_contas_sem_comercial (pd.DataFrame): Dataframe com as contas que estão sem comercial.
        contas_associadas (list): Lista com valores booleanos indicando quais contas foram associadas automaticamente.
        drive_microsoft (OneGraphDrive): Cliente do Microsoft Graph usado para ler arquivos e enviar e-mails.
        id_usuario_envio_email (str): ID do usuário Microsoft que envia os e-mails.
        bucket_lake_gold (str): Bucket da gold.
        path_htmls (str): Diretório base dos HTMLs dos e-mails.
        df_contas_novas_offshore (pd.DataFrame, optional):Dataframe com as novas contas offshore. Defaults to None.

    Returns:
       None
    """

    dominio_email = "@investimentos.one"

    # As linhas abaixo são necessárias uma vez que a base sócios, coluna Nome_bd, foi modificada em outras etapas deste código
    df_socios[["Team Leader", "Gestor / Head", "business_leader"]] = (
        df_socios[["Team Leader", "Gestor / Head", "business_leader"]]
        .astype("str")
        .replace("[-./]", "", regex=True)
        .apply(lambda col: col.apply(normaliza_caracteres_unicode))
    )

    # Trata o DataFrame de df_contas_novas_offshore
    if not df_contas_novas_offshore.empty:
        df_contas_novas_offshore.drop(columns=["cliente", "comercial"], inplace=True)
        df_contas_novas_offshore.rename(
            columns={"conta": "Conta BTG", "id": "id_assessor_hubspot"}, inplace=True
        )
        df_contas_novas_offshore["tipo"] = (
            "offshore"  # Necessário para separar o e-mail do backoffice
        )
    else:
        df_contas_novas_offshore = pd.DataFrame(
            {"Conta BTG": [], "Link": [], "id_assessor_hubspot": [], "tipo": []}
        )

    # Trata o DataFrame de df_contas_criadas_autamoticamente
    df_contas_criadas_autamoticamente["associada"] = contas_associadas
    df_contas_criadas_autamoticamente = df_contas_criadas_autamoticamente.merge(
        df_criacao_contas[["carteira", "id_assessor_hubspot"]],
        left_on="Conta BTG",
        right_on="carteira",
    ).drop(columns=["carteira"])
    # Necessário para separar o e-mail do backoffice
    df_contas_criadas_autamoticamente["tipo"] = "normal"
    df_contas_criadas_autamoticamente["id_assessor_hubspot"] = (
        df_contas_criadas_autamoticamente["id_assessor_hubspot"].fillna("213100581")
    )

    # Trata o DataFrame de contas sem associação

    df_contas_sem_associacao = pd.concat(
        [df_contas_sem_associacao, df_contas_sem_comercial]
    )

    df_contas_sem_associacao["id_assessor_hubspot"] = df_contas_sem_associacao[
        "id_assessor_hubspot"
    ].fillna("213100581")

    # Une as contas novas com as contas novas offshore e seleciona apenas contas que não foram associadas automaticamente
    df_contas_criadas_autamoticamente = pd.concat(
        objs=[df_contas_criadas_autamoticamente, df_contas_novas_offshore],
        axis=0,
        ignore_index=True,
        join="outer",
    )

    df_contas_criadas_autamoticamente["associada"] = df_contas_criadas_autamoticamente[
        "associada"
    ].fillna(False)
    df_contas_criadas_autamoticamente = df_contas_criadas_autamoticamente.loc[
        df_contas_criadas_autamoticamente["associada"] == False, :
    ]

    # Cria um array com o id hubspot dos assessores que tem contas criadas e contas não associadas
    id_assessores_contas_novas = (
        df_contas_criadas_autamoticamente["id_assessor_hubspot"].unique().ravel()
    )
    id_assessores_contas_sem_associacao = (
        df_contas_sem_associacao["id_assessor_hubspot"].unique().ravel()
    )
    id_assessores = np.concatenate(
        [id_assessores_contas_novas, id_assessores_contas_sem_associacao]
    )
    id_assessores = np.unique(id_assessores.astype(str))

    # Isola as contas de cada assessor em DataFrames separados
    for id_assessor in id_assessores:
        # Caso a conta não possua assessor o backoffice será responsável pela associação
        if id_assessor == "213100581":
            filtro = (
                df_contas_criadas_autamoticamente["id_assessor_hubspot"] == id_assessor
            ) & (df_contas_criadas_autamoticamente["tipo"] == "normal")
            contas_novas_backoffice = df_contas_criadas_autamoticamente[filtro].drop(
                columns=["associada", "id_assessor_hubspot", "tipo"]
            )

            filtro = (
                df_contas_criadas_autamoticamente["id_assessor_hubspot"] == id_assessor
            ) & (df_contas_criadas_autamoticamente["tipo"] == "offshore")
            contas_novas_offshore_backoffice = df_contas_criadas_autamoticamente[
                filtro
            ].drop(columns=["associada", "id_assessor_hubspot", "tipo"])

            filtro = df_contas_sem_associacao["id_assessor_hubspot"] == id_assessor

            contas_sem_associacao_backoffice = df_contas_sem_associacao[filtro].drop(
                columns=["id_assessor_hubspot"]
            )

            contas_novas_backoffice = pd.concat(
                [contas_novas_backoffice, contas_sem_associacao_backoffice]
            )

            # Envia e-mail para o backoffice com as contas que não possuem assessor
            # ou as contas que são offshore.

            with open(
                os.path.join(path_htmls, "email_base_backoffice.html"),
                encoding="UTF-8",
                mode="r",
            ) as email_base_backoffice:
                template_email_base_backoffice = email_base_backoffice.read()

            json_email_base_backoffice = {
                "message": {
                    "subject": "Contas criadas no Hubspot - "
                    + datetime.now().strftime("%d/%m/%Y"),
                    "body": {
                        "contentType": "HTML",
                        "content": template_email_base_backoffice.format(
                            contas_novas_backoffice.to_html(index=False),
                            contas_novas_offshore_backoffice.to_html(index=False),
                        ),
                    },
                    "toRecipients": [
                        {"emailAddress": {"address": "back.office@investimentos.one"}},
                        {
                            "emailAddress": {
                                "address": "eduardo.queiroz@investimentos.one"
                            }
                        },
                        {
                            "emailAddress": {
                                "address": "gustavo.sousa@investimentos.one"
                            }
                        },
                    ],
                }
            }

            drive_microsoft.send_email_endpoint(
                id_usuario_envio_email, json_email_base_backoffice
            )

            continue

        # Separa as contas do DF de contas_nao_associadas
        contas_nao_associadas_assessor = df_contas_sem_associacao[
            df_contas_sem_associacao["id_assessor_hubspot"] == id_assessor
        ].drop(columns=["id_assessor_hubspot"])

        # Separa as contas do DF de df_contas_criadas_autamoticamente
        contas_assessor = df_contas_criadas_autamoticamente[
            df_contas_criadas_autamoticamente["id_assessor_hubspot"] == id_assessor
        ]
        contas_assessor = contas_assessor.drop(
            columns=["associada", "id_assessor_hubspot", "tipo"]
        )

        # Une os dois DFs que possuem contas não associadas
        contas_assessor = pd.concat(
            [contas_assessor, contas_nao_associadas_assessor],
            axis=0,
            join="outer",
            ignore_index=False,
        )

        # Busca o e-mail do assessor
        email_assessor = df_socios[df_socios["Id"] == id_assessor]["E-mail"].values[0]
        email_assessor = email_assessor + dominio_email

        # Busca o nome do assessor
        nome_assessor = df_socios[df_socios["Id"] == id_assessor]["Nome_bd"].values[0]
        nome_assessor = nome_assessor.split()[0]

        with open(
            os.path.join(path_htmls, "email_base.html"), encoding="UTF-8", mode="r"
        ) as email_base:
            template_email_base = email_base.read()

        json_email_base = {
            "message": {
                "subject": "Contas criadas no Hubspot - "
                + datetime.now().strftime("%d/%m/%Y"),
                "body": {
                    "contentType": "HTML",
                    "content": template_email_base.format(
                        nome_assessor, contas_assessor.to_html(index=False)
                    ),
                },
                "toRecipients": [
                    {"emailAddress": {"address": email_assessor}},
                    {"emailAddress": {"address": "eduardo.queiroz@investimentos.one"}},
                    {"emailAddress": {"address": "gustavo.sousa@investimentos.one"}},
                ],
            }
        }

        drive_microsoft.send_email_endpoint(id_usuario_envio_email, json_email_base)

    # Cria o DF historico de contas sem associação para ser possível a lógica de e-mails recorrentes
    df_contas_sem_associacao = df_contas_sem_associacao.loc[
        (df_contas_sem_associacao["id_assessor_hubspot"] != "213100581"), :
    ].copy()

    # Não associa 1 caso o DF de contas sem associação esteja vazio
    if not df_contas_sem_associacao.empty:
        df_contas_sem_associacao.loc[:, "dias_sem_associacao"] = 1

    df_contas_sem_associacao_historico = pd.read_parquet(
        f"s3://{bucket_lake_gold}/api/hubspot/contas_sem_associacao/contas_sem_associacao.parquet"
    )
    df_contas_sem_associacao_historico = pd.concat(
        [df_contas_sem_associacao_historico, df_contas_sem_associacao],
        axis=0,
        ignore_index=True,
        join="outer",
    ).drop(columns=["id_assessor_hubspot"])
    df_contas_sem_associacao_historico = (
        df_contas_sem_associacao_historico.groupby(["Conta BTG", "Link"])
        .sum()
        .reset_index()
    )
    df_contas_sem_associacao_historico.to_parquet(
        f"s3://{bucket_lake_gold}/api/hubspot/contas_sem_associacao/contas_sem_associacao.parquet"
    )
    df_contas_sem_associacao_historico = df_contas_sem_associacao_historico.merge(
        df_contas_sem_associacao[["Conta BTG", "id_assessor_hubspot"]], on="Conta BTG"
    )

    # Faz um loop no histórico por todas as contas sem associacao
    for id_assessor in (
        df_contas_sem_associacao_historico["id_assessor_hubspot"].dropna().unique()
    ):
        contas_assessor = df_contas_sem_associacao_historico[
            df_contas_sem_associacao_historico["id_assessor_hubspot"] == id_assessor
        ]

        if (contas_assessor["dias_sem_associacao"].max() > 5) & (
            contas_assessor["dias_sem_associacao"].max() <= 10
        ):
            # Encontra contas com mais de 5 e menos de 10 dias sem associação
            contas_assessor_5 = contas_assessor[
                contas_assessor["dias_sem_associacao"].between(5, 10, inclusive="right")
            ]

            contas_assessor_5 = contas_assessor_5.drop(columns=["id_assessor_hubspot"])

            contas_assessor_5.rename(
                columns={"dias_sem_associacao": "Dias sem associação"}, inplace=True
            )

            nome_assessor = df_socios[df_socios["Id"] == id_assessor]["Nome_bd"].values[
                0
            ]
            nome_assessor = nome_assessor.split()[0]

            email_assessor = df_socios[df_socios["Id"] == id_assessor]["E-mail"].values[
                0
            ]
            email_assessor = email_assessor + dominio_email

            nome_team_leader = df_socios[df_socios["Id"] == id_assessor][
                "Team Leader"
            ].values[0]

            email_team_leader = df_socios[df_socios["Nome_bd"] == nome_team_leader][
                "E-mail"
            ].values[0]

            email_team_leader = email_team_leader + dominio_email

            with open(
                os.path.join(path_htmls, "email_cobranca_5d.html"),
                encoding="UTF-8",
                mode="r",
            ) as email_cobranca_5d:
                template_email_cobranca_5d = email_cobranca_5d.read()

            # Corpo da requisição para envio de e-mail para os comerciais
            json_email_cobranca_5d = {
                "message": {
                    "subject": "Contas criadas no Hubspot - "
                    + datetime.now().strftime("%d/%m/%Y"),
                    "body": {
                        "contentType": "HTML",
                        "content": template_email_cobranca_5d.format(
                            nome_assessor,
                            contas_assessor_5.to_html(index=False).replace(
                                "<td>", '<td align="center">'
                            ),
                        ),
                    },
                    "toRecipients": [
                        {"emailAddress": {"address": email_assessor}},
                        {"emailAddress": {"address": email_team_leader}},
                        {
                            "emailAddress": {
                                "address": "eduardo.queiroz@investimentos.one"
                            }
                        },
                        {
                            "emailAddress": {
                                "address": "gustavo.sousa@investimentos.one"
                            }
                        },
                    ],
                }
            }

            drive_microsoft.send_email_endpoint(
                id_usuario_envio_email, json_email_cobranca_5d
            )

        if contas_assessor["dias_sem_associacao"].max() > 10:
            # Encontra contas com mais de 10 dias sem associação
            contas_assessor_10 = contas_assessor[
                contas_assessor["dias_sem_associacao"] > 10
            ]
            contas_assessor_10 = contas_assessor_10.drop(
                columns=["id_assessor_hubspot"]
            )
            contas_assessor_10.rename(
                columns={"dias_sem_associacao": "Dias sem associação"}, inplace=True
            )

            nome_assessor = df_socios[df_socios["Id"] == id_assessor]["Nome_bd"].values[
                0
            ]
            nome_assessor = nome_assessor.split()[0]

            email_assessor = df_socios[df_socios["Id"] == id_assessor]["E-mail"].values[
                0
            ]
            email_assessor = email_assessor + dominio_email

            nome_team_leader = df_socios[df_socios["Id"] == id_assessor][
                "Team Leader"
            ].values[0]

            email_team_leader = df_socios[df_socios["Nome_bd"] == nome_team_leader][
                "E-mail"
            ].values[0]

            email_team_leader = email_team_leader + dominio_email

            nome_team_leader = df_socios[df_socios["Id"] == id_assessor][
                "Team Leader"
            ].values[0]
            email_team_leader = df_socios[df_socios["Nome_bd"] == nome_team_leader][
                "E-mail"
            ].values[0]
            email_team_leader = email_team_leader + dominio_email

            nome_business_leader = df_socios[df_socios["Id"] == id_assessor][
                "business_leader"
            ].values[0]
            print(nome_business_leader)
            print(nome_assessor)
            print(nome_team_leader)

            try:
                email_business_leader = df_socios[
                    df_socios["Nome_bd"] == nome_business_leader
                ]["E-mail"].values[0]
                email_business_leader = email_business_leader + dominio_email
            except:
                print(
                    f"Não foi possível encontrar um Business Leader para o assessor '{nome_assessor}'. Notificaremos o Team Leader."
                )
                # neste caso a variável 'email_business_leader' significa o e-mail do team leader (exceção criada para resolver o problema do Atendimento Câmbio Exclusivo, que não tem business leader na base de sócios, mas que pode acontecer para outros casos em que não há business leader para o assessor)
                email_business_leader = df_socios[
                    df_socios["Nome_bd"] == nome_team_leader
                ]["E-mail"].values[0]
                email_business_leader = email_business_leader + dominio_email

            with open(
                os.path.join(path_htmls, "email_cobranca_10d.html"),
                encoding="UTF-8",
                mode="r",
            ) as email_cobranca_10d:
                template_email_cobranca_10d = email_cobranca_10d.read()

            # Corpo da requisição para envio de e-mail para os comerciais
            json_email_cobranca_10d = {
                "message": {
                    "subject": "Contas criadas no Hubspot - "
                    + datetime.now().strftime("%d/%m/%Y"),
                    "body": {
                        "contentType": "HTML",
                        "content": template_email_cobranca_10d.format(
                            nome_assessor,
                            contas_assessor_10.to_html(index=False).replace(
                                "<td>", '<td align="center">'
                            ),
                        ),
                    },
                    "toRecipients": [
                        {"emailAddress": {"address": email_assessor}},
                        {"emailAddress": {"address": email_team_leader}},
                        {"emailAddress": {"address": email_business_leader}},
                        {
                            "emailAddress": {
                                "address": "eduardo.queiroz@investimentos.one"
                            }
                        },
                        {
                            "emailAddress": {
                                "address": "gustavo.sousa@investimentos.one"
                            }
                        },
                    ],
                }
            }

            drive_microsoft.send_email_endpoint(
                id_usuario_envio_email, json_email_cobranca_10d
            )

    df_contas_criadas_autamoticamente = df_contas_criadas_autamoticamente.loc[
        :, ["Conta BTG", "Link"]
    ]

    df_contas_sem_associacao = df_contas_sem_associacao.loc[:, ["Conta BTG", "Link"]]

    return None


def envia_email_erro_backoffice(
    drive_microsoft: OneGraphDrive,
    df_socios: pd.DataFrame,
    df_contas_criadas_autamoticamente: pd.DataFrame,
    df_contas_sem_associacao: pd.DataFrame,
    df_criacao_contas: pd.DataFrame,
    df_contas_sem_comercial: pd.DataFrame,
    contas_associadas: list,
    id_usuario_envio_email: str,
    bucket_lake_gold: str,
    path_htmls: str,
) -> None:
    """Envia e-mails de erro para o Backoffice, bem como emails de alerta de contas que estão há muito tempo de validação para os assessores responsáveis.

    Args:
        drive_microsoft (OneGraphDrive): Cliente no Microsft Graph usado para ler arquivos do OneDrive e enviar e-mails.
        df_socios (pd.DataFrame): Dataframe com dados dos sócios da One.
        df_contas_criadas_autamoticamente (pd.DataFrame): Dataframe com as contas que foram criadas automaticamente.
        df_contas_sem_associacao (pd.DataFrame): Dataframe com as contas sem associação.
        df_criacao_contas (pd.DataFrame): Dataframe com as contas onshore a serem criadas no Hubspot.
        df_contas_sem_comercial (pd.DataFrame): Dataframe com as contas que estão sem comercial.
        contas_associadas (list): Lista com valores booleanos indicando quais contas foram associadas automaticamente.
        id_usuario_envio_email (str): ID do usuário Microsoft que envia os e-mails
        bucket_lake_gold (str): Bucket da gold

    Returns:
       None
    """

    df_socios["Nome_bd"] = (
        df_socios["Nome_bd"]
        .replace("[-./]", "", regex=True)
        .apply(normaliza_caracteres_unicode)
    )

    # envia o email antigo sem nada de Offshore, pq deu erro em achar a base de offshore
    # Template de e-mail para envio dos três dataframes criados para o time de BackOffice

    emails_automaticos_novas_contas(
        df_contas_criadas_autamoticamente,
        df_socios,
        df_criacao_contas,
        df_contas_sem_associacao,
        df_contas_sem_comercial,
        contas_associadas,
        drive_microsoft=drive_microsoft,
        id_usuario_envio_email=id_usuario_envio_email,
        bucket_lake_gold=bucket_lake_gold,
        path_htmls=path_htmls,
    )

    df_contas_sem_associacao = df_contas_sem_associacao.loc[:, ["Conta BTG", "Link"]]
    df_contas_criadas_autamoticamente = df_contas_criadas_autamoticamente.loc[
        :, ["Conta BTG", "Link"]
    ]

    with open(
        os.path.join(path_htmls, "criar_contas_email_base_completo.html"),
        encoding="UTF-8",
        mode="r",
    ) as email_base_completo:
        template_email_base_completo = email_base_completo.read()

    template_email_base_completo = template_email_base_completo.format(
        df_contas_criadas_autamoticamente.to_html(index=False),
        df_contas_sem_associacao.to_html(index=False),
        df_contas_sem_comercial.to_html(index=False),
    )

    # Corpo da requisição para envio de e-mail para o back office
    json_email_base_completo = {
        "message": {
            "subject": "Contas criadas no Hubspot - "
            + datetime.now().strftime("%d/%m/%Y"),
            "body": {"contentType": "HTML", "content": template_email_base_completo},
            "toRecipients": [
                {"emailAddress": {"address": "back.office@investimentos.one"}},
                {"emailAddress": {"address": "guilherme.isaac@investimentos.one"}},
                {"emailAddress": {"address": "rodrigo.alvarenga@investimentos.one"}},
                {"emailAddress": {"address": "eduardo.queiroz@investimentos.one"}},
                {"emailAddress": {"address": "victor.miranda@investimentos.one"}},
                {"emailAddress": {"address": "gustavo.sousa@investimentos.one"}},
            ],
        }
    }

    drive_microsoft.send_email_endpoint(
        id_usuario_envio_email, json_email_base_completo
    )

    None


def cria_contas_offshore_apos_checar_sucesso_das_bases(
    download_receita_offshore_funcionou: bool,
    download_auc_offshore_funcionou: bool,
    df_receita_offshore: pd.DataFrame,
    df_auc_offshore: pd.DataFrame,
    mes_passado: pd.Timestamp,
    df_socios: pd.DataFrame,
    df_contas_hubspot: pd.DataFrame,
    public_object_search_request: PublicObjectSearchRequest,
    cliente_hubspot: hubspot.Client,
    drive_microsoft: OneGraphDrive,
    hoje: pd.Timestamp,
    df_contas_criadas_autamoticamente: pd.DataFrame,
    df_criacao_contas: pd.DataFrame,
    df_contas_sem_associacao: pd.DataFrame,
    df_contas_sem_comercial: pd.DataFrame,
    contas_associadas: list,
    id_usuario_envio_email: str,
    bucket_lake_gold: str,
    path_htmls: str,
) -> None:
    """Verifica sucesso de carregamento das base offshore antes de criar essas contas.

    Args:
        download_receita_offshore_funcionou (bool): Booleano que indica se houve ou não sucesso em baixar a base de receita das contas offshore offshore.
        download_auc_offshore_funcionou (bool): Booleano que indica se houve ou não sucesso em baixar a base de AuC das contas offshore offshore.
        df_receita_offshore (pd.DataFrame): Dataframe com a receita das contas offshore de clientes da One.
        df_auc_offshore (pd.DataFrame): Dataframe com o AuC das contas offshore de clientes da One.
        mes_passado (pd.Timestamp): Mês de referência dos dados offshore.
        df_socios (pd.DataFrame): Dataframe com dados dos sócios da One.
        df_contas_hubspot (pd.DataFrame): Dataframe com dados das contas de clietes da One registrados no Hubspot.
        public_object_search_request (PublicObjectSearchRequest): Cliente do Hubspot que verifica se uma conta já existe.
        cliente_hubspot (hubspot.Client): Cliente do Hubspot que interage com API para criar ou modificar contas.
        drive_microsoft (OneGraphDrive): Cliente no Microsft Graph usado para ler arquivos do OneDrive e enviar e-mails.
        hoje (pd.Timestamp): Data de hoje.
        df_contas_criadas_autamoticamente (pd.DataFrame): Dataframe com as contas que foram criadas automaticamente.
        df_criacao_contas (pd.DataFrame): Dataframe com as contas onshore a serem criadas no Hubspot.
        df_contas_sem_associacao (pd.DataFrame): Dataframe com as contas sem associação.
        df_contas_sem_comercial (pd.DataFrame): Dataframe com as contas que estão sem comercial.
        contas_associadas (list): Lista com valores booleanos indicando quais contas foram associadas automaticamente.
        id_usuario_envio_email (str): ID do usuário Microsoft que envia os e-mails.
        path_htmls (str): Diretório base dos HTMLs dos emails.

    Returns:
        None
    """
    if download_receita_offshore_funcionou and download_auc_offshore_funcionou:
        print("O download das duas bases offshore ocorreu!")
        df_contas_fora_hub_filtrado = estabelece_df_criacao_contas_offshore(
            df_auc_offshore,
            df_receita_offshore,
            mes_passado,
            df_socios,
            df_contas_hubspot,
        )
        cria_contas_offshore(
            public_object_search_request,
            cliente_hubspot,
            drive_microsoft,
            df_contas_fora_hub_filtrado,
            hoje,
            df_socios,
            df_contas_criadas_autamoticamente,
            df_criacao_contas,
            df_contas_sem_associacao,
            df_contas_sem_comercial,
            contas_associadas,
            id_usuario_envio_email,
            bucket_lake_gold,
            path_htmls,
        )
    else:
        if not download_receita_offshore_funcionou and download_auc_offshore_funcionou:
            print(
                f"O download da base de receitas offshore não ocorreu! Verifique o erro -> '''{df_receita_offshore}'''"
            )
        if download_receita_offshore_funcionou and not download_auc_offshore_funcionou:
            print(
                f"O download da base de AuC offshore não ocorreu! Verifique o erro -> '''{df_auc_offshore}''"
            )
        if (
            not download_receita_offshore_funcionou
            and not download_auc_offshore_funcionou
        ):
            print(
                f"O download de nenhuma das bases offshore funcionou! Verifique o erro na base de AuC -> '''{df_auc_offshore}''' e o erro na base de receita ->   '''{df_receita_offshore}'''"
            )
        envia_email_erro_backoffice(
            drive_microsoft,
            df_socios,
            df_contas_criadas_autamoticamente,
            df_contas_sem_associacao,
            df_criacao_contas,
            df_contas_sem_comercial,
            contas_associadas,
            id_usuario_envio_email,
            bucket_lake_gold,
            path_htmls,
        )


####FUNCAO MAIN#######
def criar_contas_hubspot_func(**kwargs):
    # CONSTANTES E DRIVERS #
    PATH_HTMLS = os.path.join(
        os.environ.get("BASE_PATH_DAGS"),
        "utils",
        "DagCriarContasHubspot",
        "arquivos_html_emails",
    )

    bucket_lake_gold = get_bucket_name("lake-gold")
    bucket_lake_bronze = get_bucket_name("lake-bronze")

    hoje = date.today()
    mes_passado = hoje - pd.DateOffset(months=1)
    hoje = pd.to_datetime(hoje)

    dict_credencias_microsft = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive_id = kwargs["GUILHERME_DRIVE_ID"]

    drive_microsoft = OneGraphDrive(
        dict_credencias_microsft["client_id"],
        dict_credencias_microsft["client_secret"],
        dict_credencias_microsft["tenant_id"],
        drive_id,
    )
    id_microsoft_usuario_tecnologia = "d64bcd7f-c47a-4343-b0b4-73f4a8e927a5"

    token_hubspot = json.loads(get_secret_value("prd/apis/hubspot"))["token"]
    headers = {
        "authorization": f"Bearer {token_hubspot}",
        "content-type": "application/json",
        "accept": "application/json",
    }
    cliente_hubspot = hubspot.Client.create(access_token=token_hubspot)
    propriedades = ["account_conta_btg"]
    public_object_search_request = PublicObjectSearchRequest(
        filter_groups=[
            {
                "filters": [
                    {
                        "value": None,
                        "propertyName": "account_conta_btg",
                        "operator": "EQ",
                    }
                ]
            }
        ],
        properties=propriedades,
        limit=1,
        after=0,
    )

    ###### EXECUÇÂO COMPLETA DO PROCESSO #########
    # SEQUÊNCIA DE CARREGAMENTO E TRATAMENTO DOS DADOS QUE SERÃO USADOS NA CRIAÇÃO DAS CONTAS

    df_contatos = carrega_base_contatos_hubspot(bucket_lake_bronze, hoje)
    df_base_btg = carrega_base_contas_btg(bucket_lake_bronze, hoje)
    df_contas_hubspot, df_contas_hubspot_cp = carrega_dados_contas_hubspot(
        cliente_hubspot, df_base_btg
    )

    df_contas_criadas_por_usuarios = estabelece_contas_sem_data_vinculacao(
        df_contas_hubspot, df_base_btg
    )
    df_mudancas_contas_novas = estabelece_mudancas_contas_novas_entre_btg_hubspot(
        df_contas_hubspot, df_base_btg, df_contas_criadas_por_usuarios
    )

    df_contas_por_assessor = carrega_base_assessores(bucket_lake_bronze, hoje)
    df_nomes_assessores = estabelece_df_nomes_assessores(df_contas_por_assessor)
    df_socios = carrega_base_socios(bucket_lake_gold)

    df_socios_com_ids = relaciona_id_btg_id_hubspot_base_socios(
        df_socios, df_nomes_assessores
    )
    df_contas_sem_comercial = estabelece_contas_sem_comercial(df_contas_hubspot_cp)
    df_contas_sem_associacao = estabelece_contas_sem_associacao(df_contas_hubspot_cp)
    df_mudancas_contas_novas = estabelece_mudancas_contas_novas_entre_btg_hubspot(
        df_contas_hubspot, df_base_btg, df_contas_criadas_por_usuarios
    )
    df_contas_por_assessor_ids_emails = relaciona_id_btg_id_hubspot_contas(
        df_socios_com_ids, df_contas_por_assessor, df_mudancas_contas_novas
    )

    df_criacao_contas = estabelece_df_criacao_contas(
        df_mudancas_contas_novas, df_contas_por_assessor_ids_emails
    )

    (
        contas_criadas,
        contas_modificadas,
        contas_associadas,
        df_contas_criadas_autamoticamente,
    ) = cria_contas_onshore(
        public_object_search_request,
        cliente_hubspot,
        df_criacao_contas,
        df_contas_por_assessor_ids_emails,
        hoje,
        df_contatos,
        df_contas_criadas_por_usuarios,
    )

    ### OFFSHORE ###
    (
        download_receita_offshore_funcionou,
        df_receita_offshore,
    ) = carrega_dados_receita_offshore(drive_microsoft, mes_passado)
    download_auc_offshore_funcionou, df_auc_offshore = carrega_dados_auc_offshore(
        drive_microsoft
    )

    cria_contas_offshore_apos_checar_sucesso_das_bases(
        download_receita_offshore_funcionou,
        download_auc_offshore_funcionou,
        df_receita_offshore,
        df_auc_offshore,
        mes_passado,
        df_socios,
        df_contas_hubspot,
        public_object_search_request,
        cliente_hubspot,
        drive_microsoft,
        hoje,
        df_contas_criadas_autamoticamente,
        df_criacao_contas,
        df_contas_sem_associacao,
        df_contas_sem_comercial,
        contas_associadas,
        id_microsoft_usuario_tecnologia,
        bucket_lake_gold,
        PATH_HTMLS,
    )
