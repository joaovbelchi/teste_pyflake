import time
from datetime import date
import hubspot
import pandas as pd
import numpy as np
from pandas import DataFrame
from utils.bucket_names import get_bucket_name
from utils.secrets_manager import get_secret_value
from deltalake import DeltaTable, write_deltalake
from utils.mergeasof import fill_retroativo, remover_antes_entrada
from utils.OneGraph.drive import OneGraphDrive
from datetime import timedelta
import json
from utils.msteams import msteams_qualidade_dados
import boto3
import pytz
from datetime import datetime
from hubspot.crm.objects import BatchInputSimplePublicObjectBatchInput


def leitura_zip(s3_bucket_landing: str, s3_object_key: str) -> DataFrame:
    """Função para carregamento de arquivos zip

    Args:
        s3_bucket_landing (str): Nome do bucket da camada Landing do Lake
        s3_object_key (str): Chave do objeto que se deseja carregar

    Returns:
        DataFrame: Dataframe pronto para processamento
    """
    return pd.read_csv(
        f"s3://{s3_bucket_landing}/{s3_object_key}",
        compression="zip",
        dtype_backend="pyarrow",
    )


def escrita_silver_contas(data_escrita: date):
    """Gera a tabela de contas do BTG na camada Silver do Lake.
    Resultado de um merge entre as tabelas da bronze: dados_cadastrais,
    dados_onboarding e contas_por_assessor

    Args:
        data_escrita (date): A data em que foi realizada a escrita
        dos dados fonte na camada Bronze.
    """
    ano_particao = data_escrita.strftime("%Y")
    mes_particao = data_escrita.strftime("%m").lstrip("0")

    bucket_bronze = get_bucket_name("lake-bronze")
    bucket_silver = get_bucket_name("lake-silver")

    rename_dicts = {
        "dados_cadastrais": {
            "nr_conta": "carteira",
            "date_escrita": "date_escrita",
            "hora_escrita": "hora_escrita",
            "nome_completo": "nome_completo",
            "dt_nascimento": "data_nascimento",
            "profissao": "profissao",
            "estado_civil": "estado_civil",
            "celular": "celular",
            "telefone": "telefone",
            "email": "email",
            "documento_cpf": "documento_cpf",
            "documento_tipo": "documento_tipo",
            "documento": "documento",
            "documento_dt_emissao": "documento_data_emissao",
            "endereco_cidade": "endereco_cidade",
            "endereco_completo": "endereco",
            "endereco_complemento": "endereco_complemento",
            "endereco_estado": "endereco_estado",
            "endereco_cep": "endereco_cep",
            "suitability": "suitability",
            "tipo_cliente": "tipo_cliente",
            "status": "status",
            "dt_abertura": "data_abertura",
            "dt_encerramento": "data_encerramento",
            "vl_pl_declarado": "pl_declarado",
            "vl_rendimento_total": "rendimento_total",
            "vl_rendimento_anual": "rendimento_anual",
            "dt_primeiro_investimento": "data_primeiro_investimento",
        },
        "dados_onboarding": {
            "nr_conta": "carteira",
            "vl_moveis": "moveis",
            "vl_imoveis": "imoveis",
            "vl_investimentos": "investimentos",
            "vl_previdencia": "previdencia",
            "vl_outros": "outros",
        },
        "contas_por_assessor": {
            "account": "carteira",
            "sg_cge": "cd_cge_officer",
            "bond_date": "ts_criado",
        },
    }

    # Leitura de DataFrames da bronze (com filtro de última escrita)
    dataframes_dict = {
        "dados_cadastrais": None,
        "dados_onboarding": None,
        "contas_por_assessor": None,
    }
    for tab in dataframes_dict.keys():
        dt = DeltaTable(f"s3://{bucket_bronze}/btg/webhooks/downloads/{tab}")
        df = dt.to_pandas(
            partitions=[
                ("ano_particao", "=", ano_particao),
                ("mes_particao", "=", mes_particao),
            ]
        )
        df = df[df["timestamp_dagrun"].dt.date == data_escrita]
        df = df[df["timestamp_dagrun"] == df["timestamp_dagrun"].max()]
        df = df[df["timestamp_escrita_bronze"] == df["timestamp_escrita_bronze"].max()]
        df["date_escrita"] = df["timestamp_dagrun"].dt.date
        df["hora_escrita"] = df["timestamp_dagrun"].dt.hour
        dataframes_dict[tab] = df

        # Renomeia colunas de DataFrames
        dataframes_dict[tab].rename(columns=rename_dicts[tab], inplace=True)
        dataframes_dict[tab] = dataframes_dict[tab][rename_dicts[tab].values()]

    # Merge dos DataFrames
    df = dataframes_dict["dados_cadastrais"].merge(
        dataframes_dict["dados_onboarding"], how="left", on="carteira"
    )
    df = df.merge(dataframes_dict["contas_por_assessor"], how="left", on="carteira")

    # Aplica schema PyArrow
    schema = {
        "carteira": "string",
        "date_escrita": "string",
        "hora_escrita": "string",
        "nome_completo": "string",
        "data_nascimento": "datetime64[us]",
        "profissao": "string",
        "estado_civil": "string",
        "celular": "string",
        "telefone": "string",
        "email": "string",
        "documento_cpf": "string",
        "documento_tipo": "string",
        "documento": "string",
        "documento_data_emissao": "datetime64[us]",
        "endereco_cidade": "string",
        "endereco": "string",
        "endereco_complemento": "string",
        "endereco_estado": "string",
        "endereco_cep": "string",
        "suitability": "string",
        "tipo_cliente": "string",
        "status": "string",
        "data_abertura": "datetime64[us]",
        "data_encerramento": "datetime64[us]",
        "pl_declarado": "float64",
        "rendimento_total": "float64",
        "rendimento_anual": "float64",
        "data_primeiro_investimento": "datetime64[us]",
        "cd_cge_officer": "string",
        "ts_criado": "datetime64[us]",
        "moveis": "float64",
        "imoveis": "float64",
        "investimentos": "float64",
        "previdencia": "float64",
        "outros": "float64",
    }

    df["ts_criado"] = pd.to_datetime(df["ts_criado"], format="mixed").dt.tz_convert(
        None
    )

    # Garante que strings de valores numéricos não serão
    # convertidos para string com casas decimais
    for col, dtype in schema.items():
        if "float" in str(df.dtypes[col]) and "string" in dtype:
            df[col] = df[col].astype("int64[pyarrow]")

        df[col] = df[col].astype(dtype)

    df = df[schema.keys()]

    dt_path = f"s3://{bucket_silver}/btg/contas"

    partitions = [("date_escrita", "=", str(data_escrita))]

    write_deltalake(
        dt_path,
        df,
        mode="overwrite",
        partition_by=[part[0] for part in partitions],
        partition_filters=partitions,
    )
    print(
        f"Overwrite realizado na camada Silver para as partições a seguir: {partitions}"
    )

    # Compactação da partição
    dt = DeltaTable(dt_path)
    dt.optimize.compact(partition_filters=partitions)
    print("Partição compactada")

    # Vacuum
    dt.vacuum(dry_run=False)
    print("Arquivos antigos apagados")


def escrita_gold_contas(data_escrita: date):
    """Gera a tabela de contas do BTG na camada Gold do Lake.
    É resultado do último incremento na tabela Silver, com filtros
    de colunas e linhas aplicados.

    Args:
        data_escrita (date): A data em que foi realizada a escrita
        dos dados fonte na camada Silver.
    """

    # Coleta nomes de buckets
    bucket_silver = get_bucket_name("lake-silver")
    bucket_gold = get_bucket_name("lake-gold")

    # Realiza leitura da Silver
    dt = DeltaTable(f"s3://{bucket_silver}/btg/contas")
    df = dt.to_pandas(
        partitions=[
            ("date_escrita", "=", str(data_escrita)),
        ]
    )

    # Aplica schema
    schema = {
        "carteira": "string",
        "date_escrita": "string",
        "hora_escrita": "string",
        "nome_completo": "string",
        "data_nascimento": "datetime64[us]",
        "profissao": "string",
        "estado_civil": "string",
        "celular": "string",
        "telefone": "string",
        "email": "string",
        "documento_cpf": "string",
        "documento_tipo": "string",
        "documento": "string",
        "documento_data_emissao": "datetime64[us]",
        "endereco_cidade": "string",
        "endereco": "string",
        "endereco_complemento": "string",
        "endereco_estado": "string",
        "endereco_cep": "string",
        "suitability": "string",
        "tipo_cliente": "string",
        "status": "string",
        "data_abertura": "datetime64[us]",
        "data_encerramento": "datetime64[us]",
        "pl_declarado": "double",
        "rendimento_total": "double",
        "rendimento_anual": "double",
        "data_primeiro_investimento": "datetime64[us]",
    }
    df = df[schema.keys()]
    for col in df.columns:
        df[col] = df[col].astype(schema[col])

    # Aplica filtro de contas encerradas e remove duplicatas
    df = df[df["status"] != "ENCERRADA"]
    df = df.drop_duplicates("carteira")

    # Grava resultado na Gold
    df.to_parquet(f"s3a://{bucket_gold}/btg/contas/contas.parquet")


def carrega_base_btg(data_interval_end: str = None):
    """Carrega a base BTG da silver, carregando um periodo de 7 dias para tras por default
       ou caso seja passado um parametro data_retroativa, carrega a partir da data passada

    Args:
        data_retroativa (str): Data em que fará a carga retroativa, no formato YYYY-MM-DD

    Returns: Base de contas BTG da silver 7 dias para trás ou a partir da data passada
    """
    bucket_silver = get_bucket_name("lake-silver")
    data_interval_end = pd.to_datetime(data_interval_end)
    days_to_load = data_interval_end - timedelta(days=7)
    days_to_load = days_to_load.strftime("%Y-%m-%d")

    partitions = [
        ("date_escrita", ">=", days_to_load),
    ]
    path = f"s3a://{bucket_silver}/btg/contas/"
    dt = DeltaTable(path)
    return dt.to_pandas(partitions=partitions)


def limpa_base_btg(base: DataFrame):
    """Altera datatypes e remove duplicatas da base BTG

    Args:
        df (pd.DataFrame): DataFrame com as contas do BTG

    Returns: Base de contas BTG com os datatypes corretos e sem duplicatas
    """
    df = base.copy()
    df = df.sort_values(by=["carteira", "date_escrita", "hora_escrita"])

    df = df.drop_duplicates(
        subset=["carteira", "date_escrita", "hora_escrita"], keep="last"
    )
    df["carteira"] = df["carteira"].astype(str)
    df["date_escrita"] = pd.to_datetime(df["date_escrita"])
    return df


def remove_encerradas_que_se_repetem(df: DataFrame):
    """Retira da base BTG as contas encerradas que se repetem, mantendo apenas a sua primeira ocorrencia

    Args:
        df (pd.DataFrame): DataFrame com as contas do BTG da silver

    Returns: Base de contas BTG sem as contas encerradas que se repetem, apenas a primeira ocorrencia é mantida
    """

    # Filtra apenas as linhas com 'status' igual a 'ENCERRADA'
    contas_encerradas = df[df["status"] == "ENCERRADA"]

    # Encontra os índices mínimos para cada grupo de 'Conta'
    indices_minimos = contas_encerradas.groupby("carteira")["date_escrita"].idxmin()

    # Seleciona as linhas correspondentes aos índices mínimos no DataFrame original
    result_df = df.loc[indices_minimos][
        ["carteira", "date_escrita", "status", "ts_criado", "data_encerramento"]
    ]

    # Agora, result_df contém as linhas desejadas do DataFrame original

    resto = df[df["status"] != "ENCERRADA"][
        [
            "carteira",
            "date_escrita",
            "status",
            "ts_criado",
            "data_encerramento",
            "hora_escrita",
        ]
    ]

    sem_encerradas_duplicadas = pd.concat([resto, result_df])
    sem_encerradas_duplicadas.sort_values(by="date_escrita", inplace=True)
    sem_encerradas_duplicadas.reset_index(drop=True, inplace=True)
    return sem_encerradas_duplicadas


def calcular_entradas(base_btg: DataFrame, base_contas: DataFrame):
    """
    Calcula as contas que entraram na base do BTG, após encontrar as entradas, verifica se elas ja estão presentes na base
    para nao duplicar as contas

    Args:
        base_btg (pd.DataFrame): DataFrame com as contas do BTG da silver
        base_contas (pd.DataFrame): Contas One

    Returns: Dataframe com as contas que entraram na base do BTG
    """
    base_contas_copia = base_contas.copy()
    base_copia_btg = base_btg.copy()

    base_contas_copia["Conta"] = base_contas_copia["Conta"].astype(str)
    base_copia_btg["carteira"] = base_copia_btg["carteira"].astype(str)

    base_copia_btg["min_date_escrita"] = base_copia_btg.groupby("carteira")[
        "date_escrita"
    ].transform("min")

    data_minima = base_copia_btg["min_date_escrita"].min()

    df_contas_in = base_copia_btg[
        (base_copia_btg["min_date_escrita"] != data_minima)
        & (base_copia_btg["min_date_escrita"] == base_copia_btg["date_escrita"])
    ]

    df_contas_in = df_contas_in.sort_values(
        ["carteira", "date_escrita", "hora_escrita"]
    ).drop_duplicates(["carteira", "date_escrita"], keep="first")

    # Filtra contas que entraram recentemente e
    # que ainda não existem ou estão fechadas na base
    # account_in_out
    base_contas_copia["data_maxima_conta"] = base_contas_copia.groupby("Conta")[
        "Data"
    ].transform("max")
    contas_abertas = base_contas_copia[
        (base_contas_copia["Abriu/Fechou"] == "Abriu")
        & (base_contas_copia["data_maxima_conta"] == base_contas_copia["Data"])
    ]["Conta"]
    df_contas_in = df_contas_in[~df_contas_in["carteira"].isin(contas_abertas)]

    df_contas_in.rename(columns={"carteira": "Conta"}, inplace=True)

    df_contas_in = df_contas_in[
        [
            "Conta",
            "date_escrita",
            "ts_criado",
        ]
    ]

    df_contas_in["Data"] = df_contas_in["ts_criado"].fillna(
        df_contas_in["date_escrita"]
    )

    # a data de entrada sempre é o ts_criado, ah nao ser que a diferenca entre o ts_criado e a data de escrita seja maior que 30 dias, ai a data de entrada é a data de escrita
    mask = (df_contas_in["date_escrita"] - df_contas_in["ts_criado"]).dt.days > 30
    df_contas_in.loc[mask, "Data"] = df_contas_in.loc[mask, "date_escrita"]

    df_contas_in.drop(columns=["ts_criado"], inplace=True)

    df_contas_in["Abriu/Fechou"] = "Abriu"
    df_contas_in["Data"] = pd.to_datetime(df_contas_in["Data"]).dt.strftime("%Y-%m-%d")

    df_contas_in["date_escrita"] = pd.to_datetime(
        df_contas_in["date_escrita"]
    ).dt.strftime("%Y-%m-%d")

    return df_contas_in


def calcular_saidas(base_btg: DataFrame, base_contas: DataFrame):
    """
    Calcula as contas que sairam na base do BTG, após encontrar as saidas, verifica se elas ja estão presentes na base
    para nao duplicar as contas

    Args:
        base_btg (pd.DataFrame): DataFrame com as contas do BTG da silver
        base_contas (pd.DataFrame): Contas One

    Returns: Dataframe com as contas que sairam na base do BTG
    """
    base_contas_copia = base_contas.copy()
    base_copia_btg = base_btg.copy()

    base_contas_copia["Conta"] = base_contas_copia["Conta"].astype(str)
    base_copia_btg["carteira"] = base_copia_btg["carteira"].astype(str)

    base_copia_btg["max_date_escrita"] = base_copia_btg.groupby("carteira")[
        "date_escrita"
    ].transform("max")

    data_maxima = base_copia_btg["max_date_escrita"].max()

    df_contas_out = base_copia_btg[
        (
            (base_copia_btg["max_date_escrita"] != data_maxima)
            & (base_copia_btg["max_date_escrita"] == base_copia_btg["date_escrita"])
        )
        | (base_copia_btg["status"] == "ENCERRADA")
    ]

    df_contas_out = df_contas_out.sort_values(
        ["carteira", "date_escrita", "hora_escrita"]
    ).drop_duplicates(["carteira", "date_escrita"], keep="last")

    # Filtra contas que saíram recentemente e
    # que constam como abertas na base account_in_out

    base_contas_copia["data_maxima_conta"] = base_contas_copia.groupby("Conta")[
        "Data"
    ].transform("max")

    contas_fechadas = base_contas_copia[
        (base_contas_copia["Abriu/Fechou"] == "Fechou")
        & (base_contas_copia["data_maxima_conta"] == base_contas_copia["Data"])
    ]["Conta"]
    df_contas_out = df_contas_out[~df_contas_out["carteira"].isin(contas_fechadas)]

    df_contas_out.rename(columns={"carteira": "Conta"}, inplace=True)
    # Cria dataframe com contas que saíram, operacional e comercial atual
    df_contas_out["Data"] = df_contas_out["date_escrita"]
    filtro = df_contas_out["status"] == "ENCERRADA"
    df_contas_out.loc[filtro, "Data"] = df_contas_out["data_encerramento"]
    df_contas_out = df_contas_out[
        [
            "Conta",
            "date_escrita",
            "Data",
        ]
    ]
    df_contas_out["Abriu/Fechou"] = "Fechou"
    df_contas_out["Data"] = pd.to_datetime(df_contas_out["Data"])
    df_contas_out["Data"] = df_contas_out["Data"].dt.strftime("%Y-%m-%d")

    df_contas_out["date_escrita"] = pd.to_datetime(
        df_contas_out["date_escrita"]
    ).dt.strftime("%Y-%m-%d")

    return df_contas_out


def preenche_comerciais(contas: DataFrame):
    """
    Preenche os campos de comercial, operacional RF, operacional RV, Origem e Grupo familiar

    Args:
        contas (pd.DataFrame): Contas One

    Returns: Dataframe Contas One com os comerciais preenchidos
    """

    bucket_silver = get_bucket_name("lake-silver")
    bucket_gold = get_bucket_name("lake-gold")

    df_advisors = pd.read_parquet(
        f"s3a://{bucket_silver}/api/btg/crm/ContasHistorico/ContasHistorico.parquet"
    )

    df_advisors = df_advisors.loc[
        :,
        [
            "Conta",
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "Grupo familiar",
            "Nome",
            "Id GF",
            "Origem do cliente",
            "Data",
            "Id Conta",
            "Contato/Empresa",
            "Origem 1",
            "Origem 2",
            "Origem 3",
            "Origem 4",
        ],
    ].copy()

    df_advisors["Data"] = pd.to_datetime(df_advisors["Data"])
    df_advisors["Conta"] = df_advisors["Conta"].astype(str)

    df_contrata = pd.read_parquet(f"s3://{bucket_gold}/api/one/socios/socios.parquet")
    df_contrata["Data início"] = pd.to_datetime(df_contrata["Data início"])
    df_contrata["Data Saída"] = pd.to_datetime(df_contrata["Data Saída"])
    df_contrata_red = df_contrata[["Nome_bd", "Data início", "Data Saída"]]

    df_contas_cp = contas.copy()  # Cria Backup para fill com merge sof reverso

    df_contas_cp["Conta"] = df_contas_cp["Conta"].astype(str)
    df_contas_cp["Data"] = pd.to_datetime(df_contas_cp["Data"])
    df_contas_cp.sort_values(by=["Data"], inplace=True)

    df_contas = contas.copy()
    df_contas["Conta"] = df_contas["Conta"].astype(str)
    df_contas["Data"] = pd.to_datetime(df_contas["Data"])
    df_contas.sort_values(by=["Data"], inplace=True)

    df_advisors.sort_values(by=["Data"], inplace=True)
    df_contas = pd.merge_asof(
        df_contas, df_advisors, on="Data", by="Conta", direction="backward"
    )  # Cria o merge_asof
    temp = pd.merge_asof(
        df_contas_cp, df_advisors, on="Data", by="Conta", direction="forward"
    )  # Cria o merge_asof reverso
    df_contas = fill_retroativo(
        df_contas, temp
    )  # Usa o merge_asof reverso, ffill e bfill para preencher nulos
    df_contas = df_contas.sort_values("Data").reset_index(drop=True)  # Ordena

    # Caso exista um valor definido para um sócio antes de sua entrada, essse valor é removido
    df_contas = remover_antes_entrada(df_contas, df_contrata_red)

    return df_contas[
        [
            "Conta",
            "Abriu/Fechou",
            "Data",
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "Origem 1",
            "Origem 2",
            "Origem 3",
            "Grupo familiar",
            "date_escrita",
        ]
    ]


def atualiza_contas_in_out(date_interval_end: str = None):
    """
    Função incremental que calcula as entradas e saídas de contas e acrescenta ao dataframe de contas historico
    A funcao possui um parametro date_interval_end que é opcional, esse parametro é uma string de data que deve ser passada no formato 'YYYY-MM-DD'
    para quando ocorrer algum erro na base de contas, ao passarmos o dia em que deu problema, a funcao ira reconstruir a base dessa data em diante

    Args: (date_interval_end): Data em que ocorreu o erro na base de contas, no formato 'YYYY-MM-DD'

    Returns: Dataframe com as contas historico atualizado de forma incremental
    """

    bucket_silver = get_bucket_name("lake-silver")
    contas_one = pd.read_parquet(
        f"s3a://{bucket_silver}/btg/api/accounts_in_out/accounts_in_out.parquet"
    )
    contas_one.drop(
        columns=[
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "Origem 1",
            "Origem 2",
            "Origem 3",
            "Grupo familiar",
        ],
        inplace=True,
    )
    contas_one["Conta"] = contas_one["Conta"].astype(str)
    contas_one["Data"] = pd.to_datetime(contas_one["Data"]).dt.strftime("%Y-%m-%d")
    contas_one["date_escrita"] = pd.to_datetime(contas_one["date_escrita"])

    df = carrega_base_btg(date_interval_end)
    df = limpa_base_btg(df)
    df = remove_encerradas_que_se_repetem(df)
    date_interval_end = pd.to_datetime(date_interval_end)
    contas_one = contas_one.loc[contas_one["date_escrita"] <= date_interval_end, :]
    contas_one["date_escrita"] = pd.to_datetime(contas_one["date_escrita"]).dt.strftime(
        "%Y-%m-%d"
    )

    df_contas_entraram = calcular_entradas(df, contas_one)
    df_contas_sairam = calcular_saidas(df, contas_one)

    accounts_in_out_final = pd.concat(
        [contas_one, df_contas_sairam, df_contas_entraram]
    )
    accounts_in_out_final_preenchido = preenche_comerciais(accounts_in_out_final)

    accounts_in_out_sem_duplicadas = accounts_in_out_final_preenchido.drop_duplicates()

    accounts_in_out_sem_duplicadas.to_parquet(
        f"s3a://{bucket_silver}/btg/api/accounts_in_out/accounts_in_out.parquet"
    )


def verifica_qualidade_contas_in_out(
    date_interval_end,
    s3_bucket: str,
    s3_table_key: str,
    s3_filename: str,
    webhook: str,
):
    bucket_silver = get_bucket_name("lake-silver")
    contas_in_out = pd.read_parquet(f"s3://{bucket_silver}/btg/api/accounts_in_out/")

    contas_in_out["Conta"] = contas_in_out["Conta"].astype(str)
    contas_in_out["Data"] = pd.to_datetime(contas_in_out["Data"])

    contas_btg = carrega_base_btg(date_interval_end)
    contas_btg = limpa_base_btg(contas_btg)
    contas_btg_hoje = contas_btg.loc[
        contas_btg["date_escrita"] == contas_btg["date_escrita"].max()
    ]

    contas_ativas = contas_btg_hoje[contas_btg_hoje["status"] != "ENCERRADA"][
        "carteira"
    ].unique()

    contas_in_out.sort_values(by=["Conta", "Data", "Abriu/Fechou"], inplace=True)
    ultima_ocorrencia_conta = contas_in_out.groupby("Conta").last().reset_index()
    contas_abertas = ultima_ocorrencia_conta[
        ultima_ocorrencia_conta["Abriu/Fechou"] == "Abriu"
    ]["Conta"].unique()

    diferenca_account_in_out = set(contas_ativas) - set(contas_abertas)
    diferenca_btg = set(contas_abertas) - set(contas_ativas)

    if diferenca_account_in_out or diferenca_btg:
        s3_object_key = f"{s3_table_key}/{s3_filename}"
        s3 = boto3.client("s3")
        last_modified = s3.get_object(
            Bucket=s3_bucket,
            Key=s3_object_key,
        )["LastModified"].isoformat()

        # Estruturando log de qualidade
        log_qualidade_json = json.dumps(
            {
                "bucket": s3_bucket,
                "tabela": s3_table_key,
                "Contas_analisar_acount_in_out": list(diferenca_account_in_out),
                "Contas_analisar_BTG": list(diferenca_btg),
                "chave_objeto_s3": s3_object_key,
                "processado_em": last_modified,
                "criado_em": datetime.now(tz=pytz.utc).isoformat(),
            },
            ensure_ascii=False,
        )

        # Cria nome do objeto do log no S3
        filename_sem_extensao = s3_filename.split(".")[0]
        qualidade_object_key = (
            f"qualidade_dados/accounts_in_out/" f"{filename_sem_extensao}.json"
        )

        s3.put_object(
            Body=log_qualidade_json,
            Bucket=s3_bucket,
            Key=qualidade_object_key,
        )
        msteams_qualidade_dados(log_qualidade_json, webhook)


########## OFFSHORE ##################


def carrega_base_offshore_filtrada(date_interval_end: str, drive_id: str):
    """Carrega base Offshore do One Drive e filtra para os ultimos 3 meses

    Args:
        date_interval_end (str): data de execução da Dag
        drive_id (str): id do drive do One Drive

    Returns:
        _type_: pandas.DataFrame
    """
    onedrive_path = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "Base_OffShore.xlsx",
    ]

    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)
    base_offshore = drive.download_file(path=onedrive_path, to_pandas=True)

    base_offshore.rename(columns={"Acc Number": "Conta"}, inplace=True)
    base_offshore["Conta"] = base_offshore["Conta"].astype(str)
    base_offshore["Data"] = pd.to_datetime(base_offshore["Data"])
    date_interval_end = pd.to_datetime(date_interval_end)
    date_interval_end = date_interval_end.replace(day=1)

    # seleciona um periodo de 3 meses, já que a base Offshore é mensal
    days_to_load = date_interval_end - pd.DateOffset(months=3)
    return base_offshore[base_offshore["Data"] >= days_to_load]


def entrada_saida(base_offshore: pd.DataFrame, datas_base: np.array):
    """Calcula entrada e saida de contas

    Args:
        base_offshore (pd.DataFrame): base de offshore do AAI
        datas_base (np.array): numpy array com as datas em que iremos consultar a base para calcular as entradas e saidas

    Returns:
        _type_: pd.DataFrame
    """
    df_offshore = base_offshore.copy()
    df_offshore["Conta"] = df_offshore["Conta"].astype(str)
    df_contas_abertas_fechadas = pd.DataFrame()
    for data in range(len(datas_base)):
        # Ignora a primeira data, pois sempre comparamos um dia com o anterior, e a primeira data não tem anterior
        if data == 0:
            continue

        df_concat = df_offshore.loc[
            (df_offshore.Data <= datas_base[data])
            & (df_offshore.Data >= datas_base[data - 1])
        ]
        df_concat["Conta"] = df_concat["Conta"].astype(str)

        df_concat["rank"] = df_concat.groupby("Conta")["Conta"].transform("count")

        df_contas_abertas = df_concat.loc[
            (df_concat.Data == datas_base[data]) & (df_concat["rank"] == 1)
        ]

        df_contas_fechadas = df_concat.loc[
            (df_concat.Data == datas_base[data - 1]) & (df_concat["rank"] == 1)
        ]

        df_concat = df_concat.loc[df_concat["rank"] != 1]

        df_concat = df_concat.drop_duplicates(subset=["Conta"], keep=False)

        df_contas_abertas = df_contas_abertas.loc[:, ["Conta", "Data"]]
        df_contas_abertas["Abriu/Fechou"] = "Abriu"

        df_contas_fechadas = df_contas_fechadas.loc[:, ["Conta", "Data"]]
        df_contas_fechadas["Abriu/Fechou"] = "Fechou"

        df_contas_abertas_fechadas = pd.concat(
            [df_contas_abertas, df_contas_fechadas, df_contas_abertas_fechadas]
        )

    df_contas_abertas_fechadas = df_contas_abertas_fechadas.sort_values(by="Data")
    return df_contas_abertas_fechadas


def carrega_base_historica_filtrada(date_interval_end: str):
    """Carrega a base historica de entrada e saida de contas OffShore e filtra para os ultimos 3 meses

    Args:
        date_interval_end (str): data de execução da Dag

    Returns:
        _type_: pd.dataFrame
    """
    bucket_lake_gold = get_bucket_name("lake-gold")
    historico = pd.read_parquet(
        f"s3://{bucket_lake_gold}/btg/accounts_in_out_offshore/"
    )
    historico["Conta"] = historico["Conta"].astype(str)
    historico["Data"] = pd.to_datetime(historico["Data"])
    date_interval_end = pd.to_datetime(date_interval_end)
    date_interval_end = date_interval_end.replace(day=1)

    # seleciona um periodo de 3 meses, já que a base Offshore é mensal
    days_to_load = date_interval_end - pd.DateOffset(months=3)
    return historico[historico["Data"] <= days_to_load]


def atualiza_contas_in_out_offshore(date_interval_end: str, drive_id: str):
    """Função principal que funciona de forma incremental para calcular entrada e saida das contas

    Args:
        date_interval_end (str): data de execução da Dag
        drive_id (str): id do drive do One Drive
    """
    bucket_lake_gold = get_bucket_name("lake-gold")
    contas_offshore = carrega_base_offshore_filtrada(date_interval_end, drive_id)
    historico_filtrado = carrega_base_historica_filtrada(date_interval_end)
    historico_filtrado.drop(
        columns=[
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "Origem 1",
            "Origem 2",
            "Origem 3",
            "Grupo familiar",
        ],
        inplace=True,
    )
    contas_offshore = contas_offshore.sort_values("Data")
    datas = contas_offshore["Data"].unique()
    df_entrada_saida = entrada_saida(contas_offshore, datas)
    df_entrada_saida_final = pd.concat([df_entrada_saida, historico_filtrado])
    df_final_preenchido_comerciais = preenche_comerciais(df_entrada_saida_final)

    df_final_preenchido_comerciais.to_parquet(
        f"s3://{bucket_lake_gold}/btg/accounts_in_out_offshore/accounts_in_out_offshore.parquet"
    )


def atualiza_status_contas_crm(timestamp_dagrun: str):
    """Atualiza Status das contas no Hubspot com referencia do accounts_in_out gold.

    Args:
        timestamp_dagrun (str): Timestamp lógico de execução do atualização.
    """
    bucket_gold = get_bucket_name("lake-gold")

    # Lendo base accounts_in_out  mais recente
    timestamp_dagrun = pd.to_datetime(timestamp_dagrun).strftime("%Y-%m-%d")
    df_accounts_in_out = pd.read_parquet(
        f"s3://{bucket_gold}/btg/accounts_in_out/accounts_in_out.parquet"
    )

    df_accounts_in_out["date_escrita"] = pd.to_datetime(
        df_accounts_in_out["date_escrita"]
    )
    df_accounts_in_out = df_accounts_in_out[
        df_accounts_in_out["date_escrita"] == timestamp_dagrun
    ]
    df_accounts_in_out = df_accounts_in_out.rename(
        columns={"Abriu/Fechou": "Abriu_Fechou"}
    )

    # Inciando cabeçalho hubspot
    token = json.loads(get_secret_value("prd/apis/hubspot"))["token"]

    headers = {
        "authorization": f"Bearer {token}",
        "content-type": "application/json",
        "accept": "application/json",
    }
    client = hubspot.Client.create(access_token=token)
    account_object_type = {"code": "2-7418328", "name": "accounts"}

    # Fazendo requisições no crm para adquirir o id de cada conta
    df_contas_crm = client.crm.objects.get_all(
        account_object_type["code"], properties=["account_conta_btg"]
    )

    # Adicionando id da conta no base df_accounts_in_out
    dict_contas_crm = {
        conta.properties["account_conta_btg"].strip(): conta.id
        for conta in df_contas_crm
    }
    df_accounts_in_out["id"] = df_accounts_in_out["Conta"].replace(dict_contas_crm)

    # Criando filtro para atualização em massa
    filtro_para_atualizazao = []
    for row in df_accounts_in_out.itertuples():
        if row.Abriu_Fechou == "Abriu":
            filtro_para_atualizazao.append(
                {
                    "account_status": "ativa",
                    "id": row.id,
                    "properties": {"account_status": "ativa"},
                }
            )
        else:
            filtro_para_atualizazao.append(
                {
                    "account_status": "encerrada",
                    "id": row.id,
                    "properties": {"account_status": "encerrada"},
                }
            )

    # Atualizar no crm o status
    for i in range(
        0, len(filtro_para_atualizazao), 100
    ):  # Há um limite de post bath de 100
        lista_batch_dimensionada = filtro_para_atualizazao[i : i + 100]
        batch_input_simple_public_object_batch_input = (
            BatchInputSimplePublicObjectBatchInput(inputs=lista_batch_dimensionada)
        )
        time.sleep(0.3)
        api_response = client.crm.objects.batch_api.update(
            object_type=account_object_type["code"],
            batch_input_simple_public_object_batch_input=batch_input_simple_public_object_batch_input,
        )
        print(api_response)


SCHEMA_BTG_WEBHOOK_CONTAS_ASSESSOR = {
    "account": {"nomes_origem": ["account"], "data_type": "int64"},
    "bond_date": {"nomes_origem": ["bondDate"], "data_type": "string"},
    "username": {"nomes_origem": ["username"], "data_type": "string"},
    "login": {"nomes_origem": ["login"], "data_type": "string"},
    "sg_cge": {"nomes_origem": ["sgCGE"], "data_type": "int64"},
    "office_cge": {"nomes_origem": ["officeCGE"], "data_type": "int64"},
    "office_name": {"nomes_origem": ["officeName"], "data_type": "string"},
    "office_document": {"nomes_origem": ["officeDocument"], "data_type": "int64"},
}

SCHEMA_BTG_WEBHOOK_DADOS_CADASTRAIS = {
    "nr_conta": {"nomes_origem": ["nr_conta"], "data_type": "int64"},
    "nome_completo": {"nomes_origem": ["nome_completo"], "data_type": "string"},
    "dt_nascimento": {"nomes_origem": ["dt_nascimento"], "data_type": "timestamp[us]"},
    "profissao": {"nomes_origem": ["profissao"], "data_type": "string"},
    "estado_civil": {"nomes_origem": ["estado_civil"], "data_type": "string"},
    "celular": {"nomes_origem": ["celular"], "data_type": "int64"},
    "telefone": {"nomes_origem": ["telefone"], "data_type": "int64"},
    "email": {"nomes_origem": ["email"], "data_type": "string"},
    "documento_cpf": {"nomes_origem": ["documento_cpf"], "data_type": "int64"},
    "documento_tipo": {"nomes_origem": ["documento_tipo"], "data_type": "string"},
    "documento": {"nomes_origem": ["documento"], "data_type": "string"},
    "documento_dt_emissao": {
        "nomes_origem": ["documento_dt_emissao"],
        "data_type": "timestamp[us]",
    },
    "endereco_cidade": {"nomes_origem": ["endereco_cidade"], "data_type": "string"},
    "endereco_completo": {"nomes_origem": ["endereco_completo"], "data_type": "string"},
    "endereco_complemento": {
        "nomes_origem": ["endereco_complemento"],
        "data_type": "string",
    },
    "endereco_estado": {"nomes_origem": ["endereco_estado"], "data_type": "string"},
    "endereco_cep": {"nomes_origem": ["endereco_cep"], "data_type": "string"},
    "suitability": {"nomes_origem": ["suitability"], "data_type": "string"},
    "dt_vencimento_suitability": {
        "nomes_origem": ["dt_vencimento_suitability"],
        "data_type": "timestamp[us]",
    },
    "tipo_cliente": {"nomes_origem": ["tipo_cliente"], "data_type": "string"},
    "status": {"nomes_origem": ["status"], "data_type": "string"},
    "tipo_investidor": {"nomes_origem": ["tipo_investidor"], "data_type": "string"},
    "dt_abertura": {"nomes_origem": ["dt_abertura"], "data_type": "timestamp[us]"},
    "dt_encerramento": {
        "nomes_origem": ["dt_encerramento"],
        "data_type": "timestamp[us]",
    },
    "vl_pl_declarado": {"nomes_origem": ["vl_pl_declarado"], "data_type": "double"},
    "vl_rendimento_total": {
        "nomes_origem": ["vl_rendimento_total"],
        "data_type": "double",
    },
    "vl_rendimento_anual": {
        "nomes_origem": ["vl_rendimento_anual"],
        "data_type": "double",
    },
    "dt_primeiro_investimento": {
        "nomes_origem": ["dt_primeiro_investimento"],
        "data_type": "timestamp[us]",
    },
}

SCHEMA_BTG_WEBHOOK_DADOS_ONBOARDING = {
    "nr_conta": {"nomes_origem": ["nr_conta"], "data_type": "int64"},
    "vl_moveis": {"nomes_origem": ["vl_moveis"], "data_type": "double"},
    "vl_imoveis": {"nomes_origem": ["vl_imoveis"], "data_type": "double"},
    "vl_investimentos": {"nomes_origem": ["vl_investimentos"], "data_type": "double"},
    "vl_previdencia": {"nomes_origem": ["vl_previdencia"], "data_type": "double"},
    "vl_outros": {"nomes_origem": ["vl_outros"], "data_type": "double"},
}
