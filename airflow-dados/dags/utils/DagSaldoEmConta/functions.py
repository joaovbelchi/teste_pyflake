import pandas as pd
import io
from datetime import datetime, timedelta
import json
from utils.OneGraph.mail import Mail
from deltalake import DeltaTable
import re
from typing import Union, Any, Tuple
from utils.usual_data_transformers import (
    formata_numero_para_texto_monetario,
    normaliza_caracteres_unicode,
)
from utils.DagSaldoEmConta.context_script import gera_context_script
from time import sleep
from random import uniform
import numpy as np


def prepara_nome_para_join(string: str) -> str:
    """Trata string para evitar divergências de enconding e diferenças upper/lower, garantindo consistência na junção de colunas com tipo string em DataFrame diferentes.

    Args:
        string (str): String a ser tratada.

    Returns:
        string (str): String tratada.
    """

    string = re.sub(r"\s+", " ", string)
    string = string.upper()
    string = string.replace("Ç", "C")
    string = normaliza_caracteres_unicode(string)

    return string


def carrega_socios() -> pd.DataFrame:
    """Carrega base de sócios.

    Returns:
        df_socios (pd.DataFrame): Dataframe com a base de socios.
    """

    context_script = gera_context_script()

    print("Carregando base de sócios...")

    df_socios = pd.read_parquet(
        f"s3://{context_script['bucket_lake_gold']}/api/one/socios/"
    )

    df_socios = filtrar_renomear_tipar(df_socios, context_script["colunas_socios"])
    df_socios["nome_assessor"] = df_socios["nome_assessor"].apply(
        prepara_nome_para_join
    )
    df_socios["email_assessor"] = df_socios["email_assessor"].str.replace(
        "oneinv.com", "investimentos.one"
    )

    return df_socios


def carrega_contas_btg() -> pd.DataFrame:
    """Carrega base de contas do BTG.
    Returns:
        df_socios (pd.DataFrame): Dataframe com a base de contas do BTG.
    """

    context_script = gera_context_script()

    print("Carregando base de contas do BTG...")

    table_base_contas_btg = DeltaTable(
        f"s3://{context_script['bucket_lake_bronze']}/btg/webhooks/downloads/dados_cadastrais/"
    )
    df_contas_btg = table_base_contas_btg.to_pandas(
        partitions=[
            ("ano_particao", "=", str(context_script["datetime_hoje_brasil"].year)),
            ("mes_particao", "=", str(context_script["datetime_hoje_brasil"].month)),
        ],
    )

    data_mais_recente_dag_contas_btg = df_contas_btg["timestamp_dagrun"].max()

    df_contas_btg = df_contas_btg[
        df_contas_btg["timestamp_dagrun"] == data_mais_recente_dag_contas_btg
    ]

    df_contas_btg = filtrar_renomear_tipar(
        df_contas_btg, context_script["colunas_contas_btg"]
    )

    print(
        f"A base de contas do BTG foi carregada e filtrada com informações do DAG que rodou em: {data_mais_recente_dag_contas_btg}"
    )

    return df_contas_btg


def carrega_clientes_assessores() -> pd.DataFrame:
    """Carrega base de contas de clientes por assessor.

    Returns:
        df_socios (pd.DataFrame): Dataframe com a base de clientes por assessor.
    """

    context_script = gera_context_script()

    print("Carregando base de contas de clientes por assessor...")

    df_clientes_assessores = pd.read_parquet(
        f"s3://{context_script['bucket_lake_silver']}/api/hubspot/clientesassessores"
    )

    data_mais_recente_clientes_assessores = df_clientes_assessores["REGISTRO"].max()
    df_clientes_assessores = df_clientes_assessores[
        df_clientes_assessores["REGISTRO"] == data_mais_recente_clientes_assessores
    ]

    df_clientes_assessores = filtrar_renomear_tipar(
        df_clientes_assessores, context_script["colunas_contas_assessores"]
    )
    df_clientes_assessores["nome_assessor"] = df_clientes_assessores[
        "nome_assessor"
    ].apply(prepara_nome_para_join)
    df_clientes_assessores["nome_operacional_rf"] = df_clientes_assessores[
        "nome_operacional_rf"
    ].apply(prepara_nome_para_join)
    df_clientes_assessores["nome_operacional_rv"] = df_clientes_assessores[
        "nome_operacional_rv"
    ].apply(prepara_nome_para_join)

    print(
        f"A base ClientesAssessores foi carregada e filtrada com informações do DAG que rodou em: {data_mais_recente_clientes_assessores}"
    )

    return df_clientes_assessores


def filtrar_renomear_tipar(df: pd.DataFrame, mappings: dict) -> pd.DataFrame:
    """Trata DataFrame selecionando coolunas, renomeando-as, aplicando-lhes tipos e funções de formatação específicas.

    Args:
        df (pd.DataFrame): DataFrame a ser tratado.
        mappings (dict): Conjunto de filtros a serem aplicados.

    Returns:
        pd.DataFrame: DataFrame tratado.
    """

    df = df[list(mappings.keys())]

    for key in mappings.keys():
        df[key] = df[key].astype(mappings[key]["tipo"])
        if "formatar" in mappings[key]:
            df[key] = df[key].apply(mappings[key]["formatar"])

    df = df.rename(columns={key: value["nome"] for key, value in mappings.items()})

    return df


def atualiza_saldo_em_conta_historico() -> None:
    """Atualiza histórico de saldo em contas.

    Raises:
        Exception: Retorna um aviso se as colunas não forem as corretas (após tratamento).
    """

    context_script = gera_context_script()

    drive_microsoft = context_script["drive_microsoft"]
    df_saldo_em_conta_historico = drive_microsoft.download_file(
        context_script["lista_caminhos_saldo_historico"],
        to_pandas=True,
        **dict(
            sep=";",
            decimal=",",
            dtype=object,
        ),
    )
    df_saldo_em_conta_historico["account"] = df_saldo_em_conta_historico[
        "account"
    ].astype(int)
    df_saldo_em_conta_historico["balance"] = (
        df_saldo_em_conta_historico["balance"].str.replace(",", ".").astype(float)
    )

    df_saldo_em_conta_agora = drive_microsoft.download_file(
        context_script["lista_caminhos_saldo_novos"],
        to_pandas=True,
        **dict(
            skiprows=2,
            dtype=object,
        ),
    )

    # teste de qualidade que sera brevemente reescrito com ge
    lista_coluna_saldo_em_conta_agora = [
        column.capitalize().strip()
        for column in df_saldo_em_conta_agora.columns.to_list()
    ]
    checagem_colunas = ["Saldo", "Nome", "Conta"]
    if set(lista_coluna_saldo_em_conta_agora) - set(checagem_colunas) != set():
        raise Exception(
            f"Esperamos as colunas {checagem_colunas} e as colunas após capitalize e trim são {lista_coluna_saldo_em_conta_agora}."
        )

    df_saldo_em_conta_agora["date"] = datetime.now().date()
    df_saldo_em_conta_agora = df_saldo_em_conta_agora.rename(
        columns={"Conta": "account", "Nome": "name", "Saldo": "balance"}
    )
    df_saldo_em_conta_agora["account"] = df_saldo_em_conta_agora["account"].astype(int)
    df_saldo_em_conta_agora["balance"] = df_saldo_em_conta_agora["balance"].astype(
        float
    )

    df_saldo_em_conta_historico_atualizado = pd.concat(
        [df_saldo_em_conta_historico, df_saldo_em_conta_agora]
    )
    df_saldo_em_conta_historico_atualizado["account"] = (
        df_saldo_em_conta_historico_atualizado["account"].astype(str)
    )

    df_saldo_em_conta_historico_atualizado = (
        df_saldo_em_conta_historico_atualizado.drop_duplicates(
            subset=["account", "date"], keep="last"
        )
    )

    buffer_saldo_em_conta_historico_atualizado = io.BytesIO()
    df_saldo_em_conta_historico_atualizado.to_csv(
        buffer_saldo_em_conta_historico_atualizado,
        sep=";",
        decimal=",",
        index=False,
        lineterminator="",
    )
    buffer_saldo_em_conta_historico_atualizado = (
        buffer_saldo_em_conta_historico_atualizado.getvalue()
    )
    drive_microsoft.upload_file(
        context_script["lista_caminhos_saldo_historico"],
        buffer_saldo_em_conta_historico_atualizado,
    )


def carrega_saldo_em_conta() -> pd.DataFrame:
    """Carrega base de saldo em conta de cada cliente da ONE.

    Returns:
        df_socios (pd.DataFrame): Dataframe com a base de saldo em conta de cada cliente da ONE.
    """

    context_script = gera_context_script()

    print("Carregando base de saldo em conta de cada cliente da ONE...")

    drive_microsoft = context_script["drive_microsoft"]
    df_saldo_em_conta = drive_microsoft.download_file(
        context_script["lista_caminhos_saldo_novos"],
        to_pandas=True,
        **dict(
            skiprows=2,
            dtype=object,
        ),
    )

    context_script["colunas_saldo_em_conta"] = {
        key.capitalize(): value
        for key, value in context_script["colunas_saldo_em_conta"].items()
    }

    df_saldo_em_conta = filtrar_renomear_tipar(
        df_saldo_em_conta, context_script["colunas_saldo_em_conta"]
    )

    df_saldo_em_conta["conta"] = df_saldo_em_conta["conta"].str.lstrip("0")

    return df_saldo_em_conta


def carrega_pl() -> pd.DataFrame:
    """Carrega base de PL de cada cliente da ONE.

    Returns:
        df_socios (pd.DataFrame): Dataframe com a base de PL de cada cliente da ONE.
    """

    context_script = gera_context_script()

    print("Carregando base de PL de cada cliente da ONE...")

    df_pl = pd.read_parquet(f"s3://{context_script['bucket_lake_gold']}/btg/api/PL/")

    df_pl[df_pl["Final do mes"] != True]

    data_mais_recente_dag_pl = df_pl["Data"].max()

    print(
        f"A base de PL foi carregada e filtrada com informações de PL para a data: {data_mais_recente_dag_pl}"
    )

    df_pl = df_pl[df_pl["Data"] == data_mais_recente_dag_pl]
    df_pl = df_pl[df_pl["Mercado"] != "TOTAL"]
    df_pl = df_pl[["Conta", "PL"]]
    df_pl = df_pl.groupby("Conta").sum("PL").reset_index()

    df_pl = filtrar_renomear_tipar(df_pl, context_script["colunas_pl"])

    return df_pl


def obter_dado_correspondente(
    dado: Any,
    coluna_referencia: str,
    coluna_correspondente: str,
    df_dados: pd.DataFrame,
) -> Union[Any, None]:
    """Função para ser aplicada na extração de dados de um DataFrame para outro

    Args:
        dado (Any): Dado do DataFrame de entrada a ser usado na busca.
        coluna_referencia (str): Coluna de busca do DataFrame de entrada.
        coluna_correspondente (str): Coluna do DataFrame de onde os dados serão extraídos.
        df_dados (pd.DataFrame): DataFrame de entrada.

    Returns:
        Any: Pode retornar qualquer dado presente em uma célula de um df_dados de entrada.
    """
    resultado = df_dados[df_dados[coluna_referencia] == dado][coluna_correspondente]
    if not resultado.empty:
        return resultado.values[0]
    else:
        return None


def estabelece_saldos_por_conta_de_cada_assessor(
    df_socios: pd.DataFrame,
    df_clientes_assessores: pd.DataFrame,
    df_saldo_em_conta: pd.DataFrame,
    df_contas_btg: pd.DataFrame,
    df_pl: pd.DataFrame,
    df_dias_de_saldo_negativo_por_conta: pd.DataFrame,
    df_dias_de_saldo_positivo_por_conta: pd.DataFrame,
) -> pd.DataFrame:
    """Gera a base de contatos a ser feito em caso de saldo negativo ou parado em conta.

    Args:
        df_socios (pd.DataFrame): Base tratada de sócios da ONE.
        df_clientes_assessores (pd.DataFrame): Base tratada de clientes por assessor.
        df_saldo_em_conta (pd.DataFrame): Base tratada de saldos em conta dos clientes.
        df_contas_btg (pd.DataFrame): Base tratada de contas do BTG.
        df_pl (pd.DataFrame): Base tratada de PL dos clientes da ONE.
        df_dias_de_saldo_negativo_por_conta (pd.DataFrame): Base tratada de saldos negativos por conta.
        df_dias_de_saldo_positivo_por_conta (pd.DataFrame): Base tratada de saldo positvos por conta.

    Returns:
        df_saldos_por_conta_de_cada_assessor (pd.DataFrame): Base de dados dos clientes com saldo negativo ou parado, relacionados com os contatos que cada comercial, trader ou advisor deve receber.
    """

    context_script = gera_context_script()

    df_saldos_por_conta_de_cada_assessor = pd.merge(
        df_saldo_em_conta, df_contas_btg, how="outer", left_on="conta", right_on="conta"
    )
    df_saldos_por_conta_de_cada_assessor = pd.merge(
        df_saldos_por_conta_de_cada_assessor,
        df_clientes_assessores,
        how="outer",
        left_on="conta",
        right_on="conta",
    )
    df_saldos_por_conta_de_cada_assessor = pd.merge(
        df_saldos_por_conta_de_cada_assessor,
        df_pl,
        how="outer",
        left_on="conta",
        right_on="conta",
    )
    df_saldos_por_conta_de_cada_assessor = pd.merge(
        df_saldos_por_conta_de_cada_assessor,
        df_dias_de_saldo_negativo_por_conta,
        how="outer",
        left_on="conta",
        right_on="conta",
    )
    df_saldos_por_conta_de_cada_assessor = pd.merge(
        df_saldos_por_conta_de_cada_assessor,
        df_dias_de_saldo_positivo_por_conta,
        how="outer",
        left_on="conta",
        right_on="conta",
    )

    df_saldos_por_conta_de_cada_assessor["percent_pl"] = (
        df_saldos_por_conta_de_cada_assessor["saldo"]
        / df_saldos_por_conta_de_cada_assessor["pl"]
    )

    df_saldos_por_conta_de_cada_assessor["email_assessor"] = (
        df_saldos_por_conta_de_cada_assessor["nome_assessor"]
        .apply(
            lambda x: obter_dado_correspondente(
                x, "nome_assessor", "email_assessor", df_socios
            )
        )
        .fillna("")
    )
    df_saldos_por_conta_de_cada_assessor["email_operacional_rf"] = (
        df_saldos_por_conta_de_cada_assessor["nome_operacional_rf"]
        .apply(
            lambda x: obter_dado_correspondente(
                x, "nome_assessor", "email_assessor", df_socios
            )
        )
        .fillna("")
    )
    df_saldos_por_conta_de_cada_assessor["email_operacional_rv"] = (
        df_saldos_por_conta_de_cada_assessor["nome_operacional_rv"]
        .apply(
            lambda x: obter_dado_correspondente(
                x, "nome_assessor", "email_assessor", df_socios
            )
        )
        .fillna("")
    )

    df_saldos_por_conta_de_cada_assessor["telefone_assessor"] = (
        df_saldos_por_conta_de_cada_assessor["nome_assessor"]
        .apply(
            lambda x: obter_dado_correspondente(
                x, "nome_assessor", "telefone_assessor", df_socios
            )
        )
        .fillna("")
    )
    df_saldos_por_conta_de_cada_assessor["telefone_operacional_rf"] = (
        df_saldos_por_conta_de_cada_assessor["nome_operacional_rf"]
        .apply(
            lambda x: obter_dado_correspondente(
                x, "nome_assessor", "telefone_assessor", df_socios
            )
        )
        .fillna("")
    )
    df_saldos_por_conta_de_cada_assessor["telefone_operacional_rv"] = (
        df_saldos_por_conta_de_cada_assessor["nome_operacional_rv"]
        .apply(
            lambda x: obter_dado_correspondente(
                x, "nome_assessor", "telefone_assessor", df_socios
            )
        )
        .fillna("")
    )

    df_saldos_por_conta_de_cada_assessor["area_assessor"] = (
        df_saldos_por_conta_de_cada_assessor["nome_assessor"]
        .apply(
            lambda x: obter_dado_correspondente(x, "nome_assessor", "area", df_socios)
        )
        .fillna("")
    )
    df_saldos_por_conta_de_cada_assessor["area_operacional_rf"] = (
        df_saldos_por_conta_de_cada_assessor["nome_operacional_rf"]
        .apply(
            lambda x: obter_dado_correspondente(x, "nome_assessor", "area", df_socios)
        )
        .fillna("")
    )
    df_saldos_por_conta_de_cada_assessor["area_operacional_rv"] = (
        df_saldos_por_conta_de_cada_assessor["nome_operacional_rv"]
        .apply(
            lambda x: obter_dado_correspondente(x, "nome_assessor", "area", df_socios)
        )
        .fillna("")
    )

    df_saldos_por_conta_de_cada_assessor = df_saldos_por_conta_de_cada_assessor[
        df_saldos_por_conta_de_cada_assessor["saldo"].notna()
    ]

    ### tratando uma conta que aparece sem nenhum assessor
    df_saldos_por_conta_de_cada_assessor[
        ["nome_assessor", "nome_operacional_rf", "nome_operacional_rv"]
    ] = df_saldos_por_conta_de_cada_assessor[
        ["nome_assessor", "nome_operacional_rf", "nome_operacional_rv"]
    ].fillna(
        ""
    )

    df_saldos_por_conta_de_cada_assessor.to_json(
        f"s3://{context_script['bucket_lake_gold']}/api/one/base_notificao_saldos_por_assessor/base_notificao_saldos_por_assessor.json"
    )

    return df_saldos_por_conta_de_cada_assessor


def carrega_dias_de_saldo() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Carrega base de dados com o número de dias em que a conta está ou com saldo positivo ou com saldo positivo.

    Returns:
        df_dias_de_saldo_negativo_por_conta (pd.DataFrame): DataFrame com relacionando cada conta de clientes de ONE com o número de dias em que o saldo está negativo.
        df_dias_de_saldo_positivo_por_conta (pd.DataFrame): DataFrame com relacionando cada conta de clientes de ONE com o número de dias em que o saldo está positivo.
    """

    context_script = gera_context_script()

    print(
        "Carregando base de dados com o número de dias em que a conta está ou com saldo positivo ou com saldo positivo..."
    )

    drive_microsoft = context_script["drive_microsoft"]
    df_dias_de_saldo = drive_microsoft.download_file(
        context_script["lista_caminhos_saldo_historico"],
        to_pandas=True,
        **dict(
            sep=";",
            decimal=",",
            dtype=object,
        ),
    )

    df_dias_de_saldo["account"] = df_dias_de_saldo["account"].astype(int)
    df_dias_de_saldo["date"] = pd.to_datetime(df_dias_de_saldo["date"])
    df_dias_de_saldo["balance"] = (
        df_dias_de_saldo["balance"].str.replace(",", ".").astype(float)
    )

    positivos_hoje = df_dias_de_saldo[
        (df_dias_de_saldo["balance"] > 0)
        & (df_dias_de_saldo["date"] == df_dias_de_saldo["date"].max())
    ]
    negativos_hoje = df_dias_de_saldo[
        (df_dias_de_saldo["balance"] < 0)
        & (df_dias_de_saldo["date"] == df_dias_de_saldo["date"].max())
    ]

    dias_negativos = df_dias_de_saldo[
        (df_dias_de_saldo["account"].isin(negativos_hoje["account"]))
    ]
    dias_positivos = df_dias_de_saldo[
        (df_dias_de_saldo["account"].isin(positivos_hoje["account"]))
    ]

    dias_negativos = dias_negativos.sort_values(["date"], ascending=False)
    dias_positivos = dias_positivos.sort_values(["date"], ascending=False)

    date_balance_negativos = dias_negativos.pivot_table(
        index="date", columns="account", values="balance", aggfunc="sum"
    )
    date_balance_negativos = date_balance_negativos.sort_values(
        ["date"], ascending=False
    )

    date_balance_positivos = dias_positivos.pivot_table(
        index="date", columns="account", values="balance", aggfunc="sum"
    )
    date_balance_positivos = date_balance_positivos.sort_values(
        ["date"], ascending=False
    )

    date_balance_negativos = date_balance_negativos.bfill()

    dict_dias_de_saldo_negativo_por_conta = {
        conta: np.where(
            (date_balance_negativos[conta].values >= 0)
            | (np.isnan(date_balance_negativos[conta]))
        )[0][0]
        for conta in date_balance_negativos.columns
    }

    df_dias_de_saldo_negativo_por_conta = pd.DataFrame(
        dict_dias_de_saldo_negativo_por_conta.items(),
        columns=["account", "negative_days"],
    )

    df_dias_de_saldo_negativo_por_conta["negative_days"] = (
        df_dias_de_saldo_negativo_por_conta["negative_days"] - 1
    )

    buffer_dias_de_saldo_negativos = io.BytesIO()
    df_dias_de_saldo_negativo_por_conta.to_excel(
        buffer_dias_de_saldo_negativos, header=True, index=False
    )
    buffer_dias_de_saldo_negativos = buffer_dias_de_saldo_negativos.getvalue()
    drive_microsoft.upload_file(
        context_script["lista_caminhos_saldos_negativos"],
        buffer_dias_de_saldo_negativos,
    )

    date_balance_positivos = date_balance_positivos.bfill()
    dias_de_saldo_positivo_por_conta = {
        conta: (date_balance_positivos[conta].values <= 0).argmax()
        for conta in date_balance_positivos.columns
    }
    df_dias_de_saldo_positivo_por_conta = pd.DataFrame(
        dias_de_saldo_positivo_por_conta.items(), columns=["account", "positive_days"]
    )

    df_dias_de_saldo_negativo_por_conta = filtrar_renomear_tipar(
        df_dias_de_saldo_negativo_por_conta,
        context_script["colunas_dias_de_saldo_negativo"],
    )
    df_dias_de_saldo_positivo_por_conta = filtrar_renomear_tipar(
        df_dias_de_saldo_positivo_por_conta,
        context_script["colunas_dias_de_saldo_positivo"],
    )

    return df_dias_de_saldo_negativo_por_conta, df_dias_de_saldo_positivo_por_conta


def envia_email_advisors(df_saldos_por_conta_de_cada_assessor: pd.DataFrame) -> None:
    """Envia e-mail para cada advisor com o consolidado das contas com saldo parado, e com a relação de cada conta que possui saldo negativo (informando há quantos dias a conta está nessa situação).

    Args:
        df_saldos_por_conta_de_cada_assessor (pd.DataFrame): DataFrame relacionando cada assessor que precisa ser notificado com os saldos positivos e negativos de seus clientes (e o número de dias em que cada cliente se encontra com saldo positivo ou negativo).
    """

    context_script = gera_context_script()

    df_advisors = df_saldos_por_conta_de_cada_assessor[
        ["nome_operacional_rf", "email_operacional_rf"]
    ].drop_duplicates()
    df_advisors = df_advisors[
        df_advisors.apply(lambda row: "" not in row.values, axis=1)
    ]
    df_advisors = df_advisors[
        ~df_advisors["nome_operacional_rf"].isin(
            context_script["lista_filtros_sem_assessor"]
        )
    ]

    link_dashboard_advisors = "https://app.powerbi.com/groups/ba523459-ef74-4c78-8c72-ddd26109d17d/reports/91a35eed-df22-41db-b212-89119094d09c/ReportSectionf3690a191de0b6199b25?ctid=356b3283-f55e-4a9f-a199-9acdf3c79fb7&pbi_source=shareVisual&visual=e80e9427a5fdf0244f6e&height=481.75&width=746.43&bookmarkGuid=46e07009-7d60-49e8-9bb3-4854b4c417c0"

    for advisor in df_advisors.itertuples():
        assunto_email = f"Saldo em conta -- {(datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y, %Hh')}"
        nome_advisor = advisor[1]
        email_advisor = advisor[2]
        lista_destinatarios = [email_advisor, "gustavo.sousa@investimentos.one"]
        remetente = context_script["id_microsoft_backoffice"]

        # ### DESCOMENTE PARA RODAR LOCAL
        # assunto_email = f"[TESTE] Saldo em conta -- {(datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y, %Hh')}"
        # lista_destinatarios = ["gustavo.sousa@investimentos.one"]
        # remetente = context_script["id_microsoft_tecnologia"]
        # ###

        df_saldos_advisor = df_saldos_por_conta_de_cada_assessor[
            df_saldos_por_conta_de_cada_assessor["nome_operacional_rf"] == nome_advisor
        ]

        df_saldos_advisor_negativos = df_saldos_advisor[df_saldos_advisor["saldo"] < 0]

        if df_saldos_advisor_negativos.empty:
            continue

        num_contas_saldo_negativo = df_saldos_advisor_negativos["conta"].count()
        valor_total_saldo_negativo = f"R$ {formata_numero_para_texto_monetario(df_saldos_advisor_negativos['saldo'].sum())}"
        df_saldos_advisor_negativos_email = filtrar_renomear_tipar(
            df_saldos_advisor_negativos,
            context_script["colunas_formato_envio_email_assessor"],
        )
        df_saldos_advisor_negativos_email["Dias negativos"] = (
            df_saldos_advisor_negativos_email["Dias negativos"].fillna(0)
        )

        df_saldos_advisor_positivos = df_saldos_advisor[df_saldos_advisor["saldo"] > 0]
        num_contas_saldo_positivo = df_saldos_advisor_positivos["conta"].count()
        valor_total_saldo_positivo = f"R$ {formata_numero_para_texto_monetario(df_saldos_advisor_positivos['saldo'].sum())}"

        primeiro_nome_advisor = nome_advisor.split(" ")[0].capitalize()

        mail_microsoft = Mail(
            context_script["dict_credencias_microsft"]["client_id"],
            context_script["dict_credencias_microsft"]["client_secret"],
            context_script["dict_credencias_microsft"]["tenant_id"],
            assunto_email,
            lista_destinatarios,
            remetente,
        )
        mail_microsoft.paragraph(f"Prezado(a) {primeiro_nome_advisor},")
        mail_microsoft.paragraph(
            "Este é um email automatizado do backoffice com informações de saldo dos seus clientes."
        )
        mail_microsoft.title("Saldos negativos")
        mail_microsoft.paragraph(
            f"Você possui {num_contas_saldo_negativo} clientes com saldo negativo, totalizando {valor_total_saldo_negativo} de saldo negativo"
        )
        mail_microsoft.table(
            df_saldos_advisor_negativos_email,
            pgnr_textual_number=["Saldo", "PL", "% Saldo Negativo / PL"],
        )
        mail_microsoft.title("Saldo parado consolidado")
        mail_microsoft.paragraph(
            f"Além disso, você possui {num_contas_saldo_positivo} clientes com saldo parado em conta, totalizando {valor_total_saldo_positivo} de saldo parado"
        )
        mail_microsoft.paragraph(
            f'Para maiores detalhes dos saldos de seus clientes, <a href="{link_dashboard_advisors}">verifique o dashboard</a>'
        )
        mail_microsoft.paragraph(
            "Quaisquer dúvidas basta responder a este mesmo e-mail!"
        )
        mail_microsoft.send_mail()
        sleep(uniform(0.05, 0.15))

        print(
            f"Email enviado para {nome_advisor} (e-mail {email_advisor}) com sucesso!"
        )


def envia_email_comerciais(df_saldos_por_conta_de_cada_assessor: pd.DataFrame) -> None:
    """Envia e-mail para cada comercial com o consolidado das contas com saldo parado, e com a relação de cada conta que possui saldo negativo (informando há quantos dias a conta está nessa situação).

    Args:
        df_saldos_por_conta_de_cada_assessor (pd.DataFrame): DataFrame relacionando cada assessor que precisa ser notificado com os saldos positivos e negativos de seus clientes (e o número de dias em que cada cliente se encontra com saldo positivo ou negativo).
    """

    context_script = gera_context_script()

    df_comerciais = df_saldos_por_conta_de_cada_assessor[
        ["nome_assessor", "email_assessor"]
    ].drop_duplicates()
    df_comerciais = df_comerciais[
        df_comerciais.apply(lambda row: "" not in row.values, axis=1)
    ]
    df_comerciais = df_comerciais[~df_comerciais["email_assessor"].isin([""])]
    df_comerciais = df_comerciais[
        ~df_comerciais["nome_assessor"].isin(
            context_script["lista_filtros_sem_assessor"]
        )
    ]

    link_dashboard_comerciais = "https://app.powerbi.com/groups/36d3c30b-0298-4c4d-ad0f-d842a3904f18/reports/80b098b6-2f77-44d7-901e-8a4dba0ef55c/ReportSection24731d466fa3465168a1?ctid=356b3283-f55e-4a9f-a199-9acdf3c79fb7&pbi_source=shareVisual&visual=cbd733229ef051697d47&height=194.93&width=660.29&bookmarkGuid=1f83514c-bc7d-43c7-bb54-1d4c1284ed7e"

    for comercial in df_comerciais.itertuples():
        assunto_email = f"Saldo em conta -- {(datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y, %Hh')}"
        nome_comercial = comercial[1]
        email_comercial = comercial[2]
        lista_destinatarios = [email_comercial, "gustavo.sousa@investimentos.one"]
        remetente = context_script["id_microsoft_backoffice"]

        # ### DESCOMENTE PARA RODAR LOCAL
        # assunto_email = f"[TESTE] Saldo em conta -- {(datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y, %Hh')}"
        # lista_destinatarios = ["gustavo.sousa@investimentos.one"]
        # remetente = context_script["id_microsoft_tecnologia"]
        # ###

        df_saldos_comercial = df_saldos_por_conta_de_cada_assessor[
            df_saldos_por_conta_de_cada_assessor["nome_assessor"] == nome_comercial
        ]

        df_saldos_comercial_negativos = df_saldos_comercial[
            df_saldos_comercial["saldo"] < 0
        ]

        if df_saldos_comercial_negativos.empty:
            continue

        num_contas_saldo_negativo = df_saldos_comercial_negativos["conta"].count()
        valor_total_saldo_negativo = f"R$ {formata_numero_para_texto_monetario(df_saldos_comercial_negativos['saldo'].sum())}"
        df_saldos_comercial_negativos_email = filtrar_renomear_tipar(
            df_saldos_comercial_negativos,
            context_script["colunas_formato_envio_email_assessor"],
        )
        df_saldos_comercial_negativos_email["Dias negativos"] = (
            df_saldos_comercial_negativos_email["Dias negativos"].fillna(0)
        )

        df_saldos_comercial_positivos = df_saldos_comercial[
            df_saldos_comercial["saldo"] > 0
        ]

        num_contas_saldo_positivo = df_saldos_comercial_positivos["conta"].count()
        valor_total_saldo_positivo = f"R$ {formata_numero_para_texto_monetario(df_saldos_comercial_positivos['saldo'].sum())}"

        primeiro_nome_comercial = nome_comercial.split(" ")[0].capitalize()

        mail_microsoft = Mail(
            context_script["dict_credencias_microsft"]["client_id"],
            context_script["dict_credencias_microsft"]["client_secret"],
            context_script["dict_credencias_microsft"]["tenant_id"],
            assunto_email,
            lista_destinatarios,
            remetente,
        )
        mail_microsoft.paragraph(f"Prezado(a) {primeiro_nome_comercial},")
        mail_microsoft.paragraph(
            "Este é um email automatizado do backoffice com informações de saldo dos seus clientes."
        )
        mail_microsoft.title("Saldos negativos")
        mail_microsoft.paragraph(
            f"Você possui {num_contas_saldo_negativo} clientes com saldo negativo, totalizando {valor_total_saldo_negativo} de saldo negativo"
        )
        mail_microsoft.table(
            df_saldos_comercial_negativos_email,
            pgnr_textual_number=["Saldo", "PL", "% Saldo Negativo / PL"],
        )
        mail_microsoft.title("Saldo parado consolidado")
        mail_microsoft.paragraph(
            f"Além disso, você possui {num_contas_saldo_positivo} clientes com saldo parado em conta, totalizando {valor_total_saldo_positivo} de saldo parado"
        )
        mail_microsoft.paragraph(
            f'Para maiores detalhes dos saldos de seus clientes, <a href="{link_dashboard_comerciais}">verifique o dashboard</a>'
        )
        mail_microsoft.paragraph(
            "Quaisquer dúvidas basta responder a este mesmo e-mail!"
        )
        mail_microsoft.send_mail()
        sleep(uniform(0.05, 0.15))

        print(
            f"Email enviado para {nome_comercial} (e-mail {email_comercial}) com sucesso!"
        )


def envia_email_traders(df_saldos_por_conta_de_cada_assessor: pd.DataFrame) -> None:
    """Envia e-mail para cada trader com o consolidado das contas com saldo parado, e com a relação de cada conta que possui saldo negativo (informando há quantos dias a conta está nessa situação).

    Args:
        df_saldos_por_conta_de_cada_assessor (pd.DataFrame): DataFrame relacionando cada assessor que precisa ser notificado com os saldos positivos e negativos de seus clientes (e o número de dias em que cada cliente se encontra com saldo positivo ou negativo).
    """

    context_script = gera_context_script()

    df_traders = df_saldos_por_conta_de_cada_assessor[
        ["nome_operacional_rv", "email_operacional_rv"]
    ].drop_duplicates()
    df_traders = df_traders[df_traders.apply(lambda row: "" not in row.values, axis=1)]
    df_traders = df_traders[
        ~df_traders["nome_operacional_rv"].isin(
            context_script["lista_filtros_sem_assessor"]
        )
    ]

    for trader in df_traders.itertuples():
        assunto_email = f"Saldo em conta -- {(datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y, %Hh')}"
        nome_trader = trader[1]
        email_trader = trader[2]
        lista_destinatarios = [email_trader, "gustavo.sousa@investimentos.one"]
        remetente = context_script["id_microsoft_backoffice"]

        # ### DESCOMENTE PARA RODAR LOCAL
        # assunto_email = f"[TESTE] Saldo em conta -- {(datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y, %Hh')}"
        # lista_destinatarios = ["gustavo.sousa@investimentos.one"]
        # remetente = context_script["id_microsoft_tecnologia"]
        # ###

        df_saldos_trader = df_saldos_por_conta_de_cada_assessor[
            df_saldos_por_conta_de_cada_assessor["nome_operacional_rv"] == nome_trader
        ]

        df_saldos_trader_negativos = df_saldos_trader[df_saldos_trader["saldo"] < 0]

        if df_saldos_trader_negativos.empty:
            continue

        num_contas_saldo_negativo = df_saldos_trader_negativos["conta"].count()
        valor_total_saldo_negativo = f"R$ {formata_numero_para_texto_monetario(df_saldos_trader_negativos['saldo'].sum())}"
        df_saldos_trader_negativos_email = filtrar_renomear_tipar(
            df_saldos_trader_negativos,
            context_script["colunas_formato_envio_email_assessor"],
        )
        df_saldos_trader_negativos_email["Dias negativos"] = (
            df_saldos_trader_negativos_email["Dias negativos"].fillna(0)
        )

        primeiro_nome_trader = nome_trader.split(" ")[0].capitalize()

        mail_microsoft = Mail(
            context_script["dict_credencias_microsft"]["client_id"],
            context_script["dict_credencias_microsft"]["client_secret"],
            context_script["dict_credencias_microsft"]["tenant_id"],
            assunto_email,
            lista_destinatarios,
            remetente,
        )
        mail_microsoft.paragraph(f"Prezado(a) {primeiro_nome_trader},")
        mail_microsoft.paragraph(
            "Este é um email automatizado do backoffice com informações de saldo dos seus clientes."
        )
        mail_microsoft.title("Saldos negativos")
        mail_microsoft.paragraph(
            f"Você possui {num_contas_saldo_negativo} clientes com saldo negativo, totalizando {valor_total_saldo_negativo} de saldo negativo"
        )
        mail_microsoft.table(
            df_saldos_trader_negativos_email,
            pgnr_textual_number=["Saldo", "PL", "% Saldo Negativo / PL"],
        )
        mail_microsoft.paragraph(
            "Quaisquer dúvidas basta responder a este mesmo e-mail!"
        )
        mail_microsoft.send_mail()
        sleep(uniform(0.05, 0.15))

        print(f"Email enviado para {nome_trader} (e-mail {email_trader}) com sucesso!")


def envia_email_sem_assessores(
    df_saldos_por_conta_de_cada_assessor: pd.DataFrame,
) -> None:
    """Envia e-mail para o backoffice com a situação de saldo dos clientes que não possuem assessores.

    Args:
        df_saldos_por_conta_de_cada_assessor (pd.DataFrame): DataFrame relacionando cada assessor que precisa ser notificado com os saldos positivos e negativos de seus clientes (e o número de dias em que cada cliente se encontra com saldo positivo ou negativo).
    """

    context_script = gera_context_script()

    assunto_email = f"Saldo em conta -- {(datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y, %Hh')} -- Clientes sem assessor"
    lista_destinatarios = [
        "back.office@investimentos.one",
        "gustavo.sousa@investimentos.one",
    ]
    remetente = context_script["id_microsoft_backoffice"]

    # ### DESCOMENTE PARA RODAR LOCAL
    # assunto_email = f"[TESTE] Saldo em conta -- {(datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y, %Hh')} -- Clientes sem assessor"
    # lista_destinatarios = ["gustavo.sousa@investimentos.one"]
    # remetente = context_script["id_microsoft_tecnologia"]
    # ###

    df_saldos_sem_assessor = df_saldos_por_conta_de_cada_assessor[
        df_saldos_por_conta_de_cada_assessor["nome_assessor"].isin(
            context_script["lista_filtros_sem_assessor"]
        )
        & df_saldos_por_conta_de_cada_assessor["nome_operacional_rf"].isin(
            context_script["lista_filtros_sem_assessor"]
        )
        & df_saldos_por_conta_de_cada_assessor["nome_operacional_rv"].isin(
            context_script["lista_filtros_sem_assessor"]
        )
    ]

    df_saldos_sem_assessor_negativos = df_saldos_sem_assessor.loc[
        df_saldos_sem_assessor["saldo"] < 0
    ].sort_values(by="saldo", ascending=True)
    num_contas_saldo_negativo = df_saldos_sem_assessor_negativos["conta"].count()
    valor_total_saldo_negativo = f"R$ {formata_numero_para_texto_monetario(df_saldos_sem_assessor_negativos['saldo'].sum())}"
    df_saldos_sem_assessor_negativos_email = filtrar_renomear_tipar(
        df_saldos_sem_assessor_negativos,
        context_script["colunas_formato_envio_email_assessor"],
    )
    df_saldos_sem_assessor_negativos_email["Dias negativos"] = (
        df_saldos_sem_assessor_negativos_email["Dias negativos"].fillna(0)
    )

    df_saldos_sem_assessor_positivos = df_saldos_sem_assessor[
        df_saldos_sem_assessor["saldo"] > 0
    ]
    num_contas_saldo_positivo = df_saldos_sem_assessor_positivos["conta"].count()
    valor_total_saldo_positivo = f"R$ {formata_numero_para_texto_monetario(df_saldos_sem_assessor_positivos['saldo'].sum())}"

    mail_microsoft = Mail(
        context_script["dict_credencias_microsft"]["client_id"],
        context_script["dict_credencias_microsft"]["client_secret"],
        context_script["dict_credencias_microsft"]["tenant_id"],
        assunto_email,
        lista_destinatarios,
        remetente,
    )
    mail_microsoft.paragraph(
        f"Segue a relação de saldos dos clientes que não possuem assessor."
    )
    mail_microsoft.title("Saldos negativos")
    mail_microsoft.paragraph(
        f"Existem {num_contas_saldo_negativo} clientess em assessor com saldo negativo, totalizando {valor_total_saldo_negativo} de saldo negativo"
    )
    mail_microsoft.table(
        df_saldos_sem_assessor_negativos_email,
        pgnr_textual_number=["Saldo", "PL", "% Saldo Negativo / PL"],
    )
    mail_microsoft.title("Saldo parado consolidado")
    mail_microsoft.paragraph(
        f"Existem {num_contas_saldo_positivo} clientes sem assessor com saldo parado em conta, totalizando {valor_total_saldo_positivo} de saldo parado"
    )
    mail_microsoft.send_mail()
    sleep(uniform(0.05, 0.15))
    print(f"Email dos clientes sem assessor enviado para o Backoffice com sucesso!")


def notifica_saldo_em_conta_func() -> None:
    """Função main do script, realiza os seguintes passos:
    1. carrega as bases neessárias ao envio dos e-mails;
    2. Relaciona as bases, gerando uma nova base com a relação de assessores a ser notificada e a situação do saldo de cada conta;
    3. dispara os e-mails para os comerciais, traders e advisors responsáveis.
    """

    context_script = gera_context_script()

    df_socios = carrega_socios()
    df_clientes_assessores = carrega_clientes_assessores()
    df_contas_btg = carrega_contas_btg()
    df_saldo_em_conta = carrega_saldo_em_conta()
    df_pl = carrega_pl()
    (
        df_dias_de_saldo_negativo_por_conta,
        df_dias_de_saldo_positivo_por_conta,
    ) = carrega_dias_de_saldo()

    df_saldos_por_conta_de_cada_assessor = estabelece_saldos_por_conta_de_cada_assessor(
        df_socios,
        df_clientes_assessores,
        df_saldo_em_conta,
        df_contas_btg,
        df_pl,
        df_dias_de_saldo_negativo_por_conta,
        df_dias_de_saldo_positivo_por_conta,
    )

    envia_email_comerciais(df_saldos_por_conta_de_cada_assessor)
    envia_email_advisors(df_saldos_por_conta_de_cada_assessor)
    envia_email_traders(df_saldos_por_conta_de_cada_assessor)
    envia_email_sem_assessores(df_saldos_por_conta_de_cada_assessor)

    assunto_email = f"Envio automático de e-mails de saldo concluído -- {context_script['datetime_hoje_brasil'].strftime('%d/%m/%Y')} - {context_script['datetime_hoje_brasil'].strftime('%H')}h"
    lista_destinatarios = [
        "back.office@investimentos.one",
        "gustavo.sousa@investimentos.one",
    ]
    remetente = context_script["id_microsoft_tecnologia"]

    # ### DESCOMENTE PARA RODAR LOCAL
    # assunto_email = f"[TESTE] Saldo em conta -- {(datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y, %Hh')}"
    # lista_destinatarios = ["gustavo.sousa@investimentos.one"]
    # ###

    mail_microsoft = Mail(
        context_script["dict_credencias_microsft"]["client_id"],
        context_script["dict_credencias_microsft"]["client_secret"],
        context_script["dict_credencias_microsft"]["tenant_id"],
        assunto_email,
        lista_destinatarios,
        remetente,
    )
    mail_microsoft.image(mail_microsoft.logo_one, mode="link")
    mail_microsoft.paragraph(
        "O processo de envio automático de e-mails de saldo em conta foi concluído com sucesso!"
    )
    mail_microsoft.send_mail()
    sleep(uniform(0.05, 0.15))


def verifica_atualizacao_arquivo_saldos() -> None:
    """Verifica se a planilha de saldos foi atualizada há menos de 30 minutos pelo BackOffice."""

    context_script = gera_context_script()

    drive_microsoft = context_script["drive_microsoft"]

    infos_arquivo_saldos = drive_microsoft.get_drive_item_metadata(
        context_script["lista_caminhos_saldo_novos"]
    )
    infos_arquivo_saldos = json.loads(infos_arquivo_saldos.content.decode("latin1"))

    ultimo_modificador = infos_arquivo_saldos["lastModifiedBy"]["user"]["displayName"]
    datetime_ultima_modificacao = datetime.strptime(
        infos_arquivo_saldos["lastModifiedDateTime"], "%Y-%m-%dT%H:%M:%SZ"
    ) - timedelta(hours=3)

    datetime_agora_no_brasil = context_script["datetime_hoje_brasil"]

    planilha_saldo_foi_atualizada_ha_menos_de_30_min = (
        datetime_agora_no_brasil - datetime_ultima_modificacao <= timedelta(minutes=30)
    )

    if planilha_saldo_foi_atualizada_ha_menos_de_30_min:
        print(
            f"Planilha de saldos em conta presente em {context_script['lista_caminhos_saldo_novos']} foi atualizada há {datetime_agora_no_brasil - datetime_ultima_modificacao}, em {datetime_ultima_modificacao} por {ultimo_modificador}. Seguiremos com o envio de e-mails para os assessores!"
        )
        return True
    else:
        assunto_email = f"Saldo em Conta - {datetime_agora_no_brasil.strftime('%d/%m/%Y')} - {datetime_agora_no_brasil.strftime('%H')}h"
        lista_destinatarios = [
            "back.office@investimentos.one",
            "gustavo.sousa@investimentos.one",
        ]
        remetente = context_script["id_microsoft_tecnologia"]

        # ### DESCOMENTE PARA RODAR LOCAL
        # assunto_email = f"[TESTE] Saldo em conta -- {(datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y, %Hh')}"
        # lista_destinatarios = ["gustavo.sousa@investimentos.one"]
        # ###

        mail_microsoft = Mail(
            context_script["dict_credencias_microsft"]["client_id"],
            context_script["dict_credencias_microsft"]["client_secret"],
            context_script["dict_credencias_microsft"]["tenant_id"],
            assunto_email,
            lista_destinatarios,
            remetente,
        )
        mail_microsoft.image(mail_microsoft.logo_one, mode="link")
        mail_microsoft.paragraph(
            "A planilha de saldos não foi atualizada nos últimos 30 min, sendo assim o e-mail de saldos não foi enviado para os comerciais e advisors."
        )
        mail_microsoft.send_mail()
        sleep(uniform(0.05, 0.15))

        print(
            f"A planilha de saldos em conta presente em {context_script['lista_caminhos_saldo_novos']} já não é atualizada desde {datetime_ultima_modificacao}, há {datetime_agora_no_brasil - datetime_ultima_modificacao}!"
        )

        return False
