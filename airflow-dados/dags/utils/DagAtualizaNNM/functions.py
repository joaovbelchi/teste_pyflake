import pandas as pd
from deltalake import DeltaTable
from datetime import datetime, timedelta
from utils.bucket_names import get_bucket_name
from typing import Literal
import json
from utils.secrets_manager import get_secret_value
import requests
from utils.mergeasof import fill_retroativo, remover_antes_entrada, mergeasof_socios
import boto3
import logging
from utils.OneGraph.drive import OneGraphDrive
import pytz
from utils.msteams import msteams_qualidade_dados
from airflow.models import Variable
import io
import base64
from dateutil.parser import isoparse
import numpy as np
from utils.tools import real_br_money_mask
from dateutil.parser import isoparse
from dateutil.relativedelta import relativedelta


def atualiza_nnm_b2c(timestamp_dagrun: str):
    """Atualiza bases account_in_out e nnm_b2c,
    além de enviar email com informações de contas
    que saíram por transferência de assessoria.

    Args:
        timestamp_dagrun (str): Timestamp lógico de execução do pipeline
    """

    def offset_to_monday(date: pd.Timestamp) -> pd.Timestamp:
        """Recebe uma data e joga para segunda-feira, caso
        a data fornecida seja sábado ou domingo.

        Args:
            date (pd.Timestamp): Data fornnecida

        Returns:
            pd.Timestamp: Data ajustada
        """
        if date.weekday() == 5:  # Saturday
            return date + pd.Timedelta(days=2)
        elif date.weekday() == 6:  # Sunday
            return date + pd.Timedelta(days=1)
        else:
            return date

    bucket_lake_gold = get_bucket_name("lake-gold")
    bucket_lake_silver = get_bucket_name("lake-silver")
    bucket_lake_bronze = get_bucket_name("lake-bronze")

    pd.set_option("float_format", "{:.2f}".format)
    pd.set_option("display.max_columns", None)
    today = pd.to_datetime(pd.to_datetime("today").date())

    dates_list = [str(i.date()) for i in pd.date_range(end=today.date(), periods=7)]

    def list_recent_files_in_bucket(bucket_name, prefix_name, dates_list=dates_list):
        # Cria uma instância do cliente S3
        s3_client = boto3.client("s3")

        all_objects = []
        for date in dates_list:
            # Lista todos os objetos no bucket especificado usando paginação
            paginator = s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=bucket_name, Prefix=f"{prefix_name}={date}"
            )

            # Recupera todos os objetos e armazena em uma lista
            date_objects = []
            for page in page_iterator:
                if "Contents" in page:
                    date_objects.extend(page["Contents"])

            # Adiciona arquivo mais recente da partição
            if date_objects:
                # Se houver partição de hora_escrita, filtrar
                # apenas a maior
                if "hora_escrita" in date_objects[0]["Key"]:
                    max_hora_escrita = max(
                        i["Key"].split("hora_escrita=")[-1].split("/")[0]
                        for i in date_objects
                    )
                    date_objects = [
                        i
                        for i in date_objects
                        if f"hora_escrita={max_hora_escrita}" in i["Key"]
                    ]

                most_recent = sorted(date_objects, key=lambda x: x["LastModified"])[-1]
                all_objects.append(most_recent)

        # Verifica se há objetos no bucket
        if not all_objects:
            raise Exception(f"O bucket '{bucket_name}' está vazio.")

        final = [f"s3://{bucket_name}/{i['Key']}" for i in all_objects]

        return final

    ###################   ATUALIZA ACCOUNT_IN_OUT    #################################

    # Baixa Contas
    prefix_name = "btg/contas/date_escrita"
    files = list_recent_files_in_bucket(bucket_lake_silver, prefix_name)

    df_contas_hist = pd.DataFrame()
    for object_key in files:
        df_contas_temp = pd.read_parquet(object_key)
        df_contas_temp["date_escrita"] = object_key.split("=")[1].split("/")[0]
        df_contas_hist = pd.concat([df_contas_hist, df_contas_temp])

    df_contas_hist = df_contas_hist.drop_duplicates(
        subset=["carteira", "date_escrita", "hora_escrita"], keep="last"
    )
    df_contas_hist = df_contas_hist[df_contas_hist["status"] != "ENCERRADA"]
    df_contas_hist["carteira"] = df_contas_hist["carteira"].astype("str")

    # Baixa account_in_out
    accounts_in_out = pd.read_parquet(
        f"s3://{bucket_lake_bronze}/api/one/accounts_in_out/"
    )

    # Baixa df_advisors_no_dup_cp
    df_advisors_no_dup_cp = pd.read_parquet(
        f"s3://{bucket_lake_gold}/api/hubspot/AdvisorsNoDupCp/"
    )
    df_advisors_no_dup_cp["Conta"] = df_advisors_no_dup_cp["Conta"].astype("str")
    df_advisors_no_dup_cp["Data"] = pd.to_datetime(df_advisors_no_dup_cp["Data"])

    df_contas_hist["min_date_escrita"] = df_contas_hist.groupby("carteira")[
        "date_escrita"
    ].transform("min")

    df_contas_hist["min_date_escrita_tabela"] = df_contas_hist["date_escrita"].min()

    # Filtra contas que entraram recentemente
    df_contas_in = df_contas_hist.sort_values(
        ["carteira", "date_escrita", "hora_escrita"]
    ).drop_duplicates(["carteira", "date_escrita"], keep="first")

    unique_date_escrita = df_contas_hist["date_escrita"].unique()
    df_unique_dates = pd.DataFrame(
        pd.to_datetime(pd.Series(unique_date_escrita)), columns=["date_escrita"]
    )
    df_unique_dates

    def shift_over_partition(group: pd.DataFrame) -> pd.DataFrame:
        """Realiza uma operação de Lag (D-1) na coluna 'date_escrita'
        para uma partição específica (deve ser utilizado em um 'apply'
        a partir de um 'groupby')

        Args:
            group (pd.DataFrame): DataFrame contendo a partição
            do grupo.

        Returns:
            pd.DataFrame: DataFrame contendo partição do grupo
            após criar nova coluna de Lag.
        """
        # Use shift to get the lagged value within each partition
        group["lag_date_escrita"] = group["date_escrita"].shift(1)
        return group

    def ultima_date_escrita(row: pd.Series) -> pd.Series:
        """Busca pelo maior valor de 'date_escrita' dentro
        do DataFrame 'df_unique_dates', desde que esse valor seja menor
        do que o valor de 'date_escrita' da linha sobre a qual
        essa função está sendo aplicada (deve ser utilizado em um 'apply'
        nas linhas de um DataFrame)

        Args:
            row (pd.Series): Linha do DataFrame

        Returns:
            pd.Series: Linha do DataFrame com a coluna adicional
            'ultima_date_escrita_tabela'
        """
        value = df_unique_dates.loc[
            df_unique_dates["date_escrita"] < row["date_escrita"], "date_escrita"
        ].max()
        row["ultima_date_escrita_tabela"] = value
        return row

    df_contas_in = (
        df_contas_in.groupby("carteira").apply(shift_over_partition).droplevel(0)
    )
    df_contas_in = df_contas_in.apply(ultima_date_escrita, axis=1)

    df_contas_in = df_contas_in.loc[
        ~(
            df_contas_in["lag_date_escrita"]
            == df_contas_in["ultima_date_escrita_tabela"]
        )
        & ~(df_contas_in["date_escrita"] == df_contas_in["min_date_escrita_tabela"]),
        :,
    ].copy()

    # Cria dataframe com contas que entraram, operacional e comercial atual
    df_contas_in = df_contas_in.merge(
        df_advisors_no_dup_cp, left_on="carteira", right_on="Conta", how="left"
    )
    df_contas_in["Conta"] = df_contas_in["Conta"].fillna(df_contas_in["carteira"])
    df_contas_in = df_contas_in[
        [
            "Conta",
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "date_escrita",
        ]
    ]
    df_contas_in["Abriu/Fechou"] = "Abriu"
    df_contas_in = df_contas_in.rename(columns={"date_escrita": "Data"})
    df_contas_in["Data"] = pd.to_datetime(df_contas_in["Data"])

    # Filtra contas que saíram recentemente
    accounts_in_out["data_maxima_conta"] = accounts_in_out.groupby("Conta")[
        "Data"
    ].transform("max")
    df_contas_hist["max_date_escrita"] = df_contas_hist.groupby("carteira")[
        "date_escrita"
    ].transform("max")
    data_maxima = df_contas_hist["max_date_escrita"].max()
    df_contas_out = df_contas_hist[
        (df_contas_hist["max_date_escrita"] != data_maxima)
        & (df_contas_hist["max_date_escrita"] == df_contas_hist["date_escrita"])
    ]
    df_contas_out = df_contas_out.sort_values(
        ["carteira", "date_escrita", "hora_escrita"]
    ).drop_duplicates(["carteira", "date_escrita"], keep="last")

    # Filtra contas que saíram recentemente e
    # que constam como abertas na base account_in_out
    contas_fechadas = accounts_in_out[
        (accounts_in_out["Abriu/Fechou"] == "Fechou")
        & (accounts_in_out["data_maxima_conta"] == accounts_in_out["Data"])
    ]["Conta"]
    df_contas_out = df_contas_out[~df_contas_out["carteira"].isin(contas_fechadas)]

    # Cria dataframe com contas que saíram, operacional e comercial atual
    df_contas_out = df_contas_out.merge(
        df_advisors_no_dup_cp, left_on="carteira", right_on="Conta", how="left"
    )
    df_contas_out = df_contas_out[
        [
            "Conta",
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "date_escrita",
        ]
    ]
    df_contas_out["Abriu/Fechou"] = "Fechou"
    df_contas_out = df_contas_out.rename(columns={"date_escrita": "Data"})
    df_contas_out["Data"] = pd.to_datetime(
        df_contas_out["Data"]
    ) + pd.offsets.BusinessDay(n=1)

    # Abre o histórico, junta ambas acima e preenche o operacional e comercial
    # de quem não havia sido preenchido pois ainda não tinha sido criado no hubspot
    cols_account_in_out = [
        "Conta",
        "Data",
        "Abriu/Fechou",
        "Comercial",
        "Operacional RF",
        "Operacional RV",
    ]
    cols_fill = ["Comercial", "Operacional RF", "Operacional RV"]

    accounts_in_out = accounts_in_out[cols_account_in_out]
    accounts_in_out = accounts_in_out.merge(
        df_advisors_no_dup_cp[["Conta", *cols_fill]],
        on="Conta",
        how="left",
        suffixes=["", "_fill"],
    )
    for col in cols_fill:
        accounts_in_out[col] = accounts_in_out[col].fillna(
            accounts_in_out[f"{col}_fill"]
        )

    accounts_in_out = accounts_in_out[cols_account_in_out]

    # Junta as bases com o histórico e salva no s3
    accounts_in_out = pd.concat(
        [accounts_in_out, df_contas_out, df_contas_in]
    ).drop_duplicates(subset=["Conta", "Data", "Abriu/Fechou"], keep="last")
    accounts_in_out.to_parquet(
        f"s3://{bucket_lake_bronze}/api/one/accounts_in_out/accounts_in_out.parquet"
    )

    ###################   ATUALIZA NNM_B2C    #################################

    # Baixa Posição
    ts_parsed = isoparse(timestamp_dagrun)
    ts_last_month = ts_parsed - relativedelta(months=1)

    partitions = [
        (str(ts_parsed.year), str(ts_parsed.month)),
        (str(ts_last_month.year), str(ts_last_month.month)),
    ]

    dt_posicao = DeltaTable(f"s3://{bucket_lake_silver}/btg/api/posicao_tratada/")

    df_posicao_hist = pd.concat(
        [
            dt_posicao.to_pandas(
                partitions=[
                    ("ano_particao", "=", yearmonth[0]),
                    ("mes_particao", "=", yearmonth[1]),
                ],
                columns=[
                    "account",
                    "date",
                    "timestamp_dagrun",
                    "data_interface",
                    "product",
                    "gross_value",
                    "stock",
                    "amount",
                    "market",
                ],
            )
            for yearmonth in partitions
        ]
    )

    rename_dict = {
        "product": "description",
        "gross_value": "net_new_money",
    }

    df_posicao_hist.rename(columns=rename_dict, inplace=True)

    df_posicao_hist = df_posicao_hist.loc[
        pd.to_datetime(df_posicao_hist["date"]) >= (ts_parsed - timedelta(days=10))
    ]
    df_posicao_hist["account"] = (
        df_posicao_hist["account"].astype("int64").astype("string")
    )

    df_posicao_hist["max_ts_dagrun"] = df_posicao_hist.groupby("date")[
        "timestamp_dagrun"
    ].transform("max")
    df_posicao_hist["min_ts_dagrun"] = df_posicao_hist.groupby(["account", "date"])[
        "timestamp_dagrun"
    ].transform("min")

    # Mantendo apenas última hora escrita
    df_posicao_out = (
        df_posicao_hist.loc[
            df_posicao_hist["timestamp_dagrun"] == df_posicao_hist["max_ts_dagrun"]
        ]
        .drop(columns=["max_ts_dagrun", "min_ts_dagrun"])
        .copy()
    )
    df_posicao_out = df_posicao_out.sort_values(["account", "date"])

    # Filtrando posição do dia da saída da conta
    df_posicao_out = df_contas_out[["Conta", "Data"]].merge(
        df_posicao_out, left_on="Conta", right_on="account", how="inner"
    )
    df_posicao_out = df_posicao_out[
        df_posicao_out["Data"].apply(offset_to_monday)
        == (pd.to_datetime(df_posicao_out["date"]) + pd.offsets.BusinessDay(n=2))
    ]

    # Criando NNM B2C de saída
    df_posicao_out["type"] = "TRANSFERENCIA_SAIDA"
    df_posicao_out["advisor"] = "One Investimentos"
    df_posicao_out["stock"] = df_posicao_out["stock"].fillna(
        df_posicao_out["description"]
    )
    df_posicao_out["date"] = df_posicao_out["Data"]
    df_posicao_out = df_posicao_out[
        (df_posicao_out["amount"] != 0) & (df_posicao_out["net_new_money"] != 0)
    ]

    # Mantendo apenas primeira hora escrita
    df_posicao_in = (
        df_posicao_hist.loc[
            df_posicao_hist["timestamp_dagrun"] == df_posicao_hist["min_ts_dagrun"]
        ]
        .drop(columns=["max_ts_dagrun", "min_ts_dagrun"])
        .copy()
    )
    df_posicao_in = df_posicao_in.sort_values(["account", "date"])

    # Filtrando posição do dia da entrada da conta
    df_contas_in["Data Ajustada"] = (
        df_contas_in["Data"].apply(offset_to_monday).dt.strftime("%Y-%m-%d")
    )
    df_posicao_in = df_contas_in[["Conta", "Data Ajustada"]].merge(
        df_posicao_in,
        left_on=["Conta", "Data Ajustada"],
        right_on=["account", "date"],
        how="inner",
    )
    df_posicao_in.rename(columns={"Data Ajustada": "Data"}, inplace=True)

    df_posicao_in["type"] = "TRANSFERENCIA_ENTRADA"
    df_posicao_in["advisor"] = "One Investimentos"
    df_posicao_in["stock"] = df_posicao_in["stock"].fillna(df_posicao_in["description"])
    df_posicao_in["date"] = pd.to_datetime(df_posicao_in["Data"]) - pd.Timedelta(days=1)
    df_posicao_in = df_posicao_in[
        (df_posicao_in["amount"] != 0) & (df_posicao_in["net_new_money"] != 0)
    ]

    # Junta ambos acima e remove os valores igual a 0
    nnm_b2c_in_out = pd.concat([df_posicao_in, df_posicao_out])
    nnm_b2c_in_out["name"] = None
    nnm_b2c_in_out = nnm_b2c_in_out[
        [
            "account",
            "name",
            "market",
            "description",
            "stock",
            "amount",
            "net_new_money",
            "date",
            "type",
            "advisor",
        ]
    ]
    nnm_b2c_in_out["net_new_money"] = np.where(
        (nnm_b2c_in_out["type"] == "TRANSFERENCIA_SAIDA"),
        nnm_b2c_in_out["net_new_money"] * (-1),
        nnm_b2c_in_out["net_new_money"],
    )

    # Abre o histórico, junta e salva no s3
    nnm_b2c_historico = pd.read_parquet(f"s3://{bucket_lake_bronze}/api/one/nnm_b2c/")
    nnm_b2c_historico = pd.concat([nnm_b2c_historico, nnm_b2c_in_out])
    nnm_b2c_historico["amount"] = nnm_b2c_historico["amount"].astype(float)
    nnm_b2c_historico["net_new_money"] = nnm_b2c_historico["net_new_money"].astype(
        float
    )
    nnm_b2c_historico["date"] = pd.to_datetime(nnm_b2c_historico["date"])
    nnm_b2c_historico = nnm_b2c_historico.drop_duplicates(
        subset=[
            "account",
            "name",
            "market",
            "description",
            "stock",
            "amount",
            "net_new_money",
            "date",
            "type",
        ]
    )
    nnm_b2c_historico.to_parquet(
        f"s3://{bucket_lake_bronze}/api/one/nnm_b2c/nnm_b2c.parquet"
    )

    ###################   ENVIA EMAIL COM SAÍDA DE CONTAS    #################################

    # Abre a base de clientes assessores para encontrar o primeiro registro
    data_entrada = pd.read_parquet(
        f"s3://{bucket_lake_gold}/api/hubspot/ContasHistoricoCp/",
        columns=["Conta", "Data"],
    )
    data_entrada["Conta"] = data_entrada["Conta"].astype(str)
    data_entrada["Data"] = pd.to_datetime(data_entrada["Data"])
    data_entrada = data_entrada.groupby("Conta")["Data"].min().reset_index()
    data_entrada.columns = ["Conta", "Data de Entrada"]

    # adicionando na base de accounts out as informações necessárias
    temp = df_contas_hist.sort_values(
        ["carteira", "date_escrita", "hora_escrita"]
    ).drop_duplicates(subset="carteira", keep="last")
    temp = temp[["carteira", "pl_declarado", "nome_completo"]]
    temp["nome_completo"] = temp["nome_completo"].str.split(" ").str[0]
    temp["carteira"] = temp["carteira"].astype(str)
    accounts_out_mail = df_contas_out.merge(
        temp, left_on="Conta", right_on="carteira", how="left"
    )
    temp = (
        nnm_b2c_in_out[nnm_b2c_in_out["type"] == "TRANSFERENCIA_SAIDA"]
        .groupby("account")["net_new_money"]
        .sum()
        * -1
    ).reset_index()
    accounts_out_mail = accounts_out_mail.merge(
        temp, left_on="Conta", right_on="account", how="left"
    )
    accounts_out_mail["pl_declarado"] = accounts_out_mail["pl_declarado"].fillna(0)
    accounts_out_mail["net_new_money"] = accounts_out_mail["net_new_money"].fillna(0)

    # Quantidade de contas que saíram
    n_contas = accounts_out_mail.shape[0]

    # Define o pl total e declarado das contas e muda os separadores de decimal e milhar das variáveis pl_total e pl_declarado
    pl_total = "R$ " + real_br_money_mask(accounts_out_mail["net_new_money"].sum())
    pl_declarado = "R$ " + real_br_money_mask(accounts_out_mail["pl_declarado"].sum())

    # Cria o dataframe final e muda os separadores de decimal e milhar das variáveis pl_total e pl_declarado
    df_result = accounts_out_mail[
        [
            "Conta",
            "nome_completo",
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "net_new_money",
            "pl_declarado",
        ]
    ]
    df_result.columns = [
        "Conta",
        "Nome",
        "Comercial",
        "Operacional RF",
        "Operacional RV",
        "PL Total",
        "PL Declarado",
    ]
    df_result["PL Total"] = df_result["PL Total"].apply(real_br_money_mask)
    df_result["PL Declarado"] = df_result["PL Declarado"].apply(real_br_money_mask)
    df_result = df_result.merge(data_entrada, on="Conta", how="left")

    # Carrega template HTML
    with open(
        "dags/repo/dags/utils/DagAtualizaNNM/arquivos_html/email_saida_contas.html",
        encoding="UTF-8",
    ) as f:
        template = f.read()

    # Formata template HTML
    replace_list = [
        ("{N_CONTAS}", n_contas),
        ("{PL_TOTAL}", pl_total),
        ("{PL_DECLARADO}", pl_declarado),
        ("{TABELA_SAIDAS}", df_result.to_html(index=False)),
    ]
    for placeholder, text in replace_list:
        template = template.replace(placeholder, str(text))

    # Corpo da requisição para envio de e-mail
    json_ = {
        "message": {
            "subject": "Saída de Contas - " + datetime.now().strftime("%d/%m/%Y"),
            "body": {"contentType": "HTML", "content": template},
            "toRecipients": [
                {"emailAddress": {"address": "cassio@investimentos.one"}},
                {"emailAddress": {"address": "guilherme.isaac@investimentos.one"}},
                {"emailAddress": {"address": "felipe.nogueira@investimentos.one"}},
            ],
        }
    }

    # get microsoft graph credentials
    secret_dict = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    microsoft_client_id = secret_dict["client_id"]
    microsoft_client_secret = secret_dict["client_secret"]

    url = "https://login.microsoftonline.com/356b3283-f55e-4a9f-a199-9acdf3c79fb7/oauth2/v2.0/token"
    payload = f"client_id={microsoft_client_id}&client_secret={microsoft_client_secret}&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default&grant_type=client_credentials"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Cookie": "fpc=AocyqXXzjRBLm2yxwKDTgJgoPQEuAQAAAL8_XdkOAAAA; stsservicecookie=estsfd; x-ms-gateway-slice=estsfd",
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    token = response.json()["access_token"]

    # Envio do e-mail via API
    if n_contas > 0:
        headers = {"Authorization": "Bearer " + token}
        response = requests.post(
            "https://graph.microsoft.com/v1.0/users/d64bcd7f-c47a-4343-b0b4-73f4a8e927a5/sendMail",
            headers=headers,
            json=json_,
        )  # Envia do tech
        print(response)


def atualizar_divisao_valores(
    df: pd.DataFrame, df_divisao_valores: pd.DataFrame
) -> pd.DataFrame:
    """Atualiza a base de dados com as divisões de valores geradas pelo aplicativo
    "Divisão Áreas".

    Args:
        df (pd.DataFrame): DataFrame com a base de NNM
        df_divisao_valores (pd.DataFrame): DataFrame com a base de divisão indicando
        quais registros da base de NNM devem ser divididos. Esta base é fruto da utilização
        do aplicativo Divisão Áreas.

    Raises:
        BaseException: Caso a captação total do dataframe de input seja diferente
        da captação total do dataframe de saída é levantando uma exceção.

    Returns:
        pd.DataFrame: DataFrame com a base de NNM com sua linhas alteradas. Esta função
        faz também a mudança do índice da base de NNM.
    """

    # Saval informação para validação ao final do código
    captacao_total = df["Captação"].sum()

    # Preenche as colunas de Percentual da divisão e Participação para o caso Default
    df["Percentual da divisao"] = 1.0
    df["Participação"] = df["Comercial"]
    df = df.reset_index(drop=True)

    if not df_divisao_valores.empty:
        # Filtra apenas as colunas e linhas que serão necessárias da base de Divisão
        df_divisao_valores = df_divisao_valores.loc[
            df_divisao_valores["Linha original"],
            [
                "Conta",
                "Mercado",
                "Captação",
                "Data",
                "Tipo",
                "Comerciais",
                "Percentuais",
            ],
        ]

        # Transforma as informações referentes a divisão de strings para listas e
        # realiza alguns tratamentos
        df_divisao_valores["Comerciais"] = df_divisao_valores["Comerciais"].str.split(
            "-"
        )
        df_divisao_valores["Percentuais"] = df_divisao_valores["Percentuais"].str.split(
            "-"
        )
        df_divisao_valores["Captação"] = df_divisao_valores["Captação"].round()
        df_divisao_valores["Base"] = "DIV"

        # Seleciona apenas as colunas necessárias para a base de NNM
        # e realiza alguns tratamentos
        df_resumido = df.loc[:, ["Conta", "Mercado", "Captação", "Data", "Tipo"]].copy()
        df_resumido["Captação"] = df_resumido["Captação"].round()
        df_resumido["Base"] = "NNM"

        # Início do algorítmo que identifica linhas iguais na base de NNM e na base
        # de divisão.
        df_concatenado = pd.concat([df_divisao_valores, df_resumido], axis=0)
        df_concatenado["n_registros_iguais"] = df_concatenado.groupby(
            ["Conta", "Mercado", "Captação", "Data", "Tipo"], dropna=False
        )["Captação"].transform("count")
        df_concatenado["n_registros_iguais_mesma_base"] = df_concatenado.groupby(
            ["Conta", "Mercado", "Captação", "Data", "Tipo", "Base"], dropna=False
        )["Captação"].transform("count")

        filtro = (
            df_concatenado["n_registros_iguais"]
            == df_concatenado["n_registros_iguais_mesma_base"]
        )
        df_concatenado = df_concatenado.loc[~filtro, :]

        df_concatenado = df_concatenado.reset_index()

        df_concatenado["rank_linhas_mesma_base"] = df_concatenado.groupby(
            ["Conta", "Mercado", "Captação", "Data", "Tipo", "Base"]
        )["Captação"].rank("first")
        df_concatenado["minimo"] = df_concatenado.groupby(
            ["Conta", "Mercado", "Captação", "Data", "Tipo"]
        )["n_registros_iguais_mesma_base"].transform("min")

        filtro = df_concatenado["rank_linhas_mesma_base"] <= df_concatenado["minimo"]
        df_concatenado = df_concatenado.loc[filtro, :]
        # Fim do algorítmo. A variável retornada contem apenas as linhas
        # das bases de NNM e Divisão que estão duplicadas entre si

        # Os tratamentos abaixo servem para levar a informação sobre a divisão de captação
        # da base de divisão para a base de NNM (neste caso é uma base reduzida apenas com
        # os valores duplicados). O merge é feito com base no indice das linhas uma vez que
        # ambas as linhas de ambas as bases foram ordenadas da mesma forma.
        df_divisao_select = (
            df_concatenado.loc[df_concatenado["Base"] == "DIV", :]
            .drop(
                columns=[
                    "n_registros_iguais",
                    "n_registros_iguais_mesma_base",
                    "rank_linhas_mesma_base",
                    "minimo",
                ]
            )
            .sort_values(["Conta", "Mercado", "Captação", "Data", "Tipo"])
            .reset_index(drop=True)
        )
        df_selected = (
            df_concatenado.loc[df_concatenado["Base"] == "NNM", :]
            .drop(
                columns=[
                    "n_registros_iguais",
                    "n_registros_iguais_mesma_base",
                    "rank_linhas_mesma_base",
                    "minimo",
                    "Comerciais",
                    "Percentuais",
                ]
            )
            .sort_values(["Conta", "Mercado", "Captação", "Data", "Tipo"])
            .reset_index(drop=True)
            .merge(
                df_divisao_select[["Comerciais", "Percentuais"]],
                left_index=True,
                right_index=True,
            )
        )

        # Leva a informação de divisão da base de NNM reduzida para a base
        # de NNM original
        df = df.merge(
            df_selected[["index", "Comerciais", "Percentuais"]],
            left_index=True,
            right_on="index",
            how="left",
            validate="1:1",
        )  # CUIDADO MUDAR O MERGE

        # Faz o tratamento das informações de divisão e replica para as colunas de
        # Percentual da participação, Participação e Captação
        df = df.explode(["Comerciais", "Percentuais"]).reset_index(drop=True)
        filtro = df["Comerciais"].isna()
        df.loc[~filtro, "Participação"] = df["Comerciais"]
        df.loc[~filtro, "Percentuais"] = df["Percentuais"].astype("float64") / 100
        df.loc[~filtro, "Percentual da divisao"] = df["Percentuais"]
        df.loc[~filtro, "Captação"] = df["Captação"] * df["Percentual da divisao"]
        df = (
            df.drop(columns=["index", "Comerciais", "Percentuais"])
            .reset_index(drop=True)
            .astype({"Captação": "float", "Percentual da divisao": "float"})
        )

        # Caso alguma inconsistência seja gerada durante o processamento desta
        # função uma exceção é levantada
        captacao_total_apos_tratamento = df["Captação"].sum()
        if round(captacao_total) != round(captacao_total_apos_tratamento):
            raise BaseException(
                "A captação total de entrada da função não bate com a de saída"
            )

    return df


def verifica_alteracao_retroativa(
    df_nnm_final_original: pd.DataFrame,
    df_nnm_final_novo: pd.DataFrame,
    data_interval_end: str,
):
    """Esta função verifica se está sendo realizada uma alteração retroativa
    durante uma atualização da base de dados de NNM. É considerado alteração
    retroativa quando, após o dia 20 do mês atual, ocorrer uma atualização da
    base de NNM que muda valores de captação dentro do mês anterior.

    Args:
        df_nnm_final_original (pd.DataFrame): Dataframe com a base de NNM contendo
        os registros que estavam presentes na base antes da data de atualização
        df_nnm_final_novo (pd.DataFrame): Dataframe com a base de NNM contendo os
        registros que serão acrescentados na base de dados.
        data_interval_end (str): Timestamp no formato ISO que representa o
        'data_interval_end' da Dag Run do Airflow
    """

    # Criando variáveis auxiliares
    data_atual = isoparse(data_interval_end)

    if data_atual.day >= 20:
        # Calcula o último dia do mês anterior para servir como data de
        # corte para a análise.
        primeiro_dia_do_mes = datetime(data_atual.year, data_atual.month, 1)
        ultimo_dia_mes_passado = primeiro_dia_do_mes - timedelta(days=1)

        # Soma o NNM por conta e data e calcula as diferenças
        # para datas anteriores a data de corte, caso haja
        # diferença de NNM é ativado o laço de alerta
        df_nnm_agrupado_original = (
            df_nnm_final_original.groupby(["account", "date"])["net_new_money"]
            .sum()
            .reset_index()
        )
        df_nnm_agrupado_novo = (
            df_nnm_final_novo.groupby(["account", "date"])["net_new_money"]
            .sum()
            .reset_index()
        )

        conferencia = pd.merge(
            df_nnm_agrupado_original,
            df_nnm_agrupado_novo,
            on=["account", "date"],
            suffixes=("_original", "_novo"),
        )

        conferencia["diferenca"] = (
            conferencia["net_new_money_original"] - conferencia["net_new_money_novo"]
        ).round()

        filtro_data = conferencia["date"] <= ultimo_dia_mes_passado
        filtro_resultado = conferencia["diferenca"] > 0
        conferencia = conferencia[filtro_data & filtro_resultado]

        if not conferencia.empty:
            conferencia = conferencia[["account", "date"]]

            df_original = df_nnm_final_original.merge(
                conferencia, on=["account", "date"], how="inner"
            )
            df_novo = df_nnm_final_novo.merge(
                conferencia, on=["account", "date"], how="inner"
            )

            # Transforma o data frame em bytes e aplica codificação base64
            # para que ele possa ser enviado por e-mail
            excel_buffer = io.BytesIO()
            df_original.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)
            xlsx_original = base64.b64encode(excel_buffer.read()).decode()

            excel_buffer = io.BytesIO()
            df_novo.to_excel(excel_buffer, index=False)
            excel_buffer.seek(0)
            xlsx_novo = base64.b64encode(excel_buffer.read()).decode()

            # get microsoft graph credentials
            secret_dict = json.loads(
                get_secret_value("apis/microsoft_graph", append_prefix=True)
            )
            microsoft_client_id = secret_dict["client_id"]
            microsoft_client_secret = secret_dict["client_secret"]

            # Criação do Token da Microsoft
            url = "https://login.microsoftonline.com/356b3283-f55e-4a9f-a199-9acdf3c79fb7/oauth2/v2.0/token"
            payload = f"client_id={microsoft_client_id}&client_secret={microsoft_client_secret}&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default&grant_type=client_credentials"
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Cookie": "fpc=AocyqXXzjRBLm2yxwKDTgJgoPQEuAQAAAL8_XdkOAAAA; stsservicecookie=estsfd; x-ms-gateway-slice=estsfd",
            }
            response = requests.request("POST", url, headers=headers, data=payload)
            token = response.json()["access_token"]

            # Carrega o template do e-mail
            arquivo = open(
                "dags/repo/dags/utils/DagAtualizaNNM/arquivos_html/alerta_alteracao_retroativa.html",
                encoding="UTF-8",
            )
            template = arquivo.read()
            arquivo.close()

            # Corpo da requisição para envio de e-mail
            json_ = {
                "message": {
                    "subject": "Qualidade de Dados Net New Money - Captação - "
                    + isoparse(data_interval_end).strftime("%d/%m/%Y"),
                    "body": {"contentType": "HTML", "content": template},
                    "toRecipients": [
                        {
                            "emailAddress": {
                                "address": "eduardo.queiroz@investimentos.one"
                            }
                        },
                        # {"emailAddress": {"address": "pedro.queiroz@investimentos.one"}}
                    ],
                    "attachments": [
                        {
                            "@odata.type": "#microsoft.graph.fileAttachment",
                            "name": "Net_New_Money_Original.xlsx",
                            "contentBytes": xlsx_original,
                        },
                        {
                            "@odata.type": "#microsoft.graph.fileAttachment",
                            "name": "Net_New_Money_Novo.xlsx",
                            "contentBytes": xlsx_novo,
                        },
                    ],
                }
            }

            # Envio do e-mail via API
            headers = {"Authorization": "Bearer " + token}
            response = requests.post(
                "https://graph.microsoft.com/v1.0/users/d64bcd7f-c47a-4343-b0b4-73f4a8e927a5/sendMail",
                headers=headers,
                json=json_,
            )


def identifica_linhas_coincidentes(df: pd.DataFrame, groupby_columns: list) -> list:
    """Esta função identifica registros que tenham valores iguais (mesmo módulo)
    porém com sinais diferentes dentro de uma mesma base de dados levando em consideração
    os critérios de agrupamento definidos na variável groupby_columns. Para que esta função
    retorne o resultado esperado a coluna contendo a informação numérica que se deseja
    identificar deverá ter o nome Captação.

    Args:
        df (pd.DataFrame): Dataframe que irá ser analisado pela função.
        groupby_columns (list): Colunas pelas quais a função irá aplicar o agrupamento
        para identificar os registros coincidentes.

    Returns:
        list: retorna uma lista com os indices das linhas que são coincidentes.
    """

    # A coluna Flag irá indicar as linhas que possuem inconsistencias
    # a princípio todas as linhas são marcadas com True e vão sendo
    # modificados ao longo do código
    df["Flag"] = True
    df["Captação_abs"] = df["Captação"].abs()

    # O trecho abaixo verifica se em um mesmo dia para e para uma mesma conta o registro é único ou então são multiplos
    # registros com valor de captação igual (inclusive o sinal)
    groupby_set = groupby_columns + ["Captação"]
    df["Captação_count"] = df.groupby(groupby_set)["Captação"].transform("count")

    groupby_set = groupby_columns + ["Captação_abs"]
    df["Captação_abs_count"] = df.groupby(groupby_set)["Captação_abs"].transform(
        "count"
    )

    filtro = df["Captação_count"] == df["Captação_abs_count"]
    df.loc[filtro, "Flag"] = False
    df = df.drop(columns=["Captação_count", "Captação_abs_count"])

    # O código abaixo cria as colunas necessárias para fazer a identificação de pares de movimentações
    # com valor igual e sinal diferente. Para entender melhor o funcionamento deste algorítmo sugiro
    # executar o mesmo linha a linha e printar os resultados. Foi adicionado também um exemplo do
    # funcionamento deste algoritmo na documentação deste código.
    groupby_set = groupby_columns + ["Captação"]
    df["Captação_rank"] = df.groupby(groupby_set)["Captação"].rank("first")
    df["Captação_qtd"] = df.groupby(groupby_set)["Captação_rank"].transform("max")

    groupby_set = groupby_columns + ["Captação_abs"]
    df["Captação_qtd_min"] = df.groupby(groupby_set)["Captação_qtd"].transform("min")

    filtro = df["Captação_rank"] > df["Captação_qtd_min"]
    df.loc[filtro, ["Flag"]] = False
    df = df.drop(
        columns=["Captação_abs", "Captação_rank", "Captação_qtd", "Captação_qtd_min"]
    )

    # Pega o indice das linhas
    idx_movimentacoes = df[df["Flag"]].index.values.tolist()

    return idx_movimentacoes


def remove_registros_inconsistentes(
    df: pd.DataFrame, columns: list, id_registros: dict
) -> list:
    """Esta função foi criada para identificar operações inconsitentes de Net New Money.
    O algoritmo implementado irá verificar se em uma mesma data e para uma mesma conta há operações
    de Net New Money com valores iguais porem com sinais diferentes (operações que somam zero). Este
    algoritmo será executado apenas para as movimentações que estiverem dentro dos mercados e
    tipos de movimentações definidos na variável id_registros.

    Args:
        df (pd.DataFrame): DataFrame com a base de Net New Money ou Movimentação
        columns (list): Lista com as colunas que serão utilizadas para fazer a verificação.
        As colunas devem estar sempre na seguinte ordem: Conta, Mercado, Captação, Data, Tipo
        id_registros (dict): Dicionario onde as chaves indicam os mercados que serão analisados
        e o valor indica os tipos de movimentação que serão analisados pelo algorítmo implementado
        por esta função.

    Returns:
        list: Lista com os indices das linhas que possuem inconsistências de movimentação.
    """

    # Seleciona apenas as colunas necessárias do DataFrame
    # e renomeia as mesmas
    df = df.loc[:, columns].copy()
    df.columns = ["Conta", "Mercado", "Captação", "Data", "Tipo"]

    # Arredonda a coluna de Captação para evitar
    # que problemas de ponto flutuante possam impactar
    # no resultado da função.
    df.loc[:, "Captação"] = df["Captação"].round(2)

    # Lista que será povoada com os registros inconsistentes
    idx_movimentacoes_inconsistentes = []

    # Faz um loop nos mercados/tipos que possuem inconsistencias
    for mercado in id_registros:
        tipo = id_registros[mercado]

        # Filtra o DF apenas com os Tipos e Mercados específicos
        if mercado == "MISTO":
            filtro_mercado = ~(df["Mercado"] == mercado)
        else:
            filtro_mercado = df["Mercado"] == mercado

        # Filtra o DF apenas com os Tipos e Mercados específicos
        filtro_tipo = df["Tipo"].isin(tipo)
        df_selecao = df.loc[filtro_mercado & filtro_tipo, :].copy()

        idx_movimentacoes = identifica_linhas_coincidentes(
            df_selecao, ["Conta", "Data"]
        )

        idx_movimentacoes_inconsistentes += idx_movimentacoes

    return idx_movimentacoes_inconsistentes


def identifica_retirada_e_restituicao_capital(
    df_nnm: pd.DataFrame, df_mov: pd.DataFrame
) -> list:
    """Na base de nnm há registros de retirada
    que são considerados pelo btg de forma errada, para identificação destes
    casos buscamos na base de movimentacao um registro que seja do tipo
    restituição de capital, que tenha ocorrido em até 40 dias após o registro de
    nnm, que possua o mesmo ativo e que tenha a mesma quantidade. Quando estas
    condições são aceitas esta função irá identificar os indices destes registros
    na base de nnm e irá retorna-los.

    Args:
        df_nnm (pd.DataFrame): Base de nnm
        df_mov (pd.DataFrame): Base de movimentação

    Returns:
        list: lista com os indices dos registros da base de nnm que são do tipo
    retirada mas que possuem um "par" com registros do tipo restituição de
    capital na base de movimentação.
    """

    # Seleciona apenas as movimentações do tipo restituição de capital
    filtro = df_mov["type"] == "RESTITUIÇÃO DE CAPITAL"
    df_mov_filtrado = (
        df_mov.loc[filtro, ["account", "date", "amount", "stock"]]
        .sort_values("date")
        .rename(
            columns={
                "account": "conta",
                "date": "data",
                "amount": "Captação",
                "stock": "ativo",
            }
        )
        .astype({"conta": "str"})
        .reset_index()
        .set_index("data")
        .copy()
    )
    stock = df_mov.loc[filtro, "stock"].unique()
    df_mov_filtrado["Captação"] = -df_mov_filtrado["Captação"]

    # Seleciona apenas os nnms do tipo retirada e que
    # possuam os mesmos ativos que os registros de
    # movimentacao filtrados
    filtro_type = df_nnm["type"] == "RETIRADA"
    filtro_stock = df_nnm["stock"].isin(stock)
    df_nnm_filtrado = (
        df_nnm.loc[filtro_type & filtro_stock, ["account", "date", "amount", "stock"]]
        .sort_values("date")
        .rename(
            columns={
                "account": "conta",
                "date": "data",
                "amount": "Captação",
                "stock": "ativo",
            }
        )
        .astype({"conta": "str"})
        .reset_index()
        .set_index("data")
        .copy()
    )

    # Concatena as duas bases filtradas
    df_concat = pd.concat([df_mov_filtrado, df_nnm_filtrado]).sort_values("data")

    # O código abaixo cria uma janela temporal deslizante
    # em cada janela de tempo é aplicada a função que
    # identifica linhas coincidentes considerando que
    # a coluna quantidade será analisada por esta
    # função para a identificação de linhas coincidentes
    idx_linhas = []
    idx_linhas_window = []

    window = pd.Timedelta(days=40)

    for df_window in df_concat.rolling(window=window):
        df_window = df_window.set_index(["index"])
        filtro = df_window.index.isin(idx_linhas)
        df_window = df_window.loc[~filtro, :].copy()
        idx_linhas_window = identifica_linhas_coincidentes(
            df_window, ["conta", "ativo"]
        )
        idx_linhas += idx_linhas_window
        idx_linhas = list(set(idx_linhas))

    return idx_linhas


def identifica_movimentacoes_mesmo_gf(df: pd.DataFrame, columns: list) -> list:
    """Esta função foi criada para realizar a remoção de operações inconsitentes de Net New Money.
    O algoritmo implementado irá verificar se em uma mesma data e para uma mesma conta há operações
    de Net New Money com valores iguais porem com sinais diferentes (operações que somam zero). Este
    algoritmo será executado apenas para as movimentações que estiverem dentro dos mercados e
    tipos de movimentações definidos na variável id_registros.

    Args:
        df (pd.DataFrame): DataFrame com a base de Net New Money ou Movimentação
        columns (list): Lista com as colunas que serão utilizadas para fazer a verificação.
        As colunas devem estar sempre na seguinte ordem: Conta, Mercado, Captação, Data, Tipo
        id_registros (dict): Dicionario onde as chaves indicam os mercados que serão analisados
        e o valor indica os tipos de movimentação que serão analisados pelo algorítmo implementado
        por esta função.

    Returns:
        list: Lista com os indices das linhas que possuem inconsistências de movimentação.
    """

    # Seleciona apenas as colunas necessárias do DataFrame
    # e renomeia as mesmas
    df = df.loc[:, columns].copy()
    df.columns = ["Grupo familiar", "Mercado", "Captação", "Data", "Tipo"]

    # Arredonda a coluna de Captação para evitar
    # que problemas de ponto flutuante possam impactar
    # no resultado da função.
    df["Captação"] = df["Captação"].round(2)

    idx_movimentacoes_inconsistentes = identifica_linhas_coincidentes(
        df,
        [
            "Grupo familiar",
            "Data",
        ],
    )

    return idx_movimentacoes_inconsistentes


def identifica_movimentacoes_mesmo_gf_time_based(
    df: pd.DataFrame, columns: list
) -> list:
    """Esta função foi criada para realizar a remoção de operações inconsitentes de Net New Money.
    O algoritmo implementado irá verificar se em uma mesma data e para uma mesma conta há operações
    de Net New Money com valores iguais porem com sinais diferentes (operações que somam zero). Este
    algoritmo será executado apenas para as movimentações que estiverem dentro dos mercados e
    tipos de movimentações definidos na variável id_registros.

    Args:
        df (pd.DataFrame): DataFrame com a base de Net New Money ou Movimentação
        columns (list): Lista com as colunas que serão utilizadas para fazer a verificação.
        As colunas devem estar sempre na seguinte ordem: Conta, Mercado, Captação, Data, Tipo
        id_registros (dict): Dicionario onde as chaves indicam os mercados que serão analisados
        e o valor indica os tipos de movimentação que serão analisados pelo algorítmo implementado
        por esta função.

    Returns:
        list: Lista com os indices das linhas que possuem inconsistências de movimentação.
    """

    # Seleciona apenas as colunas necessárias do DataFrame
    # e renomeia as mesmas
    df = df.loc[:, columns].copy()
    df.columns = ["Grupo familiar", "Mercado", "Captação", "Data", "Tipo"]

    # Arredonda a coluna de Captação para evitar
    # que problemas de ponto flutuante possam impactar
    # no resultado da função.
    df["Captação"] = df["Captação"].round(2)

    filtro_prev = df["Mercado"] == "PREVIDÊNCIA"
    df_prev = df.loc[filtro_prev, :].copy()
    df_prev = df_prev.reset_index().set_index("Data")

    idx_linhas = []
    idx_linhas_window = []

    window = pd.Timedelta(days=35)

    for df_window in df_prev.rolling(window=window):
        df_window = df_window.set_index(["index"])
        filtro = df_window.index.isin(idx_linhas)
        df_window = df_window.loc[~filtro, :].copy()
        idx_linhas_window = identifica_linhas_coincidentes(
            df_window, ["Grupo familiar"]
        )
        idx_linhas += idx_linhas_window
        idx_linhas = list(set(idx_linhas))

    return idx_linhas


def identifica_movimentacoes_mesmo_gf_agrupadas(df, columns):
    # Seleciona apenas as colunas necessárias do DataFrame
    # e renomeia as mesmas
    df = df.loc[:, columns].copy()
    df.columns = [
        "Conta",
        "Grupo familiar",
        "Mercado",
        "Captação",
        "Data",
        "Tipo",
        "Ativo",
    ]

    df_agrupado = (
        df.groupby(["Grupo familiar", "Conta", "Data", "Mercado", "Ativo"])["Captação"]
        .sum()
        .reset_index()
    )

    # Arredonda a coluna de Captação para evitar
    # que problemas de ponto flutuante possam impactar
    # no resultado da função.
    df_agrupado.loc[:, "Captação"] = df_agrupado["Captação"].round(2)

    ids = identifica_linhas_coincidentes(df_agrupado, ["Grupo familiar", "Data"])

    filtro = df_agrupado.index.isin(ids)
    df_agrupado.loc[filtro, "flag"] = True
    df_agrupado["flag"] = df_agrupado["flag"].fillna(False)

    df = (
        df.reset_index()
        .merge(
            df_agrupado[
                ["Grupo familiar", "Conta", "Data", "Mercado", "Ativo", "flag"]
            ],
            on=["Grupo familiar", "Conta", "Data", "Mercado", "Ativo"],
            how="left",
        )
        .set_index("index")
    )
    df = df[df["flag"]]

    idx_linhas = df.index.to_list()

    return idx_linhas


def envio_email_qualidade_silver(df: pd.DataFrame, data_interval_end: str):
    """Esta função faz o envio do e-mail de alerta para casos em que
    um registro da base de Net New Money superou o valor de
    R$ 100.000.000,00.

    Args:
        df (pd.DataFrame): Dataframe com os registros que serão exibidos no
        e-mail.
        data_interval_end (str): Timestamp no formato ISO que representa o
        'data_interval_end' da Dag Run do Airflow
    """

    # Realiza breve tratamento estético na base de dados.
    map_columns = {
        "account": "Conta",
        "market": "Mercado",
        "description": "Descrição",
        "stock": "Ativo",
        "net_new_money": "Captação",
        "date": "Data",
        "type": "Tipo",
    }
    df = df[list(map_columns)]
    df = df.rename(columns=map_columns)

    # get microsoft graph credentials
    secret_dict = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    microsoft_client_id = secret_dict["client_id"]
    microsoft_client_secret = secret_dict["client_secret"]

    # Criação do Token da Microsoft
    url = "https://login.microsoftonline.com/356b3283-f55e-4a9f-a199-9acdf3c79fb7/oauth2/v2.0/token"
    payload = f"client_id={microsoft_client_id}&client_secret={microsoft_client_secret}&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default&grant_type=client_credentials"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Cookie": "fpc=AocyqXXzjRBLm2yxwKDTgJgoPQEuAQAAAL8_XdkOAAAA; stsservicecookie=estsfd; x-ms-gateway-slice=estsfd",
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    token = response.json()["access_token"]

    # Carrega o template do e-mail
    arquivo = open(
        "dags/repo/dags/utils/DagAtualizaNNM/arquivos_html/alerta_flag_captacao_anormal.html",
        encoding="UTF-8",
    )
    template = arquivo.read()
    arquivo.close()
    template = template.format(
        df.to_html(index=False),
    )

    # Corpo da requisição para envio de e-mail
    json_ = {
        "message": {
            "subject": "Qualidade de Dados Net New Money - Captação - "
            + data_interval_end.strftime("%d/%m/%Y"),
            "body": {"contentType": "HTML", "content": template},
            "toRecipients": [
                {"emailAddress": {"address": "eduardo.queiroz@investimentos.one"}},
            ],
        }
    }

    # Envio do e-mail via API
    headers = {"Authorization": "Bearer " + token}
    response = requests.post(
        "https://graph.microsoft.com/v1.0/users/d64bcd7f-c47a-4343-b0b4-73f4a8e927a5/sendMail",
        headers=headers,
        json=json_,
    )


def atualiza_nnm_silver(
    tipo_atualizacao: Literal["historica", "incremental"],
    data_interval_end: str,
    webhook,
    ti,
):
    # Validação de parâmetro
    TIPOS_ATUALIZACAO = ["historica", "incremental"]
    if not tipo_atualizacao in TIPOS_ATUALIZACAO:
        raise Exception(
            f"O valor atribuído ao parâmetro 'tipo_atualizacao' é {tipo_atualizacao}."
            f"Deveria ser um dentre {TIPOS_ATUALIZACAO}"
        )

    # Variáveis auxiliares
    bucket_lake_silver = get_bucket_name("lake-silver")
    bucket_lake_bronze = get_bucket_name("lake-bronze")

    # Leitura de bases históricas
    df_nnm_historico_one = pd.read_parquet(
        f"s3://{bucket_lake_silver}/api/btg/s3/nnm/nnm_historico.parquet"
    )
    df_nnm_b2c = pd.read_parquet(
        f"s3://{bucket_lake_bronze}/api/one/nnm_b2c/nnm_b2c.parquet",
        columns=[
            "account",
            "name",
            "market",
            "description",
            "stock",
            "amount",
            "net_new_money",
            "date",
            "type",
        ],
    )
    df_account_in_out = pd.read_parquet(
        f"s3://{bucket_lake_bronze}/api/one/accounts_in_out/accounts_in_out.parquet",
        columns=["Conta", "Data", "Abriu/Fechou"],
    )
    df_account_in_out["Conta"] = df_account_in_out["Conta"].astype("int64")

    # Leitura base de contas
    # Abre base de contas gerado pela Dag Run desejada
    dt = DeltaTable(f"s3://{bucket_lake_silver}/btg/contas")
    df_contas = dt.to_pandas(
        partitions=[
            ("date_escrita", "=", isoparse(data_interval_end).strftime("%Y-%m-%d")),
        ]
    )
    df_contas["carteira"] = df_contas["carteira"].astype("int64")

    ano_particao = str(isoparse(data_interval_end).year)
    mes_particao = str(isoparse(data_interval_end).month)

    if tipo_atualizacao == "historica":
        # Leitura de relatórios históricos provenientes do Access
        dt_path = (
            f"s3://{bucket_lake_bronze}/btg/access/relatorio/net_new_money_historico"
        )
        df_nnm_historico_btg = DeltaTable(dt_path).to_pandas(
            partitions=[
                ("ano_particao", "=", ano_particao),
                ("mes_particao", "=", mes_particao),
            ]
        )

        dt_path = (
            f"s3://{bucket_lake_bronze}/btg/access/relatorio/movimentacao_historico"
        )
        df_mov_historico_btg = DeltaTable(dt_path).to_pandas(
            partitions=[
                ("ano_particao", "=", ano_particao),
                ("mes_particao", "=", mes_particao),
            ],
            columns=[
                "conta",
                "nome",
                "mercado",
                "movimentacao",
                "ativo",
                "soma_de_qtd",
                "valor_bruto",
                "dt_compra",
                "lancamento",
                "timestamp_dagrun",
                "timestamp_escrita_bronze",
            ],
        )

        # Filtra última escrita da Dag Run na partição
        df_nnm_historico_btg = df_nnm_historico_btg[
            df_nnm_historico_btg["timestamp_dagrun"] == data_interval_end
        ]
        df_nnm_historico_btg = df_nnm_historico_btg[
            df_nnm_historico_btg["timestamp_escrita_bronze"]
            == df_nnm_historico_btg["timestamp_escrita_bronze"].max()
        ]
        df_mov_historico_btg = df_mov_historico_btg[
            df_mov_historico_btg["timestamp_dagrun"] == data_interval_end
        ]
        df_mov_historico_btg = df_mov_historico_btg[
            df_mov_historico_btg["timestamp_escrita_bronze"]
            == df_mov_historico_btg["timestamp_escrita_bronze"].max()
        ]

        # Renomeia colunas
        map_colunas = {
            "cod_carteira": "account",
            "nome": "name",
            "mercado": "market",
            "tipo": "type",
            "descricao": "description",
            "ativo": "stock",
            "quantidade": "amount",
            "captacao": "net_new_money",
            "data": "date",
        }
        df_nnm_historico_btg.rename(columns=map_colunas, inplace=True)
        df_nnm_historico_btg = df_nnm_historico_btg[map_colunas.values()]

        map_colunas = {
            "conta": "account",
            "nome": "name",
            "mercado": "market",
            "movimentacao": "description",
            "ativo": "stock",
            "soma_de_qtd": "amount",
            "valor_bruto": "net_new_money",
            "dt_compra": "date",
            "lancamento": "type",
        }
        df_mov_historico_btg.rename(columns=map_colunas, inplace=True)
        df_mov_historico_btg = df_mov_historico_btg[map_colunas.values()]

        # Identifica quais mercados e tipos de movimentação devem ser verificados
        id_registro = {
            "FUNDOS": ["TA", "TR"],
            "RENDA VARIÁVEL": ["DEPÓSITO", "RETIRADA"],
            "PREVIDÊNCIA": [
                "TRANSFERÊNCIA DE SAÍDA TOTAL SEM GESTÃO FINANCEIRA",
                "TRANSFERENCIA DE ENTRADA MESMO FUNDO",
            ],
        }

        # Aplica a função que remove registros inconsistentes da base de net new money histórica
        idxs = remove_registros_inconsistentes(
            df_nnm_historico_btg,
            ["account", "market", "net_new_money", "date", "type"],
            id_registro,
        )
        filtro = df_nnm_historico_btg.index.isin(idxs)
        nnm_inconsistente_bronze = df_nnm_historico_btg.loc[filtro, :].copy()
        df_nnm_historico_btg = df_nnm_historico_btg.loc[~filtro, :].copy()

        # Aplica a função que remove registros inconsistentes da base de movimentação histórica
        idxs = remove_registros_inconsistentes(
            df_mov_historico_btg,
            ["account", "market", "net_new_money", "date", "type"],
            id_registro,
        )
        filtro = df_mov_historico_btg.index.isin(idxs)
        movimentacao_inconsistente_bronze = df_mov_historico_btg.loc[filtro, :].copy()
        df_mov_historico_btg = df_mov_historico_btg.loc[~filtro, :].copy()

        # Há evidências de que o BTG pode considerar uma movimentação do tipo RETIRADA como
        # nnm e a movimentação do tipo RESTITUIÇÃO DE CAPITAL (no mesmo dia e com mesmo valor) como não
        # sendo nnm. O código abaixo identifica a ocorrência deste tipo de erro e remove da base
        idxs = identifica_retirada_e_restituicao_capital(
            df_nnm_historico_btg, df_mov_historico_btg
        )
        filtro = df_nnm_historico_btg.index.isin(idxs)
        df_removidos = df_nnm_historico_btg.loc[filtro, :].copy()
        nnm_inconsistente_bronze = pd.concat([nnm_inconsistente_bronze, df_removidos])
        df_nnm_historico_btg = df_nnm_historico_btg.loc[~filtro, :].copy()

        # Há evidências de que o BTG pode considerar uma movimentação do tipo TA como
        # nnm e a movimentação do tipo TR (no mesmo dia e com mesmo valor) como não
        # sendo nnm. O código abaixo identifica a ocorrência deste tipo de erro
        # e levanta uma exceção para que seja verificado manualmente.
        data_min_mov = movimentacao_inconsistente_bronze["date"].min()
        data_max_mov = movimentacao_inconsistente_bronze["date"].max()
        data_min_nnm = nnm_inconsistente_bronze["date"].min()
        data_max_nnm = nnm_inconsistente_bronze["date"].max()

        data_min = max(data_min_mov, data_min_nnm)
        data_max = min(data_max_mov, data_max_nnm)

        mov_ta_tr = movimentacao_inconsistente_bronze[
            (movimentacao_inconsistente_bronze["date"] >= pd.to_datetime(data_min))
            & (movimentacao_inconsistente_bronze["date"] <= pd.to_datetime(data_max))
        ]

        nnm_ta_tr = nnm_inconsistente_bronze[
            (nnm_inconsistente_bronze["date"] >= pd.to_datetime(data_min))
            & (nnm_inconsistente_bronze["date"] <= pd.to_datetime(data_max))
        ]

        nnm_ta_tr_agrupado = (
            nnm_ta_tr.groupby("type")["net_new_money"].sum().reset_index()
        )
        mov_ta_tr_agrupado = (
            mov_ta_tr.groupby("type")["net_new_money"].sum().reset_index()
        )

        validacao = pd.merge(
            nnm_ta_tr_agrupado, mov_ta_tr_agrupado, on="type", suffixes=("_nnm", "_mov")
        )
        validacao["resultado"] = (
            validacao["net_new_money_nnm"] - validacao["net_new_money_mov"]
        )

        resultado_validacao = round(validacao["resultado"].sum())

        if resultado_validacao != 0:
            raise BaseException(
                "Remoção de registros TA, TR inconsistente entre base de NNM e Movimentação. Verificar manualmente as bases."
            )
        # Fim da validação TA TR

        # Há evidências de que o BTG pode considerar uma movimentação do tipo TA como
        # nnm e a movimentação do tipo Débito (no mesmo dia e com mesmo valor) como não
        # sendo nnm. O código abaixo identifica a ocorrência deste tipo de erro
        # e levanta um alerta no grupo de qualidade

        # Filtra as bases históricas considerando
        # as datas máximas e mínimas nas duas e bases e considerando
        # apenas os registros do tipo TA na base de NNM
        # e os registros do tipo DÉBITO na base de
        # movimentação

        data_min_mov = df_mov_historico_btg["date"].min()
        data_max_mov = df_mov_historico_btg["date"].max()
        data_min_nnm = df_nnm_historico_btg["date"].min()
        data_max_nnm = df_nnm_historico_btg["date"].max()

        data_min = max(data_min_mov, data_min_nnm)
        data_max = min(data_max_mov, data_max_nnm)

        mov_deb = df_mov_historico_btg.loc[
            (df_mov_historico_btg["date"] >= pd.to_datetime(data_min))
            & (df_mov_historico_btg["date"] <= pd.to_datetime(data_max))
            & (df_mov_historico_btg["type"] == "DÉBITO"),
            :,
        ]

        nnm_ta = df_nnm_historico_btg.loc[
            (df_nnm_historico_btg["date"] >= pd.to_datetime(data_min))
            & (df_nnm_historico_btg["date"] <= pd.to_datetime(data_max))
            & (df_nnm_historico_btg["type"].isin(["TA"])),
            :,
        ]

        nnm_mov_ta_deb = pd.concat([mov_deb, nnm_ta])

        # Aplica a função que identifica linha inconsistentes nas bases de dados
        id_registro = {"MISTO": ["TA", "DÉBITO"]}

        nnm_mov_ta_deb = nnm_mov_ta_deb.reset_index()

        idxs = remove_registros_inconsistentes(
            nnm_mov_ta_deb.reset_index(drop=True),
            ["account", "market", "net_new_money", "date", "type"],
            id_registro,
        )

        # Seleciona as linhas que estão sendo removidas nas duas bases
        filtro = nnm_mov_ta_deb.index.isin(idxs)
        df_removidos = nnm_mov_ta_deb.loc[filtro, :].copy()

        # Filtra as linhas que serão removidas na base de NNM
        idxs = df_removidos[df_removidos["type"] == "TA"]["index"].to_list()
        filtro = df_nnm_historico_btg.index.isin(idxs)
        df_nnm_removidos = df_nnm_historico_btg.loc[filtro, :].copy()
        df_nnm_historico_btg = df_nnm_historico_btg.loc[~filtro, :].copy()

        # Filtra as linhas que serão removidas na base de Movimentação
        idxs = df_removidos[df_removidos["type"] == "DÉBITO"]["index"].to_list()
        filtro = df_mov_historico_btg.index.isin(idxs)
        df_mov_historico_btg = df_mov_historico_btg.loc[~filtro, :].copy()

        # Gera um alerta no canal do teams caso o tratamento acima remova
        # alguma linha
        if not df_removidos.empty:
            nnm_inconsistente_bronze = pd.concat(
                [nnm_inconsistente_bronze, df_removidos]
            )
            log_qualidade_json = json.dumps(
                {
                    "bucket": bucket_lake_silver,
                    "tabela": "nnm_historico_offshore",
                    "movimentacao_ta_deb": df_removidos.to_json(),
                    "criado_em": datetime.now(tz=pytz.utc).isoformat(),
                },
                ensure_ascii=False,
                indent=2,
            )
            log_name = f"nnm_processamento_silver_{data_interval_end}.json"
            s3 = boto3.client("s3")
            s3.put_object(
                Body=log_qualidade_json,
                Bucket=bucket_lake_silver,
                Key=f"qualidade_dados/nnm_silver/{log_name}",
            )
            webhook = Variable.get("msteams_webhook_qualidade")
            msteams_qualidade_dados(log_qualidade_json, webhook)

        # Fim da validação DÉBITO TA

        # Qundo há algum registro inconsistente a ser removido ele é salvo em um arquivo de logs
        if not nnm_inconsistente_bronze.empty:
            # Confirma se os registros removidos somam zero e printa o número de linhas removidas
            captacao_total_removida = round(
                nnm_inconsistente_bronze["net_new_money"].sum()
            )
            numero_registros_removidos = nnm_inconsistente_bronze.shape[0]
            logging.warning(numero_registros_removidos)
            # if captacao_total_removida != 0:
            #     raise BaseException(
            #         "Os registros identificados como inconsistentes na base de mov cloud não somam zero."
            #     )

            nnm_registros_removidos = pd.read_parquet(
                "s3://one-teste-logs/nnm_silver/nnm_registros_removidos_historico.parquet"
            )
            nnm_registros_removidos = pd.concat(
                [nnm_registros_removidos, nnm_inconsistente_bronze]
            )
            nnm_registros_removidos.to_parquet(
                "s3://one-teste-logs/nnm_silver/nnm_registros_removidos_historico.parquet"
            )

    elif tipo_atualizacao == "incremental":
        map_colunas = {
            "nr_conta": "account",
            "mercado": "market",
            "historico_movimentacao": "description",
            "ativo": "stock",
            "quantidade": "amount",
            "vl_captacao": "net_new_money",
            "vl_bruto": "net_new_money_bruto",
            "dt_movimentacao": "date",
            "flag_nnm": "flagnnm",
            "tipo_lancamento": "type",
        }

        extra_cols = ["timestamp_dagrun", "timestamp_escrita_bronze"]

        dt_mov_bronze = DeltaTable(
            f"s3://{bucket_lake_bronze}/btg/webhooks/downloads/movimentacao"
        )
        movimentacao_bronze = dt_mov_bronze.to_pandas(
            partitions=[
                ("ano_particao", "=", ano_particao),
                ("mes_particao", "=", mes_particao),
            ],
            columns=list(map_colunas.keys()) + extra_cols,
        )
        movimentacao_bronze = movimentacao_bronze[
            (movimentacao_bronze["timestamp_dagrun"] == data_interval_end)
        ]
        movimentacao_bronze = movimentacao_bronze[
            (
                movimentacao_bronze["timestamp_escrita_bronze"]
                == movimentacao_bronze["timestamp_escrita_bronze"].max()
            )
        ]

        # Renomeia colunas
        movimentacao_bronze.rename(columns=map_colunas, inplace=True)
        movimentacao_bronze = movimentacao_bronze[map_colunas.values()]

        # Padroniza dtypes
        movimentacao_bronze["account"] = movimentacao_bronze["account"].astype("int64")
        for col in ["amount", "net_new_money"]:
            movimentacao_bronze[col] = movimentacao_bronze[col].astype("float64")
        movimentacao_bronze["date"] = pd.to_datetime(movimentacao_bronze["date"])

        # Identifica quais mercados e tipos de movimentação devem ser verificados
        id_registro = {
            "FUNDOS": ["TA", "TR"],
            "RENDA VARIÁVEL": ["DEPÓSITO", "RETIRADA"],
            "PREVIDÊNCIA": [
                "TRANSFERÊNCIA DE SAÍDA TOTAL SEM GESTÃO FINANCEIRA",
                "TRANSFERENCIA DE ENTRADA MESMO FUNDO",
            ],
            "MISTO": ["DÉBITO", "TA"],
        }

        # Aplica a função que remove registros inconsistentes da base de movimentação
        idxs_1 = remove_registros_inconsistentes(
            movimentacao_bronze,
            ["account", "market", "net_new_money", "date", "type"],
            id_registro,
        )
        # Cria a base de nnm e a base de movimentação para
        # aplicação da função que identifica retirada e restituição de capital
        df_nnm_historico_btg = movimentacao_bronze[
            movimentacao_bronze["flagnnm"] == "SIM"
        ]
        df_mov_historico_btg = movimentacao_bronze
        idxs_2 = identifica_retirada_e_restituicao_capital(
            df_nnm_historico_btg, df_mov_historico_btg
        )
        idxs = idxs_1 + idxs_2

        filtro = movimentacao_bronze.index.isin(idxs)
        movimentacao_inconsistente_bronze = movimentacao_bronze.loc[filtro, :].copy()

        # Qundo há algum registro inconsistente a ser removido ele é salvo em um arquivo de logs
        if not movimentacao_inconsistente_bronze.empty:
            # Confirma se os registros removidos somam zero e printa o número de linhas removidas
            captacao_total_removida = round(
                movimentacao_inconsistente_bronze["net_new_money"].sum()
            )
            numero_registros_removidos = movimentacao_inconsistente_bronze.shape[0]
            logging.warning(numero_registros_removidos)
            if captacao_total_removida != 0:
                raise BaseException(
                    "Os registros identificados como inconsistentes na base de mov cloud não somam zero."
                )

            nnm_registros_removidos = pd.read_parquet(
                "s3://one-teste-logs/nnm_silver/nnm_registros_removidos.parquet"
            )
            nnm_registros_removidos = pd.concat(
                [nnm_registros_removidos, movimentacao_inconsistente_bronze]
            )
            nnm_registros_removidos.to_parquet(
                "s3://one-teste-logs/nnm_silver/nnm_registros_removidos.parquet"
            )
        movimentacao_bronze = movimentacao_bronze.loc[~filtro, :].copy()

        # Filtra NNM
        df_nnm_historico_btg = movimentacao_bronze[
            movimentacao_bronze["flagnnm"] == "SIM"
        ].drop(columns=["flagnnm"])

        # Filtra Movimentações
        df_mov_historico_btg = movimentacao_bronze.copy()

    # Define data mínima e data máxima de atualização do NNM
    # Se o tipo de atualização for histórica não iremos atualizar
    # os ultimos 7 dias
    if tipo_atualizacao == "historica":
        data_minima = max(
            df_nnm_historico_btg["date"].min().date(),
            (isoparse(data_interval_end) - timedelta(days=30)).date(),
        )
        data_maxima = (isoparse(data_interval_end) - timedelta(days=3)).date()
    elif tipo_atualizacao == "incremental":
        data_minima = (isoparse(data_interval_end) - timedelta(days=4)).date()
        data_maxima = isoparse(data_interval_end).date()

    ti.xcom_push(key="data_maxima_nnm_silver", value=data_maxima.strftime("%Y-%m-%d"))
    ti.xcom_push(key="data_minima_nnm_silver", value=data_minima.strftime("%Y-%m-%d"))

    # Esta copia é feita para verificar se há alguma alteração retroativa de NNM
    # Alteração retroativa é qualquer mudança que ocorre no mês anterior após o dia
    # 20 do mês atual
    df_nnm_final_original = df_nnm_historico_one.loc[
        (df_nnm_historico_one["date"] >= pd.to_datetime(data_minima))
        & (df_nnm_historico_one["date"] <= pd.to_datetime(data_maxima)),
        :,
    ].copy()

    # Filtra período que se deseja manter no histórico. B2C é removido pois
    # é concatenado completo no final do processamento
    df_nnm_historico_one = df_nnm_historico_one[
        (
            (df_nnm_historico_one["date"] < pd.to_datetime(data_minima))
            | (df_nnm_historico_one["date"] > pd.to_datetime(data_maxima))
            | ~(df_nnm_historico_one["account"].isin(df_contas["carteira"]))
        )
        & ~(
            df_nnm_historico_one["type"].isin(
                ["TRANSFERENCIA_SAIDA", "TRANSFERENCIA_ENTRADA"]
            )
        )
    ]

    # Filtra período que se deseja sobreescrever do histórico
    df_nnm_historico_btg = df_nnm_historico_btg[
        (df_nnm_historico_btg["date"] >= pd.to_datetime(data_minima))
        & (df_nnm_historico_btg["date"] <= pd.to_datetime(data_maxima))
    ]
    df_mov_historico_btg = df_mov_historico_btg[
        (df_mov_historico_btg["date"] >= pd.to_datetime(data_minima))
        & (df_mov_historico_btg["date"] <= pd.to_datetime(data_maxima))
    ]

    df_account_in_out = df_account_in_out[
        (df_account_in_out["Data"] >= pd.to_datetime(data_minima))
    ]

    # Adições no NNM por conceito definido pela One
    df_mov_historico_btg = filtra_nnm_em_movimentacoes(df_mov_historico_btg)

    # Se a execução for histórica, a coluna 'flagnnm' não existe no DataFrame
    if "flagnnm" in df_mov_historico_btg.columns:
        df_mov_historico_btg = df_mov_historico_btg.drop(columns=["flagnnm"])

    # Une NNMs advindos de bases de NNM e movimentações
    df_nnm_novos = pd.concat([df_nnm_historico_btg, df_mov_historico_btg])
    df_nnm_novos = df_nnm_novos[df_nnm_novos["account"].isin(df_contas["carteira"])]

    # Se a execução for histórica, a coluna 'net_new_money_bruto' não existe no DataFrame
    if "net_new_money_bruto" in df_nnm_novos.columns:
        df_nnm_novos = df_nnm_novos.drop(columns=["net_new_money_bruto"])

    # Mantém apenas registros maiores ou iguais a data de entrada de B2C ou
    # menores ou iguais a data de saída
    df_nnm_novos.sort_values("date", inplace=True)
    df_account_in_out.sort_values("Data", inplace=True)

    # Ajuste de data de saída é necessário para filtrarmos movimentações
    # que ficariam redundantes com o B2C. Ex: movimentação de retirada no
    # dia 28 igual ao B2C do dia 29
    df_account_in_out.reset_index(drop=True, inplace=True)
    df_account_in_out.loc[
        (df_account_in_out["Abriu/Fechou"] == "Fechou")
        & (df_account_in_out["Data"].dt.dayofweek != 0),
        "Data",
    ] = df_account_in_out["Data"] - pd.Timedelta(days=2)
    df_account_in_out.loc[
        (df_account_in_out["Abriu/Fechou"] == "Fechou")
        & (df_account_in_out["Data"].dt.dayofweek == 0),
        "Data",
    ] = df_account_in_out["Data"] - pd.Timedelta(days=3)
    df_account_in_out = df_account_in_out.sort_values("Data")

    df_nnm_novos = pd.merge_asof(
        df_nnm_novos,
        df_account_in_out,
        left_by="account",
        right_by="Conta",
        left_on="date",
        right_on="Data",
    )

    # O merge_asof não dá match com linhas anteriores à primeira
    # data do DataFrame da direita. Ou seja, movimentações anteriores
    # à primeira data presente no accounts_in_out ficarão sem data de
    # Abriu/Fechou. A lógica abaixo visa resolver esse problema
    primeiras_entradas = (
        df_account_in_out[df_account_in_out["Abriu/Fechou"] == "Abriu"]
        .groupby("Conta")["Data"]
        .min()
        .reset_index()
    )
    primeiras_entradas.rename(
        columns={"Data": "Primeira Entrada", "Conta": "account"}, inplace=True
    )
    df_nnm_novos = df_nnm_novos.merge(primeiras_entradas, on="account", how="left")

    # Filtra movimentações por causa do b2c
    df_nnm_novos = df_nnm_novos[
        ~(
            (df_nnm_novos["date"] < df_nnm_novos["Data"])
            & (df_nnm_novos["Abriu/Fechou"] == "Abriu")
        )
        & ~(
            (df_nnm_novos["date"] >= df_nnm_novos["Data"])
            & (df_nnm_novos["Abriu/Fechou"] == "Fechou")
        )
        & ~(df_nnm_novos["date"] < df_nnm_novos["Primeira Entrada"])
    ].drop(columns=["Conta", "Data", "Abriu/Fechou", "Primeira Entrada"])

    df_nnm_final = pd.concat(
        [df_nnm_novos, df_nnm_b2c, df_nnm_historico_one]
    ).sort_values(["date", "account"])

    # Realiza algumas padronizações nos dados
    df_nnm_final["market"] = df_nnm_final["market"].str.upper()

    # Grava Net New Money
    df_nnm_final["account"] = df_nnm_final["account"].astype("int64")

    # Filtro a NNM sobreescrito
    df_nnm_final_novo = df_nnm_final.loc[
        (df_nnm_final["date"] >= pd.to_datetime(data_minima))
        & (df_nnm_final["date"] <= pd.to_datetime(data_maxima)),
        :,
    ].copy()

    verifica_alteracao_retroativa(
        df_nnm_final_original, df_nnm_final_novo, data_interval_end
    )

    df_nnm_final.to_parquet(
        f"s3://{bucket_lake_silver}/api/btg/s3/nnm/nnm_historico.parquet"
    )


def atualiza_nnm_silver_com_offshore(drive_id: str):
    bucket_lake_silver = get_bucket_name("lake-silver")

    # Leitura do NNM Silver
    df_nnm_historico = pd.read_parquet(
        f"s3a://{bucket_lake_silver}/api/btg/s3/nnm/nnm_historico.parquet"
    )
    df_nnm_historico = df_nnm_historico.astype({"account": "str", "type": "str"})

    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)
    df_offshore = drive.download_file(
        ["One Analytics", "Banco de dados", "Histórico", "Net New Money OFFSHORE.xlsx"],
        to_pandas=True,
        usecols=[0, 6, 7],
    )
    df_offshore.columns = ["account", "net_new_money", "date"]

    # Trata a base de nnm offshore
    df_offshore = df_offshore.loc[df_offshore["net_new_money"] != 0, :]
    df_offshore["account"] = df_offshore["account"].astype(str)
    df_offshore["date"] = pd.to_datetime(df_offshore["date"])
    df_offshore = df_offshore.sort_values(["date", "account"]).reset_index(drop=True)
    df_offshore = df_offshore[
        ~(
            (df_offshore["account"].isin(["80961", "81686", "2692382"]))
            & (df_offshore["net_new_money"] < 0)
        )
    ]

    # Define as colunas market e type para a base offshore
    filtro = df_offshore["net_new_money"] >= 0
    df_offshore.loc[filtro, "type"] = "CRÉDITO"
    df_offshore.loc[~filtro, "type"] = "DÉBITO"
    df_offshore["market"] = "OFF-SHORE"

    # Remove linhas da base de Off-shore que tenham captação vaiza
    df_offshore = df_offshore.dropna(subset="net_new_money")

    # Une o nnm_histórico com o nnm_offshore e salva na silver
    df_nnm_historico = pd.concat([df_nnm_historico, df_offshore])
    df_nnm_historico.to_parquet(
        f"s3://{bucket_lake_silver}/api/btg/s3/nnm/nnm_historico_com_offshore.parquet"
    )


def verifica_qualidade_silver(data_interval_end: datetime, webhook, ti):
    data_minima = ti.xcom_pull(key="data_minima_nnm_silver")
    data_maxima = ti.xcom_pull(key="data_maxima_nnm_silver")
    bucket_lake_silver = get_bucket_name("lake-silver")

    # Verifica novas categorias na coluna market
    categorias_mercado_padrao = [
        "CONTA CORRENTE",
        "TRANSFERENCIA_ENTRADA",
        "RENDA VARIÁVEL",
        "RENDA FIXA",
        "FUNDOS",
        "PREVIDÊNCIA",
        "DERIVATIVOS",
        "TRANSFERENCIA_SAIDA",
        "VALOR EM TRÂNSITO",
        "CRIPTOATIVOS",
        "OFF-SHORE",
        "CRY",
        "PRECATÓRIOS",
    ]

    categorias_tipo_padrao = [
        "CRÉDITO",
        "TRANSFERENCIA_ENTRADA",
        "DEPÓSITO",
        "TRANSFERÊNCIA DE CUSTÓDIA",
        "DEPÓSITO DE CUSTÓDIA",
        "RETIRADA DE CUSTÓDIA",
        "DÉBITO",
        "TA",
        "PORTABILIDADE DE ENTRADA",
        "RETIRADA",
        "TR",
        "PORTAB. SAÍDA TOTAL",
        "CONTRIBUICAO - APLICACAO",
        "TRANSFERENCIA EXTERNA -ENTRADA",
        "UNIFICACAO  RESERVA ENTRADA",
        "MUDANCA DE FUNDO - SAIDA",
        "MUDANCA DE FUNDO -  ENTRADA",
        "PORTABILIDADE DE SAÍDA PARCIAL REAIS",
        "TRANSFERENCIA EXTERNA - SAIDA",
        "TRANSFERENCIA_SAIDA",
        "None",
        "TRANSFERÊNCIA DE SAÍDA TOTAL SEM GESTÃO FINANCEIRA",
        "TRANSFERENCIA DE ENTRADA MESMO FUNDO",
        "AQ",
        "RESGATE",
        "CONTRIBUIÇÃO",
        "APORTE",
        "UNIFICACAO  RESERVA SAIDA",
        "ALTERAÇÃO DE CADASTRO DE CONTA - ENTRADA",
        "ALTERAÇÃO DE CADASTRO DE CONTA - SAÍDA",
        "RS",
        "ALTERACAO DO CADASTRO DE PREVIDÊNCIA EXTERNA(ENTRADA)",
        "DEPOSITO",
        "PORTAB. SAÍDA PARCIAL",
        "RESGATE PARCIAL EM REAIS",
    ]

    # Faz a leitura do NNM e separa o período que foi atualizado para que seja validado.
    df_nnm = pd.read_parquet(
        f"s3a://{bucket_lake_silver}/api/btg/s3/nnm/nnm_historico_com_offshore.parquet"
    )

    df_nnm = df_nnm.loc[
        (df_nnm["date"] >= pd.to_datetime(data_minima))
        & (df_nnm["date"] <= pd.to_datetime(data_maxima)),
        :,
    ]

    # Verifica se foram acrescentadas novas categorias na coluna
    # mercado da base de NNM.
    categorias_mercado = df_nnm["market"].unique().tolist()
    novas_categorias_mercado = [
        categoria
        for categoria in categorias_mercado
        if categoria not in categorias_mercado_padrao
    ]
    if len(novas_categorias_mercado) != 0:
        flag_mercado = True
    else:
        flag_mercado = False

    # Verifica se foram acrescentadas novas categorias na coluna
    # tipo da base de NNM.
    categorias_tipo = df_nnm["type"].unique().tolist()
    novas_categorias_tipo = [
        categoria
        for categoria in categorias_tipo
        if categoria not in categorias_tipo_padrao
    ]
    if len(novas_categorias_tipo) != 0:
        flag_tipo = True
    else:
        flag_tipo = False

    # Verifica se houve o acréscimo de algum registro com
    # captação maior ou igual a R$ 100.000.000,00
    filtro_captacao = df_nnm["net_new_money"] >= 100000000
    df_captacao_suspeita = df_nnm.loc[filtro_captacao, :]
    if not df_captacao_suspeita.empty:
        flag_captacao = True
    else:
        flag_captacao = False

    # Cria o log de qualidade
    log_qualidade_json = json.dumps(
        {
            "bucket": bucket_lake_silver,
            "tabela": "nnm_historico_offshore",
            "novas_categorias_mercado": novas_categorias_mercado,
            "novas_categorias_tipo": novas_categorias_tipo,
            "captacao_suspeita": df_captacao_suspeita.to_json(),
            "criado_em": datetime.now(tz=pytz.utc).isoformat(),
        },
        ensure_ascii=False,
        indent=2,
    )

    # Caso algumas das condições de qualidade acima sejam atendidas
    # o log de qualidade será salvo e um alerta será enviado no
    # canal de qualidade do teams
    cond = flag_mercado or flag_tipo or flag_captacao

    if cond:
        log_name = f"nnm_silver_{data_interval_end}.json"
        s3 = boto3.client("s3")
        s3.put_object(
            Body=log_qualidade_json,
            Bucket=bucket_lake_silver,
            Key=f"qualidade_dados/nnm_silver/{log_name}",
        )
        webhook = Variable.get("msteams_webhook_qualidade")
        msteams_qualidade_dados(log_qualidade_json, webhook)

    # Caso a condição de captação suspeita seja atendida é
    # enviado um e-mail para a área de negócios
    if flag_captacao:
        envio_email_qualidade_silver(df_captacao_suspeita, data_interval_end)


def atualiza_nnm_gold(drive_id: str):
    MAP_TIPO = {
        "CONTRIBUIÇÃO": "APORTE",
        "CONTRIBUICAO - APLICACAO": "APORTE",
        "APORTE": "APORTE",
        "DEPÓSITO": "CAPTAÇÃO ",
        "DEPOSITO": "CAPTAÇÃO",
        "CRÉDITO": "CRÉDITO",
        "DÉBITO": "DÉBITO",
        "AQ": "APLICAÇÃO",
        "TA": "MIGRAÇÃO DE FUNDOS ENTRADA",
        "TR": "MIGRAÇÃO DE FUNDOS SAÍDA",
        "PORTABILIDADE DE ENTRADA": "PORTABILIDADE ENTRADA",
        "MUDANCA DE FUNDO -  ENTRADA": "PORTABILIDADE ENTRADA",
        "TRANSFERENCIA EXTERNA -ENTRADA": "PORTABILIDADE ENTRADA",
        "UNIFICACAO  RESERVA ENTRADA": "PORTABILIDADE ENTRADA",
        "ALTERAÇÃO DE CADASTRO DE CONTA - ENTRADA": "PORTABILIDADE ENTRADA",
        "ALTERACAO DO CADASTRO DE PREVIDÊNCIA EXTERNA(ENTRADA)": "PORTABILIDADE ENTRADA",
        "MUDANCA DE FUNDO - SAIDA": "PORTABILIDADE SAÍDA",
        "TRANSFERENCIA EXTERNA - SAIDA": "PORTABILIDADE SAÍDA",
        "PORTAB. SAÍDA TOTAL": "PORTABILIDADE SAÍDA",
        "UNIFICACAO  RESERVA SAIDA": "PORTABILIDADE SAÍDA",
        "PORTABILIDADE DE SAÍDA PARCIAL REAIS": "PORTABILIDADE SAÍDA",
        "ALTERAÇÃO DE CADASTRO DE CONTA - SAÍDA": "PORTABILIDADE SAÍDA",
        "RS": "RESGATE",
        "RESGATE": "RESGATE PREVIDÊNCIA EXTERNA",
        "RETIRADA": "SAQUE",
        "TRANSFERÊNCIA DE CUSTÓDIA": "TRANSFERÊNCIA DE CUSTÓDIA",
        "DEPÓSITO DE CUSTÓDIA": "TRANSFERÊNCIA DE CUSTÓDIA ENTRADA",
        "RETIRADA DE CUSTÓDIA": "TRANSFERÊNCIA DE CUSTÓDIA SAÍDA",
        "TRANSFERENCIA_ENTRADA": "TROCA ASSESSORIA ENTRADA",
        "TRANSFERENCIA_SAIDA": "TROCA ASSESSORIA SAÍDA",
        "TRANSFERENCIA DE ENTRADA MESMO FUNDO": "UNIFICAÇÃO CERTIFICADOS PREVIDÊNCIA",
        "TRANSFERÊNCIA DE SAÍDA TOTAL SEM GESTÃO FINANCEIRA": "UNIFICAÇÃO CERTIFICADOS PREVIDÊNCIA",
    }

    MAP_MERCADO = {"CRY": "CRIPTOATIVOS"}

    bucket_lake_gold = get_bucket_name("lake-gold")
    bucket_lake_silver = get_bucket_name("lake-silver")

    # Leitura do NNM Silver
    df_nnm = pd.read_parquet(
        f"s3a://{bucket_lake_silver}/api/btg/s3/nnm/nnm_historico_com_offshore.parquet"
    )
    df_nnm["account"] = df_nnm["account"].astype(str)
    df_nnm["date"] = pd.to_datetime(df_nnm["date"])

    # Abrindo base contas historico
    df_advisors = pd.read_parquet(
        f"s3a://{bucket_lake_silver}/api/btg/crm/ContasHistorico/ContasHistorico.parquet"
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

    # Carregando a base sócios
    df_contrata = pd.read_parquet(
        f"s3://{bucket_lake_gold}/api/one/socios/socios.parquet"
    )
    df_contrata["Data início"] = pd.to_datetime(df_contrata["Data início"])
    df_contrata["Data Saída"] = pd.to_datetime(df_contrata["Data Saída"])
    df_contrata_red = df_contrata[["Nome_bd", "Data início", "Data Saída"]]

    # Tratando a base de NNM
    map_colunas = {
        "account": "Conta",
        "market": "Mercado",
        "description": "Descricao",
        "net_new_money": "Captação",
        "date": "Data",
        "type": "Tipo",
        "stock": "Ativo",
    }

    df_nnm = (
        df_nnm.loc[:, list(map_colunas)].rename(map_colunas, axis=1).sort_values("Data")
    )

    df_nnm["Tipo"] = df_nnm["Tipo"].replace(MAP_TIPO)
    df_nnm["Mercado"] = df_nnm["Mercado"].replace(MAP_MERCADO)

    # Início do Merge-asof
    df_nnm_cp = df_nnm.copy()
    df_nnm = pd.merge_asof(
        df_nnm, df_advisors, on="Data", by="Conta", direction="backward"
    )  # Une com o clientes assessores
    temp = pd.merge_asof(
        df_nnm_cp, df_advisors, on="Data", by="Conta", direction="forward"
    )  # Para preencher missing apenas
    df_nnm = fill_retroativo(df_nnm, temp)
    df_nnm["Mercado"].fillna("CONTA CORRENTE", inplace=True)
    df_nnm = df_nnm.sort_values("Data").reset_index(drop=True)
    df_nnm.drop("Descricao", axis=1, inplace=True)
    df_nnm = remover_antes_entrada(df_nnm, df_contrata_red)
    df_nnm = df_nnm.sort_values("Data").reset_index(drop=True)
    df_nnm["Nome e Conta"] = (
        df_nnm["Nome"].str.split(" ").str[0] + " - " + df_nnm["Conta"]
    )
    # Fim do Merge-asof

    # Dinheiro novo é o enviado por cada conta dentro dos 30 primeiros dias a partir do primeiro aporte
    primeiro_aporte = (
        df_nnm.groupby("Conta")["Data"]
        .min()
        .reset_index()
        .rename(columns={"Data": "new_money_limit"})
    )
    # Calcula a data limite para o dinheiro novo
    primeiro_aporte["new_money_limit"] = primeiro_aporte[
        "new_money_limit"
    ] + pd.DateOffset(days=30)
    # Define quais captações são dinheiro novo
    df_nnm = df_nnm.merge(primeiro_aporte, on="Conta", how="left")
    df_nnm.loc[df_nnm["Data"] <= df_nnm["new_money_limit"], "Dinheiro novo"] = True
    df_nnm["Dinheiro novo"] = df_nnm["Dinheiro novo"].fillna(False)
    df_nnm.drop("new_money_limit", inplace=True, axis=1)

    # Cria uma coluna definindo que o NNM desta base é referente à Oneinvestimentos Assessoria de Investimentos (Classificação 0)
    df_nnm["Negócio"] = "AAI"

    # Carrega o DataFrame com a indicação das linhas que precisam ser subistituiídas em função da divisão de captação entre áreas.
    df_divisao_valores = pd.read_parquet(
        f"s3://{bucket_lake_gold}/api/one/classificacao_3/nnm_divisao_valores.parquet"
    )

    # Realiza a divisão da captação entre áreas com base nas informações da base df_divisao_valores.
    df_nnm = atualizar_divisao_valores(df_nnm, df_divisao_valores)

    # Salva a base de NNM em cloud para ser utilizada pelo aplicativo de divisão entre áreas.
    df_nnm = df_nnm.astype({"Id Conta": "str", "Contato/Empresa": "str"})

    df_nnm.to_parquet(
        f"s3://{bucket_lake_gold}/api/one/net_new_money/net_new_money_local.parquet"
    )

    df_nnm = mergeasof_socios(
        df_nnm,
        "Participação",
        "Data",
        {
            "area": "Área Participação",
            "business_unit": "Unidade de Negócios Participação",
            "department": "Departamento Participação",
            "business_leader":"Business Leader Participação",
            "team_leader":"Team Leader Participação"
        },
    )

    df_nnm = mergeasof_socios(df_nnm, "Comercial", "Data")

    # Salva o nnm local para processamento junto com a base da Gestora
    buffer = io.BytesIO()
    df_nnm.to_parquet(buffer)
    bytes_nnm = buffer.getvalue()

    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )

    drive = OneGraphDrive(drive_id=drive_id, **credentials)
    drive.upload_file(
        [
            "One Analytics",
            "OWM Analytics",
            "Banco de dados",
            "nnm_gold_aai_teste.parquet",
        ],
        bytes_nnm,
    )

    # Carrega a base de NNM da Gestora
    drive = OneGraphDrive(drive_id=drive_id, **credentials)
    df_nnm_gestora = drive.download_file(
        ["One Analytics", "OWM Analytics", "Banco de dados", "nnm_gold.parquet"],
        to_pandas=True,
    )

    df_nnm_gestora["Negócio"] = "GESTORA"
    df_nnm = pd.concat([df_nnm, df_nnm_gestora])
    df_nnm = df_nnm.reset_index(drop=True)

    # Realiza alguns tratamentos na base de dados
    filtro = (df_nnm["Ativo"].isna()) & (df_nnm["Mercado"] == "OFF-SHORE")
    df_nnm.loc[filtro, "Ativo"] = df_nnm["Conta"]
    df_nnm = df_nnm.dropna(subset="Ativo")

    # Aplica as funções que identificam transações coincidentes e
    # cria a coluna de NNM real
    idx_mesmo_gf = identifica_movimentacoes_mesmo_gf(
        df_nnm, columns=["Grupo familiar", "Mercado", "Captação", "Data", "Tipo"]
    )
    filtro = df_nnm.index.isin(idx_mesmo_gf)

    idx_temporal = identifica_movimentacoes_mesmo_gf_time_based(
        df_nnm[~filtro], ["Grupo familiar", "Mercado", "Captação", "Data", "Tipo"]
    )
    filtro = df_nnm.index.isin(idx_mesmo_gf + idx_temporal)

    idx_agrupado = identifica_movimentacoes_mesmo_gf_agrupadas(
        df_nnm[~filtro],
        ["Conta", "Grupo familiar", "Mercado", "Captação", "Data", "Tipo", "Ativo"],
    )
    filtro = df_nnm.index.isin(idx_mesmo_gf + idx_temporal + idx_agrupado)

    df_nnm["NNM Verdadeiro"] = True
    df_nnm.loc[filtro, "NNM Verdadeiro"] = False

    filtro = df_nnm["NNM Verdadeiro"] == False
    resultado_validacao = abs(df_nnm.loc[filtro, :]["Captação"].sum())

    if resultado_validacao > 1:
        raise BaseException("Os valores de NNM False superam R$ 1,00")

    df_nnm = df_nnm.drop(["Ativo", "nome"], axis=1)

    filtro = df_nnm["Negócio"] != "GESTORA"
    df_nnm = df_nnm[filtro]

    # Salva na gold
    df_nnm.to_parquet(
        f"s3a://{bucket_lake_gold}/api/btg/s3/nnm/NNM.parquet", index=False
    )


def filtra_nnm_em_movimentacoes(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra registros de DataFrame contendo movimentações
    da Base BTG para serem incluídos na nossa base de Net New Money da One
    a partir de critérios especiais. Todos os critérios aqui presentes foram
    adicionados por não serem considerados como Net New Money pelos critérios
    do BTG, então precisamos buscá-los da base de movimentações.

    Args:
        df (pd.DataFrame): DataFrame de movimentações do BTG com schema da camada
        bronze do Lake

    Returns:
        pd.DataFrame: DataFrame contendo apenas registros de movimentação
        selecionados para serem incrementados ao Net New Money
    """
    criterio = (
        (
            (df["market"] == "DERIVATIVOS")
            & (df["description"].str.contains("LIQUID|VENCIMENTO NDF"))
            & (df["net_new_money"] > 0)
        )
        | (
            (df["description"].astype(str).str.contains("LIQ. DE FATURA"))
            & (df["date"] >= "2022-07-20")
        )
        | (
            (df["market"] == "CONTA CORRENTE")
            & (df["description"].str.contains("PAGTO DE FEE DE ESTRUC/ASSESS"))
            & (df["date"] > "2021-08-04")
        )
    )

    # Se a execução for histórica, a coluna 'net_new_money_bruto' não existe no DataFrame
    if "net_new_money_bruto" in df.columns:
        df.loc[criterio, "net_new_money"] = df["net_new_money_bruto"]
    return df[criterio]
