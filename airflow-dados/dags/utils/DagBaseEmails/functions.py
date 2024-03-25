import pandas as pd
import json
from datetime import datetime
import boto3
from typing import Union
from dateutil.parser import isoparse
from dateutil.relativedelta import relativedelta
from deltalake import write_deltalake, DeltaTable

from utils.OneGraph.drive import OneGraphDrive
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name


def requisicao_graph_api(drive: str, max_date: str, user_email: str) -> list:
    """Fazer requisição no endpoint que lista os e-mails do usuário especificado

    Args:
        drive (str): drive onde estão as informações do e-mail
        max_date (str): data do último registro na base
        user_email (str): e-mail do usuário. Geralmente D-1

    Returns:
        list: JSON contendo data de envio, remetente e destinatário de
        todos e-mails do usuário desde o último registro (max-date)
    """
    # Especificar parâmetros da requisição
    # Estou passando como string para evitar a formatação no momento da requisição
    kwargs = {
        "params": (
            "$select=sentDateTime,from,toRecipients,ccRecipients"
            + f"&$filter=sentDateTime+ge+{max_date}T00:00:00Z"
        )
    }

    # Fazer a requisição utilizando o e-mail do usuário
    files = drive.list_email_endpoint(user_email, **kwargs)

    # Inicializar as variáveis
    url = None
    json_files = []

    # Iterar até ler todos os e-mails
    while True:
        # Se a variável "url" já possuir um valor,
        # a requisição será no enpoint especificado no response anterior
        if url:
            files = drive.list_email_endpoint(**{"endpoint": url.split("v1.0/")[-1]})

        # Adicionar os valores à lista de responses
        json_files += files.json()["value"]

        # Caso tenha outra página, ler os e-mails utilizando o link
        try:
            url = files.json()["@odata.nextLink"]

        # Caso contrário, encerrar a iteração
        except:
            break

    return json_files


def requisicao_por_usuario(
    drive_id: str,
    s3_bucket,
    s3_table_key: str,
    s3_partitions_key: str,
    s3_bucket_save: str,
    timestamp_dagrun: datetime,
) -> list:
    """Fazer requisição no endpoint que lista os e-mails para cada usuário especificado
    e armazenar resultados em um único JSON

    Args:
        drive_id (str): ID do drive do Onedrive onde estão os e-mails
        s3_bucket (str): bucket em que o JSON foi salvo
        s3_table_key (str): path em que o JSON foi salvo
        s3_partitions_key (str): partições utilizadas para salvar o arquivo
        s3_bucket_save (str): bucket em que o arquivo foi salvo após os tratamentos
        timestamp_dagrun (datetime): horário que a DAG foi executada. Usado para especificar o arquivo

    Returns:
        list: JSON contendo data de envio, remetente e destinatário de
        todos e-mails para todos os usuários especificados
    """
    # Credenciais da Graph API da Microsoft
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    bucket_lake_gold = get_bucket_name("lake-gold")

    today = datetime.now().strftime("%Y-%m-%d")

    # Ler a base de sócios
    df_socios_raw = pd.read_parquet(
        f"s3://{bucket_lake_gold}/api/one/socios/socios.parquet"
    )

    # Selecionar sócios para ler os registros de e-mails
    df_socios = df_socios_raw.loc[
        (df_socios_raw["Data Saída"].isna())
        & (df_socios_raw["Data início"] < today)
        & (df_socios_raw["Departamento"].isin(["PRODUTOS"]))
        & (df_socios_raw["Área"].astype(str).str.contains("MESA RF|MESA RV"))
    ]

    # Padrnoizar e-mails
    df_socios.loc[:, "E-mail"] = (
        df_socios["E-mail"].astype(str).str.replace("oneinv.com", "investimentos.one")
    )

    # Converter coluna em lista
    lista_emails = df_socios["E-mail"].to_list()

    print(lista_emails)

    # Leitura últimos meses na Silver
    # Definir timestamp do início da DAG
    ts_parsed = isoparse(timestamp_dagrun)

    # Criar lista com últimos dois meses, incluindo o atual
    ts_list = [ts_parsed - relativedelta(months=i) for i in range(0, 3)]

    # Ler as partições especificadas
    dt = DeltaTable(f"s3://{s3_bucket_save}/{s3_table_key}")
    df = dt.to_pandas(
        filters=[
            [("ano_particao", "=", str(ts.year)), ("mes_particao", "=", str(ts.month))]
            for ts in ts_list
        ]
    )

    # Identificar timestamp da última execução agendada da DAG
    max_date = df["timestamp_dagrun"].max()
    max_date = max_date.strftime("%Y-%m-%d")

    print(max_date)

    # Inicializar variável
    json_result = []

    # Iterar sobre a lista de usuários
    for user_email in lista_emails:
        # Fazer requisição na API para o usuário em questão
        json_user = requisicao_graph_api(drive, max_date, user_email)

        # Adicionar resultado no JSON final
        json_result += [json_user]

    json_result = json.dumps(json_result[0], ensure_ascii=False)

    s3 = boto3.client("s3")
    s3.put_object(
        Body=json_result,
        Bucket=s3_bucket,
        Key=f"{s3_table_key}/{s3_partitions_key}/emails_{timestamp_dagrun}.json",
    )


def expand_columns(json_data: str) -> pd.DataFrame:
    """Transformar colunas que são listas ou dicionários
    em novas colunas e expandir para novas linhas

    Args:
        json_data (str): response da requisição no endpoint de e-mails da Graph API

    Returns:
        pd.DataFrame: dataframe contendo o "log" dos e-mails, sendo 1 linha por destinatário para cada e-mail
    """
    # Expandir colunas
    # Definir colunas de referência
    merge_cols = ["id", "sentDateTime"]

    # Colunas de lista de dicionários
    to_df = pd.json_normalize(json_data, ["toRecipients"], merge_cols)
    cc_df = pd.json_normalize(json_data, ["ccRecipients"], merge_cols)

    # Coluna de dicionários
    from_df = pd.json_normalize(json_data)

    # Unir os dataframes pelo id e data
    df = from_df.merge(to_df, on=merge_cols, how="outer")
    df = df.merge(cc_df, on=merge_cols, how="outer", suffixes=("_to", "_cc"))

    df["sentDateTime"] = pd.to_datetime(
        pd.to_datetime(df["sentDateTime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    )

    df = df.drop(["toRecipients", "ccRecipients"], axis=1)

    # Renomear as colunas
    df.rename(
        {
            "from.emailAddress.address": "from",
            "emailAddress.address_to": "toRecipients",
            "emailAddress.address_cc": "ccRecipients",
        },
        axis=1,
        inplace=True,
    )

    return df


def leitura_landing(
    s3_bucket: str,
    s3_table_key: str,
    s3_partitions_key: str,
    timestamp_dagrun: datetime,
    expand_cols: bool = False,
) -> pd.DataFrame:
    """Ler o JSON salvo na landing e converte-lo em dataframe

    Args:
        s3_bucket (str): bucket em que o JSON foi salvo
        s3_table_key (str): path em que o JSON foi salvo
        s3_partitions_key (str): partições utilizadas para salvar o arquivo
        timestamp_dagrun (datetime): horário que a DAG foi executada. Usado para especificar o arquivo
        expand_cols (bool): definir se é para aplicar a função "expand_columns" ou não. Defaults to False

    Returns:
        pd.DataFrame: dataframe criado a partir do response da requisição
    """
    s3 = boto3.resource("s3")

    key = f"{s3_table_key}/{s3_partitions_key}/emails_{timestamp_dagrun}.json"

    obj = s3.Object(s3_bucket, key)
    data = obj.get()["Body"].read().decode("utf-8")
    json_data = json.loads(data)

    if expand_cols:
        return expand_columns(json_data)

    return pd.DataFrame(json_data)


def tratamento_base_emails(
    s3_bucket_read: str,
    s3_table_key: str,
    s3_partitions_key: str,
    s3_bucket_save: str,
    timestamp_dagrun: str,
):
    """Função para ler arquivo da bronze, aplicar tratamentos necessários,
    sendo o principal o explode das colunas de destinatários, e salvar na silver

    Args:
        s3_bucket_read (str): bucket do arquivo sem tratamentos
        s3_table_key (str): path em que o arquivo foi salvo
        s3_partitions_key (str): partições utilizadas para salvar o arquivo
        s3_bucket_save (str): bucket em que o arquivo será salvo após os tratamentos
        timestamp_dagrun (str): horário em que a dag foi trigada. Usado para particionar o arquivo
    """
    # Especificar o caminho para leitura do arquivo
    path = f"s3://{s3_bucket_read}/{s3_table_key}"

    # Converter timestamp_dagrun para datetime
    timestamp_dagrun = datetime.strptime(timestamp_dagrun, "%Y%m%dT%H%M%S")

    # Ler o arquivo mais atual de acordo com o timestamp da DAG
    dt = DeltaTable(path)
    df = dt.to_pandas(
        partitions=[
            ("ano_particao", "=", str(timestamp_dagrun.year)),
            ("mes_particao", "=", str(timestamp_dagrun.month)),
        ]
    )

    # Filtra apenas a data de escrita mais recente da
    # data de referencia da execução da DAG
    df["timestamp_escrita_bronze_max"] = df.groupby("timestamp_dagrun")[
        "timestamp_escrita_bronze"
    ].transform("max")
    filtro = df["timestamp_escrita_bronze"] == df["timestamp_escrita_bronze_max"]
    df = df.loc[filtro, :].drop(columns="timestamp_escrita_bronze_max").copy()

    # Aplicar tratamentos
    # Especificar tipo da data
    df["sent_date_time"] = pd.to_datetime(df["sent_date_time"])

    df["mail_to"] = df[["to_recipients", "cc_recipients"]].values.tolist()

    # Adicionar uma linha por destinatário
    df = df.explode("mail_to")

    # Selecionar colunas
    df = df.loc[:, ["id", "from", "sent_date_time", "mail_to"]]

    # Manter apenas linhas com registro completo
    df = df.loc[df["mail_to"].notnull()]

    # Adiciona timestamp_dagrun e timestamp_escrita_bronze
    if isinstance(timestamp_dagrun, str):
        timestamp_dagrun = pd.to_datetime(timestamp_dagrun)

    df["timestamp_dagrun"] = timestamp_dagrun
    df["timestamp_dagrun"] = df["timestamp_dagrun"].astype("timestamp[us][pyarrow]")
    df["timestamp_escrita_bronze"] = datetime.now()
    df["timestamp_escrita_bronze"] = df["timestamp_escrita_bronze"].astype(
        "timestamp[us][pyarrow]"
    )
    print("Colunas novas adicionadas ao DataFrame")

    # Adiciona colunas de partições
    # De: 'ano_particao=2023/mes_particao=9'
    # Para: [('ano_particao', '=', '2023'), ('mes_particao', '=', '9')]
    partitions = []
    for part in s3_partitions_key.split("/"):
        split = part.split("=")
        part_name = split[0]
        part_value = split[1]

        df[part_name] = part_value
        partitions.append((part_name, "=", part_value))

    print("Colunas de partições adicionadas ao DataFrame")

    df = df.reset_index(drop=True)

    # Escreve no S3
    dt_path = f"s3://{s3_bucket_save}/{s3_table_key}"

    write_deltalake(
        dt_path,
        df,
        mode="overwrite",
        partition_by=[part[0] for part in partitions],
        partition_filters=partitions,
    )
    print("Append realizado na camada Silver")
