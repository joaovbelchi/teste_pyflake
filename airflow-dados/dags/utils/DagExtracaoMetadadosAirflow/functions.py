import pandas as pd
import boto3
import json
from requests.auth import HTTPBasicAuth
import requests
from airflow.hooks.base import BaseHook
from utils.bucket_names import get_bucket_name
import re
from deltalake import DeltaTable, write_deltalake
from dateutil.relativedelta import relativedelta
from dateutil.parser import isoparse
from deltalake.exceptions import TableNotFoundError
import math
from utils.requests_function import http_session


def leitura_rest_api_airflow(
    s3_bucket: str, s3_key: str, airflow_obj: str, is_event_log: bool = False
) -> pd.DataFrame:
    """Função para carregamento de JSONs retornados pela
    REST API do Airflow

    Args:
        s3_bucket (str): Nome do bucket do Lake
        s3_key (str): Chave do objeto que se deseja carregar
        airflow_obj (str): Nome do objeto do Airflow que está sendo
        consultado

    Returns:
        DataFrame: Dataframe pronto para processamento
    """

    if is_event_log:
        s3 = boto3.client("s3")
        rsp = s3.get_object(Bucket=s3_bucket, Key=s3_key)["Body"].read()
        records = [j for i in json.loads(rsp) for j in i[airflow_obj]]
        df = pd.DataFrame.from_records(records)
    else:
        df = pd.read_json(f"s3://{s3_bucket}/{s3_key}", orient="records")
        df = pd.json_normalize(df[airflow_obj].explode(), max_level=0)

    return df


def extracao_metadados_event_logs(s3_obj_key: str, limit: int, order_by: str):
    """Extração de Event Logs do Airflow. Como o retorno da API é limitado
    a 100 registros por requisição, a obtenção de dados é feita de forma iterativa.
    Os resultados são escritos na camada Landing do Data Lake.

    Args:
        s3_obj_key (str): Nome do objeto em que será gravado o resultado.
        limit (int): Número de resultados a serem extraídos e gravados no Lake.
        order_by (str): Propriedade dos Event Logs que orienta a ordenação
        dos resultados da API.

    Raises:
        Exception: Se o status_code for diferente de 200
    """
    conn = BaseHook.get_connection("airflow_prd")
    hostname = conn.host
    login_name = conn.login
    login_password = conn.password

    results = []
    for i in range(0, limit, 100):
        params = {"order_by": order_by, "offset": i}

        url = f"https://{hostname}/api/v1/eventLogs"
        headers = {"Content-Type": "application/json"}
        res = requests.get(
            url,
            auth=HTTPBasicAuth(login_name, login_password),
            headers=headers,
            params=params,
        )

        if res.status_code != 200:
            raise Exception()

        results.append(json.loads(res.text))

    content = json.dumps(results).encode("utf-8")

    bucket = get_bucket_name("lake-landing")
    s3 = boto3.client("s3")
    s3.put_object(Body=content, Bucket=bucket, Key=s3_obj_key)


def extract_text_between_markers(
    input_string: str, marker_left: str, marker_right: str
) -> str:
    """Usa regex para buscar algum texto delimitado
    por duas sequências de caracteres específicas. Retorna
    a última ocorrência.

    Args:
        input_string (str): Texto onde será realizada a varredura
        marker_left (str): Delimitador à esquerda
        marker_right (str): Delimitador à direita

    Returns:
        str: Última ocorrência do texto entre dois delimitadores
    """

    pattern = f"{marker_left}(.*?){marker_right}"
    matches = re.findall(pattern, input_string)

    return matches[-1]


def parse_task_execution_logs(s3_obj_key: str):
    """Itera sobre todos os objetos atuais do bucket de logs do
    Airflow para obter informações de execuções de tasks.

    Args:
        s3_obj_key (str): chave do S3 em que será gravado o resultado
        na landing
    """

    bucket_logs = get_bucket_name("airflow-logs")

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_logs)

    counter = 0
    records = []

    # Para cada objeto (arquivo de log) retornado em cada página (paginação de API),
    # metadados da execução da task do Airflow são extraídos do path, ou do conteúdo
    # do arquivo.
    for page in pages:

        if not "Contents" in page:
            continue

        for obj in page["Contents"]:

            counter += 1

            if obj["Key"].endswith(".log"):

                response = s3.get_object(Bucket=bucket_logs, Key=obj["Key"])
                content = response["Body"].read().decode("utf-8")

                try:
                    # A linha de log que contém o padrão ´Marking task as´ é selecionada
                    parsed_row = [
                        i for i in content.split("\n") if "Marking task as" in i
                    ][0]

                    # Um registro é criado para cada log. Cada objeto da lista será um valor
                    # em um DataFrame onde há uma linha para cada arquivo de log e uma coluna
                    # para cada metadado extraído
                    record = [
                        extract_text_between_markers(
                            parsed_row, "Marking task as ", "\."
                        ),
                        extract_text_between_markers(obj["Key"], "dag_id=", "/"),
                        extract_text_between_markers(obj["Key"], "/task_id=", "/"),
                        extract_text_between_markers(obj["Key"], "/run_id=", "/"),
                        extract_text_between_markers(parsed_row, "start_date=", ","),
                        extract_text_between_markers(parsed_row, "end_date=", "$"),
                        extract_text_between_markers(obj["Key"], "/attempt=", ".log$"),
                    ]

                    records.append(record)
                # Quando não existe o padrão ´Marking task as´ em nenhuma linha dos logs,
                # valores personalizados de status da execução foram criados, a partir de
                # diferentes padrões
                except IndexError:
                    status = "UNKNOWN"
                    if "Task exited with return code Negsignal.SIGKILL" in content:
                        status = "SIGKILL"
                    elif (
                        "ERROR - Received SIGTERM. Terminating subprocesses" in content
                    ):
                        status = "SIGTERM"
                    elif "Dependencies not met" in content:
                        status = "DEPENDENCIES NOT MET"

                    # Seleciona todas as linhas que começam com um timestamp delimitado por colchetes.
                    # Log padrão dos componentes internos do Airflow
                    linhas_com_timestamp = [
                        re.findall("^\[(.*?)\]", i)
                        for i in content.split("\n")
                        if re.findall("^\[(.*?)\]", i)
                    ]

                    # Seleciona o primeiro e último para criar os valores de 'start_date'
                    # e 'end_date'
                    primeiro_ts = linhas_com_timestamp[0][0]
                    ultimo_ts = linhas_com_timestamp[-1][0]

                    record = [
                        status,
                        extract_text_between_markers(obj["Key"], "dag_id=", "/"),
                        extract_text_between_markers(obj["Key"], "/task_id=", "/"),
                        extract_text_between_markers(obj["Key"], "/run_id=", "/"),
                        primeiro_ts,
                        ultimo_ts,
                        extract_text_between_markers(obj["Key"], "/attempt=", ".log$"),
                    ]

            if page["Contents"][-1] == obj:
                print(f"{counter} objetos processados")

    # Grava resultados na landing
    df = pd.DataFrame(
        records,
        columns=[
            "task_run_status",
            "dag_id",
            "task_id",
            "run_id",
            "start_date",
            "end_date",
            "attempt",
        ],
    )

    bucket_landing = get_bucket_name("lake-landing")

    if not df.empty:
        df.to_parquet(f"s3://{bucket_landing}/{s3_obj_key}")


def escrita_gold(ts_dagrun: str, schema: dict, path_bronze: str, path_gold: str):
    """Realiza a escrita na camada Gold dos metadados extraídos do
    Airflow. Realiza uma operação de UPSERT nos dados.

    Args:
        ts_dagrun (str): Timestamp lógico de execução do pipeline
        schema (dict): Schema das tabelas bronze e Gold
        path_bronze (str): Path da tabela origem na camada Bronze
        path_gold (str): Path da tabela destino na camada Gold
    """

    def aplica_dtypes(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
        """Aplica os dtypes definidos no Schema no DataFrame

        Args:
            df (pd.DataFrame): DataFrame antes da aplicação do schema
            schema (dict): Schema utilizado nas camadas bronze e gold

        Returns:
            pd.DataFrame: DataFrame com schema aplicado
        """
        for col in df.columns:
            df[col] = df[col].astype(f"{schema[col]['data_type']}[pyarrow]")
        return df

    ts_dagrun_parsed = isoparse(ts_dagrun)
    ts_list = [ts_dagrun_parsed, ts_dagrun_parsed - relativedelta(months=1)]
    partitions = [
        [("ano_particao", "=", str(ts.year)), ("mes_particao", "=", str(ts.month))]
        for ts in ts_list
    ]
    columns = list(schema.keys())

    df_new = (
        DeltaTable(path_bronze)
        .to_pandas(filters=partitions, columns=columns)
        .drop_duplicates(subset=columns)
    )

    df_new = aplica_dtypes(df_new, schema)

    try:
        df_current = DeltaTable(path_gold).to_pandas()
        df_current = aplica_dtypes(df_current, schema)
    except TableNotFoundError:
        df_current = pd.DataFrame()

    df_final = (
        pd.concat([df_current, df_new])
        .drop_duplicates(subset=columns)
        .reset_index(drop=True)
    )

    write_deltalake(
        path_gold,
        df_final,
        mode="overwrite",
    )
    print("Overwrite realizado na Gold")

    # Vacuum
    dt = DeltaTable(path_gold)
    dt.vacuum(dry_run=False)
    print("Arquivos antigos apagados")


def extracao_metadados_generico(
    s3_obj_key: str, endpoint: str, end_date_gte: str = None
):
    """
    Função genérica de extração de dados do Airflow. Como o retorno das APIs são limitados
    a 100 registros por requisição, a obtenção de dados é feita de forma iterativa.
    Os resultados são escritos na camada Landing do Data Lake.

    Args:
        s3_obj_key (str): Nome do objeto em que será gravado o resultado.
        endpoint (str): Endpoint da API desejada.
        end_date_gte (str): 'end_date' mínima a partir da qual serão extraídas
        Dag Runs.

    Raises:
        Exception: Se o status_code for diferente de 200
    """
    conn = BaseHook.get_connection("airflow_prd")
    hostname = conn.host
    login_name = conn.login
    login_password = conn.password

    custom_params = {"end_date_gte": end_date_gte}

    results = []
    offset_current = 0
    session = http_session()
    while True:
        params = {
            "offset": offset_current,
            **{i: j for i, j in custom_params.items() if j},
        }

        url = f"https://{hostname}/{endpoint}"
        headers = {"Content-Type": "application/json"}
        res = session.get(
            url,
            auth=HTTPBasicAuth(login_name, login_password),
            headers=headers,
            params=params,
        )

        res_dict = res.json()
        results.append(res_dict)

        offset_max = math.floor(res_dict["total_entries"] / 100) * 100
        if offset_current == offset_max:
            break

        offset_current += 100

    content = json.dumps(results).encode("utf-8")

    bucket = get_bucket_name("lake-landing")
    s3 = boto3.client("s3")
    s3.put_object(Body=content, Bucket=bucket, Key=s3_obj_key)
