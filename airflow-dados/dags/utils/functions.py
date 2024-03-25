from datetime import datetime
import json
import boto3
import pandas as pd
import pytz
from utils.msteams import msteams_qualidade_dados
from deltalake import write_deltalake, DeltaTable
from typing import Callable, Union
import requests
from requests.auth import HTTPBasicAuth
import uuid
from utils.secrets_manager import get_secret_value
from urllib.request import urlopen
from dateutil.parser import isoparse
from utils.OneGraph.drive import OneGraphDrive
from typing import List, Literal
from io import BytesIO


def download_arquivo_webhook_btg(
    bucket: str, response_object_key: str, download_object_key: str
):
    """Lê a response de um webhook que foi gravada no S3, coleta
    a URL de download do arquivo solcitado no webhook e realiza
    o download e upload desse arquivo para o S3

    Args:
        bucket (str): Nome do bucket do S3
        response_object_key (str): Chave do objeto da response do webhook
        download_object_key (str): chave do objeto onde será gravado
        o conteúdo do arquivo solcitado via webhook
    """

    s3 = boto3.resource("s3")
    content_object = s3.Object(bucket, response_object_key)
    file_content = content_object.get()["Body"].read().decode("utf-8")
    json_content = json.loads(file_content)

    url = json_content.get("url")
    if not url:
        url = json_content["response"]["url"]

    bytes_ = urlopen(url).read()

    s3 = boto3.client("s3")
    s3.put_object(Body=bytes_, Bucket=bucket, Key=download_object_key)


def verifica_qualidade_landing(
    s3_bucket: str,
    s3_table_key: str,
    s3_partitions_key: str,
    s3_filename: str,
    schema: dict,
    webhook: str,
    funcao_leitura_landing: Callable,
    kwargs_funcao_leitura_landing: dict,
):
    """Realiza validações de qualidade de dados em um arquivo
    na landing. Se houverem problemas de qualidade, um log de
    qualidade no formato JSON é enviado para um diretório no S3.
    Se o problema de qualidade for crítico, uma exceção é levantada.

    Args:
        s3_bucket (str): Nome do bucket da camada landing.
        s3_table_key (str): Prefixo da chave completa do objeto.
        Vai até imediatamente antes do primeiro diretório de partição.
        s3_partitions_key (str): Parte da chave do objeto que contém as
        partições.
        s3_filename (str): Nome do arquivo na Landing com extensão.
        schema (dict): Schema da tabela, no formato definido no
        arquivo 'utils/leitura_landing.py'.
        webhook (str): Webhook do canal de alertas de qualidade
        de dados no Teams.
        funcao_leitura_landing (Callable): Função customizada que realiza
        a leitura de um arquivo na landing e retorna um DataFrame pandas.
        Necessário pois cada arquivo na landing possui suas particularidades
        de leitura.
        kwargs_funcao_leitura_landing (dict): Parâmetros chave-valor para
        passar para a função customizada de leitura.

    Raises:
        Exception: Levanta exceção se algum problema de qualidade
        crítico é encontrado.
    """
    # Leitura objeto
    df = funcao_leitura_landing(**kwargs_funcao_leitura_landing)

    # Corrige problemas conhecidos
    df = corrige_problemas_conhecidos(df, schema)

    # Verifica colunas duplicadas
    colunas_recebidas = list(df.columns)
    colunas_duplicadas = [
        i for i in set(colunas_recebidas) if colunas_recebidas.count(i) > 1
    ]

    # Verifica colunas extras, faltantes e com múltiplas correspondências
    mapa_dtypes = {}
    colunas_mult_corresp = []
    colunas_faltantes = []
    colunas_df = set(df.columns)

    for col_bronze, schema_landing in schema.items():
        cols_landing = set(schema_landing["nomes_origem"])
        intersecao = colunas_df & cols_landing
        mapa_dtypes = {
            **mapa_dtypes,
            **{col: schema_landing["data_type"] for col in intersecao},
        }

        if len(intersecao) >= 2:
            colunas_mult_corresp.append(col_bronze)
            colunas_recebidas -= intersecao

        if not len(intersecao):
            colunas_faltantes.append(col_bronze)

        colunas_df -= intersecao

    colunas_extras = list(colunas_df)

    # Verifica colunas fora do tipo
    colunas_fora_do_tipo = []
    for col, dtype in mapa_dtypes.items():
        try:
            df[col] = df[col].astype(f"{dtype}[pyarrow]")
        except Exception:
            colunas_fora_do_tipo.append(col)

    # Coleta metadados do objeto do S3
    s3_object_key = f"{s3_table_key}/{s3_partitions_key}/{s3_filename}"
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
            "colunas_recebidas_landing": colunas_recebidas,
            "colunas_com_multiplas_correspondencias_bronze": colunas_mult_corresp
            or None,
            "colunas_duplicadas_landing": colunas_duplicadas or None,
            "colunas_faltantes_bronze": colunas_faltantes or None,
            "colunas_extras_landing": colunas_extras or None,
            "colunas_fora_do_tipo_landing": colunas_fora_do_tipo or None,
            "schema": schema,
            "chave_objeto_s3": s3_object_key,
            "processado_em": last_modified,
            "criado_em": datetime.now(tz=pytz.utc).isoformat(),
        },
        ensure_ascii=False,
    )

    # Cria nome do objeto do log no S3
    filename_sem_extensao = s3_filename.split(".")[0]
    qualidade_object_key = (
        f"qualidade_dados/schema_landing/{s3_partitions_key}/"
        f"{filename_sem_extensao}.json"
    )

    # Cria condição de problema sério de qualidade
    cond = (
        colunas_faltantes
        or colunas_fora_do_tipo
        or colunas_mult_corresp
        or colunas_duplicadas
    )

    # Escreve log de qualidade no S3 e alerta no Teams se há problemas
    if cond or colunas_extras:
        s3.put_object(
            Body=log_qualidade_json,
            Bucket=s3_bucket,
            Key=qualidade_object_key,
        )
        msteams_qualidade_dados(log_qualidade_json, webhook)

    # Levanta exceção se há problemas que inviabilizam o restante da execução
    if cond:
        raise Exception(
            f"Problemas de qualidade de dados encontrados. Log: {log_qualidade_json}"
        )


def transferencia_landing_para_bronze(
    s3_bucket_bronze: str,
    s3_table_key: str,
    s3_partitions_key: str,
    schema: dict,
    data_interval_end: Union[datetime, str],
    funcao_leitura_landing: Callable,
    kwargs_funcao_leitura_landing: dict,
):
    """Transforma um arquivo da camada Landing para a Bronze.
    O schema desejado é aplicado e a escrita é realizada via
    'append' do Deltalake. Além disso, colunas com partições,
    timestamp da Dagrun e timestamp do processamento são incluídas.
    Após a escrita, são realizadas operações de compactação de partição
    e vacuuming da tabela utlizando comandos Deltalake.


    Args:
        s3_bucket_bronze (str): Nome do bucket da camada Bronze.
        s3_table_key (str): Prefixo da chave completa do objeto na Landing.
        s3_partitions_key (str): Parte da chave do objeto na Landing
        que contém as partições.
        schema (dict): Schema da tabela, no formato definido no
        arquivo 'utils/leitura_landing.py'.
        data_interval_end (Union[datetime, str]): Propriedade 'data_interval_end' da
        Dagrun do Airflow.
        funcao_leitura_landing (Callable): Função customizada que realiza
        a leitura de um arquivo na landing e retorna um DataFrame pandas.
        Necessário pois cada arquivo na landing possui suas particularidades
        de leitura.
        kwargs_funcao_leitura_landing (dict): Parâmetros chave-valor para
        passar para a função customizada de leitura.
    """

    # Leitura objeto
    df = funcao_leitura_landing(**kwargs_funcao_leitura_landing)
    print("Leitura da landing finalizada")

    # Corrige problemas conhecidos
    df = corrige_problemas_conhecidos(df, schema)

    # Renomeia colunas
    colunas_df = list(df.columns)
    rename_dict = {
        col_landing: col_bronze
        for col_bronze, schema_landing in schema.items()
        for col_landing in schema_landing["nomes_origem"]
        if col_landing in colunas_df
    }
    df = df.rename(columns=rename_dict)

    # Remove colunas extras
    df = df[schema.keys()]

    # Aplica dtypes
    for col in df.columns:
        df[col] = df[col].astype(f"{schema[col]['data_type']}[pyarrow]")
    print("Schema aplicado no DataFrame")

    # Adiciona timestamp_dagrun e timestamp_escrita_bronze
    if isinstance(data_interval_end, str):
        data_interval_end = pd.to_datetime(data_interval_end)

    df["timestamp_dagrun"] = data_interval_end
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

    # Escreve no S3
    dt_path = f"s3://{s3_bucket_bronze}/{s3_table_key}"

    write_deltalake(
        dt_path,
        df,
        mode="append",
        partition_by=[part[0] for part in partitions],
    )
    print("Append realizado na camada Bronze")

    # Compactação da partição
    dt = DeltaTable(dt_path)
    dt.optimize.compact(partition_filters=partitions)
    print("Partição compactada")

    # Vacuum
    dt.vacuum(dry_run=False)
    print("Arquivos antigos apagados")


def corrige_problemas_conhecidos(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """Função para corrigir problemas conhecidos, para os quais já
    existe um tratamento definido. Essa função deve ser executada antes de
    aplicar o schema na base.

    Args:
        df (pd.DataFrame): DataFrame com a base já com as colunas renomeadas
        mas antes de aplicar o schema.
        schema (dict): Schema da tabela, no formato definido no
        arquivo 'utils/<DAG>/leitura_landing.py'.

    Returns:
        pd.DataFrame: DataFrame com problemas conhecidos resolvidos e
        pronto para aplicação do schema.
    """

    # Datas estranhas. Exemplo: '0001-12-30 12:00:00 BC'
    timestamp_cols = [
        (col_land, schema[col_bronze]["data_type"])
        for col_bronze in schema.keys()
        for col_land in schema[col_bronze]["nomes_origem"]
        if "timestamp" in schema[col_bronze]["data_type"] and col_land in df.columns
    ]
    for col, col_dtype in timestamp_cols:
        df[col] = df[col].astype("string")
        df.loc[
            df[col].str.contains("BC")
            | df[col].str.contains("0001")
            | df[col].str.contains("9999"),
            col,
        ] = pd.NA
        df[col] = pd.to_datetime(df[col], format="mixed").astype(
            f"{col_dtype}[pyarrow]"
        )

    return df


def leitura_excel_s3(
    s3_bucket_landing: str, s3_object_key: str, **kwargs
) -> pd.DataFrame:
    """Função para carregamento de arquivos exportados do sistema
    Access do BTG para DataFrame

    Args:
        s3_bucket_landing (str): Nome do bucket da camada Landing do Lake
        s3_object_key (str): Chave do objeto que se deseja carregar

    Returns:
        DataFrame: Dataframe pronto para processamento
    """
    return pd.read_excel(
        f"s3://{s3_bucket_landing}/{s3_object_key}", dtype_backend="pyarrow", **kwargs
    )


def leitura_csv(s3_bucket_landing: str, s3_object_key: str) -> pd.DataFrame:
    """Função para carregamento de arquivos csv

    Args:
        s3_bucket_landing (str): Nome do bucket da camada Landing do Lake
        s3_object_key (str): Chave do objeto que se deseja carregar

    Returns:
        DataFrame: Dataframe pronto para processamento
    """
    return pd.read_csv(
        f"s3://{s3_bucket_landing}/{s3_object_key}", dtype_backend="pyarrow"
    )


def trigger_webhook_btg(endpoint: str):
    """Função resposável por se autenticar na API  e acionar
    um webhook cadastrado no Portal do Desenvolvedor do BTG

    Args:
        endpoint (str): URL completa da chamada de API que aciona
        o webhook
    """
    # Coleta credenciais
    secret_dict = json.loads(get_secret_value("prd/apis/btgpactual"))
    client_id = secret_dict["client_id"]
    client_secret = secret_dict["client_secret"]

    # Fluxo de autenticação
    link = (
        "https://api.btgpactual.com/iaas-auth/api/v1/authorization/oauth2/accesstoken"
    )
    header = {"x-id-partner-request": client_secret}
    post_data = {"grant_type": "client_credentials"}
    req = requests.post(
        link,
        auth=HTTPBasicAuth(client_id, client_secret),
        data=post_data,
        headers=header,
    )

    # Token de acesso à API do BTG
    token = req.headers["access_token"]

    # UUID gerado randômicamente, usado para identificar cada request efetuado.
    # Sempre utilizar um diferente para cada requisição, mesmo que sejam os mesmos parâmetros
    uuid_var = uuid.uuid4().hex

    # Requisição
    header = {"x-id-partner-request": uuid_var, "access_token": token}

    req = requests.get(endpoint, headers=header)

    print("Response: ", req)


def verifica_objeto_s3(
    bucket: str, object_key: str, min_last_modified: datetime
) -> bool:
    """Verifica se um objeto foi atualizado recentemente
    ou não no S3, dado um datetime mínimo

    Args:
        bucket (str): Nome do Bucket do S3
        object_key (str): Chave do objeto do S3
        min_last_modified (datetime): Critério que define
        se o objeto foi atualizado recentemente ou não.

    Returns:
        bool: True se o objeto foi atualizado recentemente. False,
        caso contrário
    """
    client = boto3.client("s3")
    results = client.list_objects(Bucket=bucket, Prefix=object_key)
    contents = results.get("Contents")

    if not contents:
        print("Nenhum arquivo encontrado")
        return False

    if contents[0]["LastModified"] < min_last_modified:
        print("Arquivo desatualizado")
        return False

    return True


def email_list(destinatarios: list) -> list:
    """Função para gerar lista de destinatários no formato necessário para fazer o post na Graph API da Microsoft.

    Args:
        destinatarios (str): string contendo os destinatários. Os e-mails devem estar separados por ";"

    Returns:
        list: lista contendo os destinatários que receberão o e-mail
    """
    to = []
    # Iterar sobre o total de e-mails para criar um dicionário para cada destinatário
    # e, posteriormente, adiciona-lo na lista
    for email in destinatarios:
        result = {"emailAddress": {"address": email}}
        to.append(result)

    return to


def parse_ts_dag_run(
    dag_run_id: str,
    data_interval_end: str,
    external_trigger: bool,
    return_datetime: bool = False,
) -> str:
    """Retorna o timestamp escrito na dag_run_id.

    Args:
        dag_run_id (str): Propriedade da Dag Run.
        data_interval_end (str): Propriedade da Dag Run.
        external_trigger (bool): Determina se a Dag Run foi agendada
        ou se o trigger foi externo.
        return_datetime (bool): Determina se o objeto retornado
        será um datetime ou um string.

    Returns:
        str: Data Interval End no caso de runs agendadas. Execution
        Date no caso de Runs manuais
    """
    if external_trigger:
        parsed = isoparse(dag_run_id.split("__")[-1])
    else:
        parsed = data_interval_end

    if return_datetime:
        return parsed

    return parsed.strftime("%Y%m%dT%H%M%S")


def s3_to_onedrive(
    s3_bucket: str,
    s3_obj_key: str,
    onedrive_drive_id: str,
    onedrive_path: List[str],
):
    """Realiza o download de um objeto do S3 para a memória e realiza
    o upload para um drive do Onedrive

    Args:
        s3_bucket (str): Nome do bucket do S3
        s3_obj_key (str): Nome da chave do objeto no S3
        onedrive_drive_id (str): ID do drive do Onedrive
        onedrive_path (List[str]): Path do Onedrive (com estrutura de pastas em formato de lista)
    """
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )

    if onedrive_path[-1].endswith(".csv") and s3_obj_key.endswith(".parquet"):
        df = pd.read_parquet(f"s3://{s3_bucket}/{s3_obj_key}")
        buffer = BytesIO()
        df.to_csv(buffer, index=False, decimal=",", sep=";")
        object_content = buffer.getvalue()

    else:
        s3_client = boto3.client("s3")
        s3_response_object = s3_client.get_object(Bucket=s3_bucket, Key=s3_obj_key)
        object_content = s3_response_object["Body"].read()

    drive = OneGraphDrive(drive_id=onedrive_drive_id, **credentials)
    drive.upload_file(
        onedrive_path,
        object_content,
    )


def leitura_bronze(s3_path: str, timestamp_dagrun: datetime) -> pd.DataFrame:
    """Realiza a leitura de versão mais atual de determinado
    timestamp_dagrun de tabela na camada Bronze do Data Lake.

    Args:
        s3_path (str): Path completo da tabela no S3
        timestamp_dagrun (datetime): Timestamp lógico de execução do pipeline

    Returns:
        pd.DataFrame: DataFrame contendo resultado
    """
    dt = DeltaTable(s3_path)
    df = dt.to_pandas(
        partitions=[
            ("ano_particao", "=", str(timestamp_dagrun.year)),
            ("mes_particao", "=", str(timestamp_dagrun.month)),
        ]
    )
    df = df.loc[df["timestamp_dagrun"] == timestamp_dagrun].copy()
    df = df.loc[
        df["timestamp_escrita_bronze"] == df["timestamp_escrita_bronze"].max()
    ].copy()

    return df


def transferencia_onedrive_para_s3(
    drive_id: str, onedrive_path: List[str], s3_bucket: str, s3_object_key: str
) -> None:
    """Faz o download de um arquivo localizado no Onedrive
    e realiza o upload do conteúdo para um objeto em um
    bucket do S3 

    Args:
        drive_id (str): ID do drive do Onedrive onde reside o
        arquivo
        onedrive_path (List[str]): Lista contendo nomes de pastas e nome
        do arquivo no último item, formando o path. Exemplo:
        ['pasta_1', 'pasta_2', 'arquivo.csv']
        s3_bucket (str): Nome do bucket do S3 para onde se deseja
        realizar o upload
        s3_object_key (str): Nome desejado para o objeto no S3 (não
        incluir nome do bucket aqui. É da primeira pasta pra frente)
    """
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)
    bytes = drive.download_file(path=onedrive_path).content
    print("Download realizado de arquivo realizado.")

    client = boto3.client("s3")
    client.put_object(Body=bytes, Bucket=s3_bucket, Key=s3_object_key)
    print("Upload de arquivo realizado.")


def trigger_glue(glue_job: str, arguments: dict, ti):
    """Esta função realiza o trigger de um job no AWS Glue.

    Args:
        glue_job (str): Nome do Job no Glue.
        arguments (dict): Conjunto de argumentos que serão passados para o Job do Glue
        ti: Task Instance

    """
    client = boto3.client("glue", region_name="us-east-1")

    run_info = client.start_job_run(JobName=glue_job, Arguments=arguments)

    run_id = run_info["JobRunId"]

    key = glue_job + "_run_id"
    ti.xcom_push(key=key, value=run_id)
