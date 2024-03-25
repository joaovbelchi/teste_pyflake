import json

from requests import HTTPError
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import pandas as pd
from sqlalchemy import create_engine
from deltalake import DeltaTable, write_deltalake

from utils.requests_function import http_session
from utils.secrets_manager import get_secret_value

from airflow.exceptions import AirflowSkipException


def transferencia_rds_para_s3_disparos_wpp(
    data_interval_end: datetime, s3_landing_file_path: str
):
    """Faz a extração da tabela DISPAROS_WPP do banco de dados BLIP
    armazenado no serviço RDS da AWS. A cada execução esta função extrai
    os dois últimos meses de dados armazenados no banco de dados e
    salva na camada landing do data lake.

    Args:
        data_interval_end (datetime): data de referência de execução da DAG.
        s3_landing_file_path (str): path completo de armazenamento
        do arquivo.

    """

    data_extracao = data_interval_end - relativedelta(days=1)

    # Obtem credenciais para acesso ao RDS
    credenciais = json.loads(get_secret_value("prd/rds/database/admin"))

    # Configs de acesso ao RDS
    db_port = "3306"
    db_name = "BLIP"
    db_host = credenciais["host"]
    db_password = credenciais["password"]
    db_username = credenciais["username"]

    # Criando a conexão usando SQLAlchemy
    connection_str = (
        f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    engine = create_engine(connection_str)

    # Executando a consulta
    sql_query = f"""
    SELECT * 
    FROM BLIP.DISPAROS_WPP 
    WHERE date(Data_envio) = '{data_extracao.strftime("%Y-%m-%d")}'
    """
    df = pd.read_sql(sql_query, engine)

    if df.empty:
        raise AirflowSkipException

    df.to_parquet(s3_landing_file_path)


def transferencia_airtable_para_s3_templates_receita(
    s3_landing_file_path: str, data_interval_end: datetime, schema: dict
):
    """Faz a extração da tabela TEMPLATE RECEITA do serviço airtable mantido pela
    equipe de marketing. A cada execução esta função extrai todos
    os dados presentes na tabela em questão do airtable e armazena
    na camada landing do data lake.

    Args:
        s3_landing_file_path (str): Path de armazenamento da table no
        S3.
        data_interval_end (datetime): Data de referencia da execução da DAG.
        schema (dict): Dicionário com o schema da base de dados que está sendo
        extraída.
    """

    # Exemplo de uso da função
    base_id = "appqmK3lCzCeoeZ8O"  # id da base de dados no airtable
    table_name = "tblXwytDk5FxK4TQs"  # id da tabela no airtable
    token = json.loads(get_secret_value("prd/apis/airtable_token"))["token"]

    session = http_session()
    # URL da API do Airtable para obter os registros da tabela
    url = f"https://api.airtable.com/v0/{base_id}/{table_name}"
    headers = {"Authorization": f"Bearer {token}"}
    response = session.get(url, headers=headers)

    dict_dados_extraidos = {"offset": 0}
    df = pd.DataFrame()
    data_inicio_extracao = (data_interval_end - relativedelta(days=45)).strftime(
        "%Y-%m-%d"
    )
    data_fim_extracao = data_interval_end.strftime("%Y-%m-%d")

    while "offset" in dict_dados_extraidos:

        filtro = {
            "filterByFormula": f"AND(DATETIME_FORMAT({{Data}}, 'YYYY-MM-DD') >= '{data_inicio_extracao}', DATETIME_FORMAT({{Data}}, 'YYYY-MM-DD') <= '{data_fim_extracao}')",
            "offset": dict_dados_extraidos["offset"],
        }

        # URL da API do Airtable para obter os registros da tabela
        url = f"https://api.airtable.com/v0/{base_id}/{table_name}"
        headers = {"Authorization": f"Bearer {token}"}
        response = session.get(url, headers=headers, params=filtro)

        # Verifique se a solicitação foi bem-sucedida
        # if response.status_code == 200:
        dict_dados_extraidos = response.json()
        records = dict_dados_extraidos.get("records", [])

        # Itere sobre os registros e extraia os campos
        rows = []
        for record in records:
            fields = record.get("fields", {})
            rows.append(fields)

        df_page = pd.DataFrame(rows)
        df = pd.concat([df, df_page])

    for coluna in schema:
        coluna_experada = schema[coluna]["nomes_origem"][0]
        if coluna_experada not in df.columns:
            df[coluna_experada] = pd.NA

    df.to_parquet(s3_landing_file_path)


def transferencia_bronze_2_silver_disparos_wpp(
    date_interval_end: datetime,
    path_bronze: str,
    path_silver: str,
    schema: dict,
):
    """Processa os dados dos disparos de mensagens do WhatsApp e os armazena
    na camada "silver" do data lake. O processamento inclui dados referentes
    ao mês específico e ao mês anterior a esse mês de referência.

    Args:
        date_interval_end (datetime): data de referência do processamento da DAG.
        path_bronze (str): path para armazenamento da tabela na camada bronze.
        path_silver (str): path para armazenamento da tabela na camada silver.
        schema (dict): schema da base de dados na camada silver.
    """

    # O código abaixo irá carregar o mes atual e o último
    # mes de processamentos da camada bronze do data lake
    data_final_processamento = date_interval_end + relativedelta(
        months=2, day=1, days=-1
    )
    data_inicial_processamento = (date_interval_end - relativedelta(months=1)).replace(
        day=1
    )

    df = DeltaTable(path_bronze).to_pandas(
        filters=[
            ("timestamp_dagrun", ">=", data_inicial_processamento),
            ("timestamp_dagrun", "<=", data_final_processamento),
        ],
    )

    df["data_envio_formatada"] = df["data_envio"].dt.date

    # Filtra apenas a data de escrita mais recente da
    # data de referencia da execução da DAG
    df["timestamp_escrita_bronze_max"] = df.groupby("data_envio_formatada")[
        "timestamp_escrita_bronze"
    ].transform("max")
    filtro = df["timestamp_escrita_bronze"] == df["timestamp_escrita_bronze_max"]
    df = df.loc[filtro, :].drop(columns="timestamp_escrita_bronze_max").copy()

    # Como o processamento faz a extracao dos dados do dia anterior é necessário
    # fazer o filtro abaixo para que o DataFrame que irá sobreescrever a tabela
    # de dados salva no lake possua apenas dados do mês atual e do último mês de
    # competêcia.
    data_final_processamento_ajustada = date_interval_end + relativedelta(
        months=1, day=1, days=-1
    )
    filtro_data = (df["data_envio"] >= data_inicial_processamento) & (
        df["data_envio"] <= data_final_processamento_ajustada
    )
    df = df.loc[filtro_data, :].copy()

    df["ano_particao"] = df["data_envio"].dt.year.astype(int)
    df["mes_particao"] = df["data_envio"].dt.month.astype(int)

    # Remove colunas extras
    df = df.loc[:, schema.keys()].copy()

    # Aplica dtypes
    for col in df.columns:
        df[col] = df[col].astype(f"{schema[col]['data_type']}[pyarrow]")

    print("Schema aplicado no DataFrame")

    df.reset_index(inplace=True, drop=True)

    # O trecho abaixo cria uma lista com as partições da tabela que
    # serão salvas, no seguinte formato:
    # [[('ano_particao', '=', '2024'), ('mes_particao', '=', '2')],
    # [('ano_particao', '=', '2024'), ('mes_particao', '=', '3')]]
    lista_ano_mes = list(
        set([(data.year, data.month) for data in df["data_envio"].dt.date.unique()])
    )

    partition_filters = [
        [("ano_particao", "=", str(ano_mes[0])), ("mes_particao", "=", str(ano_mes[1]))]
        for ano_mes in lista_ano_mes
    ]

    # Faz um loop nas pasrtições que foram criadas no código acima
    # e realiza um overwrite nessas particoes com os novos dados processados
    for partition in partition_filters:

        ano = int(partition[0][2])
        mes = int(partition[1][2])

        df_particao = df.loc[
            (df["ano_particao"] == ano) & (df["mes_particao"] == mes)
        ].copy()
        df_particao = df_particao.reset_index(drop=True)

        write_deltalake(
            table_or_uri=path_silver,
            data=df_particao,
            partition_filters=partition,
            partition_by=["ano_particao", "mes_particao"],
            mode="overwrite",
        )

    # Otimiza arquivos em partições
    # e exclui arquivos antigos
    dt = DeltaTable(path_silver)
    dt.optimize.compact()
    print("Partição compactada")

    dt.vacuum(dry_run=False)
    print("Arquivos antigos apagados")


def transferencia_bronze_2_silver_templates_receita(
    date_interval_end: datetime,
    path_bronze: str,
    path_silver: str,
    schema: dict,
):
    """Processa os dados dos templates de receita e os armazena
    na camada "silver" do data lake. O processamento inclui dados referentes
    ao mês específico e aos dois meses anteriores a esse mês de referência.

    Args:
        date_interval_end (datetime): data de referência do processamento da DAG.
        path_bronze (str): path para armazenamento da tabela na camada bronze.
        path_silver (str): path para armazenamento da tabela na camada silver.
        schema (dict): schema da base de dados na camada silver.
    """

    # O código abaixo irá carregar o mes atual e o último
    # mes de processamentos da camada bronze do data lake
    data_final_processamento = date_interval_end + relativedelta(
        months=3, day=1, days=-1
    )
    data_inicial_processamento = (date_interval_end - relativedelta(months=2)).replace(
        day=1
    )

    print(data_final_processamento)
    print(data_inicial_processamento)

    df = DeltaTable(path_bronze).to_pandas(
        filters=[
            ("timestamp_dagrun", ">=", data_inicial_processamento),
            ("timestamp_dagrun", "<=", data_final_processamento),
        ],
    )

    df["data_formatada"] = df["data"].dt.date

    # Filtra apenas a data de escrita mais recente da
    # data de referencia da execução da DAG
    df["timestamp_escrita_bronze_max"] = df.groupby("data_formatada")[
        "timestamp_escrita_bronze"
    ].transform("max")
    filtro = df["timestamp_escrita_bronze"] == df["timestamp_escrita_bronze_max"]
    df = df.loc[filtro, :].drop(columns="timestamp_escrita_bronze_max").copy()

    # Como o processamento faz a extracao dos dados do dia anterior é necessário
    # fazer o filtro abaixo para que o DataFrame que irá sobreescrever a tabela
    # de dados salva no lake possua apenas dados do mês atual e do último mês de
    # competêcia.
    data_final_processamento_ajustada = date_interval_end + relativedelta(
        months=1, day=1, days=-1
    )
    filtro_data = (df["data"] >= data_inicial_processamento) & (
        df["data"] <= data_final_processamento_ajustada
    )
    df = df.loc[filtro_data, :].copy()

    df["ano_particao"] = df["data"].dt.year.astype(int)
    df["mes_particao"] = df["data"].dt.month.astype(int)

    # Remove colunas extras
    df = df.loc[:, schema.keys()].copy()

    # Aplica dtypes
    for col in df.columns:
        df[col] = df[col].astype(f"{schema[col]['data_type']}[pyarrow]")

    print("Schema aplicado no DataFrame")

    df.reset_index(inplace=True, drop=True)

    # O trecho abaixo cria uma lista com as partições da tabela que
    # serão salvas, no seguinte formato:
    # [[('ano_particao', '=', '2024'), ('mes_particao', '=', '2')],
    # [('ano_particao', '=', '2024'), ('mes_particao', '=', '3')]]
    lista_ano_mes = list(
        set([(data.year, data.month) for data in df["data"].dt.date.unique()])
    )

    partition_filters = [
        [("ano_particao", "=", str(ano_mes[0])), ("mes_particao", "=", str(ano_mes[1]))]
        for ano_mes in lista_ano_mes
    ]

    # Faz um loop nas pasrtições que foram criadas no código acima
    # e realiza um overwrite nessas particoes com os novos dados processados
    for partition in partition_filters:

        ano = int(partition[0][2])
        mes = int(partition[1][2])

        df_particao = df.loc[
            (df["ano_particao"] == ano) & (df["mes_particao"] == mes)
        ].copy()
        df_particao = df_particao.reset_index(drop=True)

        write_deltalake(
            table_or_uri=path_silver,
            data=df_particao,
            partition_filters=partition,
            partition_by=["ano_particao", "mes_particao"],
            mode="overwrite",
        )

    # Otimiza arquivos em partições
    # e exclui arquivos antigos
    dt = DeltaTable(path_silver)
    dt.optimize.compact()
    print("Partição compactada")

    dt.vacuum(dry_run=False)
    print("Arquivos antigos apagados")
