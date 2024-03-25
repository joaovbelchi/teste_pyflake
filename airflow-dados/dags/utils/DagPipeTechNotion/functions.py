import pandas as pd
from utils.bucket_names import get_bucket_name
from utils.requests_function import http_session
import boto3
import json
from deltalake import DeltaTable
from dateutil.parser import isoparse
from utils.secrets_manager import get_secret_value


def leitura_bronze(data_interval_end: str) -> pd.DataFrame:
    """Realiza a leitura da camada bronze da base de projetos tech do Notion

    Args:
        data_interval_end (str): data da execução da DAG

    Returns:
        pd.DataFrame: Dataframe com os dados da base de projetos tech do Notion da camada Bronze
    """
    bucket_lake_bronze = get_bucket_name("lake-bronze")
    dt_path = f"s3://{bucket_lake_bronze}/notion/api_notion/tecnologia/projetos/"

    ano_particao = str(isoparse(data_interval_end).year)
    mes_particao = str(isoparse(data_interval_end).month)

    df_projetos = DeltaTable(dt_path).to_pandas(
        partitions=[
            ("ano_particao", "=", ano_particao),
            ("mes_particao", "=", mes_particao),
        ]
    )

    df_projetos_atual = df_projetos[
        (df_projetos["timestamp_dagrun"] == data_interval_end)
        & (
            df_projetos["timestamp_escrita_bronze"]
            == df_projetos["timestamp_escrita_bronze"].max()
        )
    ]

    return df_projetos_atual


def download_base_projetos_tech_notion(
    bucket: str, download_object_key: str
) -> pd.DataFrame:
    """Função para fazer a requisição de uma base de dados do Notion

    Args:
        bucket (str): Nome do bucket onde a base de dados será salva
        download_object_key (str): Chave do objeto no S3

    Returns:
        pd.DataFrame: Dataframe com os dados da base de dados do Notion
    """

    credentials = json.loads(get_secret_value("prd/apis/notion"))
    notion_key = credentials["token"]

    database_id = "0a0ceba0af2e40a08eb773bd7f0faa0c"
    headers = {
        "Authorization": f"Bearer {notion_key}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

    url = f"https://api.notion.com/v1/databases/{database_id}/query"

    session = http_session()
    response = session.request("POST", url, headers=headers)
    data = response.json()

    bytes_ = json.dumps(data, ensure_ascii=False)
    s3 = boto3.client("s3")
    s3.put_object(Body=bytes_, Bucket=bucket, Key=download_object_key)


def leitura_json_to_dataframe(
    s3_bucket_landing: str, s3_object_key: str
) -> pd.DataFrame:
    """Função para ler um arquivo json do S3 e transformar em um dataframe

    Args:
        s3_bucket_landing (str): path da camada landing
        s3_object_key (str): chave do objeto no S3

    Returns:
        pd.DataFrame: Dataframe com os dados do arquivo json
    """
    s3 = boto3.resource("s3")
    content_object = s3.Object(s3_bucket_landing, s3_object_key)
    file_content = content_object.get()["Body"].read().decode("utf-8")
    json_content = json.loads(file_content)

    df = pd.DataFrame([item["properties"] for item in json_content["results"]])

    columns = df.columns

    # converte as colunas para json para depois normalizar
    for col in columns:
        df[col] = df[col].apply(lambda x: json.dumps(x))
    return df


def str_para_dict(s):
    return json.loads(s)


def dataframe_por_coluna(df: pd.DataFrame) -> dict:
    """Função para criar um dicionário de dataframes a partir de um dataframe

    Args:
        df (pd.DataFrame): Dataframe com os dados da base de dados do Notion

    Returns:
        dict: Dicionário de dataframes, em que cada chave é uma coluna do dataframe original
    """
    colunas = df.columns.drop(["timestamp_escrita_bronze", "timestamp_dagrun"])
    dataframes = {}
    for coluna in colunas:
        df[coluna] = df[coluna].apply(str_para_dict)
        nome_do_dataframe = f"df_{coluna}"
        dataframes[nome_do_dataframe] = pd.json_normalize(df[coluna])

    return dataframes


def tratando_df_area_subarea_squad(dataframe_dict: dict) -> dict:
    """Função para tratar o dataframe de Area_subarea_Squad, utilizando o metodo pd.json_normalize

    Args:
        dataframe_dict (dict): Dicionário de dataframes, em que cada chave é uma coluna do dataframe original

    Returns:
        dict: Dicionário de dataframes modificado na chave df_Area_e_subarea_ou_Squad
    """
    df_area_e_subarea_ou_squad_normalizado = pd.json_normalize(
        dataframe_dict["df_area_e_subarea_ou_squad"]["multi_select"]
    )
    df_area_e_subarea_ou_squad_final = pd.json_normalize(
        df_area_e_subarea_ou_squad_normalizado[0]
    )

    dataframe_dict["df_area_e_subarea_ou_squad"] = df_area_e_subarea_ou_squad_final

    return dataframe_dict


def tratando_df_projeto(dataframe_dict: dict) -> dict:
    """Função para tratar o dataframe de Projeto, utilizando o metodo pd.json_normalize

    Args:
        dataframe_dict (dict): Dicionário de dataframes, em que cada chave é uma coluna do dataframe original

    Returns:
        dict: Dicionário de dataframes modificado na chave df_Projeto
    """
    df_projeto_normalizado = pd.json_normalize(dataframe_dict["df_projeto"]["title"])
    df_projeto_final = pd.json_normalize(df_projeto_normalizado[0])

    dataframe_dict["df_projeto"] = df_projeto_final

    return dataframe_dict


def extrair_nomes_responsaveis(responsaveis: list) -> list:
    """Função para extrair os nomes dos responsaveis de uma lista de responsaveis

    Args:
        responsaveis (list): Lista de responsaveis

    Returns:
        list: Lista de nomes dos responsaveis
    """
    nomes_responsaveis = [resp["name"] for resp in responsaveis if "name" in resp]

    return nomes_responsaveis if nomes_responsaveis else None


def tratando_df_responsaveis(dataframe_dict: dict) -> dict:
    """Função para tratar o dataframe de Responsaveis, aplicando a funcao extrair_nomes_responsaveis

    Args:
        dataframe_dict (dict): Dicionário de dataframes, em que cada chave é uma coluna do dataframe original

    Returns:
        dict: Dicionário de dataframes modificado na chave df_Responsaveis
    """
    df_responsaveis = dataframe_dict["df_responsaveis"]
    df_responsaveis["responsaveis"] = df_responsaveis["people"].apply(
        extrair_nomes_responsaveis
    )

    dataframe_dict["df_responsaveis"] = df_responsaveis

    return dataframe_dict


def extrair_sprints(sprint_col: list) -> list:
    """Função para extrair os nomes das sprints de uma lista de sprints

    Args:
        sprint_col (list): Lista de sprints

    Returns:
        list: Lista de nomes das sprints
    """
    sprints = [resp["name"] for resp in sprint_col]

    return sprints if sprints else None


def tratando_df_sprints(dataframe_dict: dict) -> dict:
    """Função para tratar o dataframe de Sprints, aplicando a funcao extrair_sprints

    Args:
        dataframe_dict (dict): Dicionário de dataframes, em que cada chave é uma coluna do dataframe original

    Returns:
        dict: Dicionário de dataframes modificado na chave df_Sprints_priorizado
    """
    df_sprints_priorizado = dataframe_dict["df_sprints_priorizado"]
    df_sprints_priorizado["sprints"] = df_sprints_priorizado["multi_select"].apply(
        extrair_sprints
    )

    dataframe_dict["df_sprints_priorizado"] = df_sprints_priorizado

    return dataframe_dict


def rename_columns(dataframe_dict: dict) -> dict:
    """Função para renomear as colunas dos dataframes

    Args:
        dataframe_dict (dict): Dicionário de dataframes, em que cada chave é uma coluna do dataframe original

    Returns:
        dict: Dicionário de dataframes com as colunas renomeadas
    """
    # Se uma coluna nova for adicionada ou uma coluna for retirada o codigo nao quebra
    for key in dataframe_dict.keys():
        if key == "df_deadline":
            dataframe_dict[key] = dataframe_dict[key].rename(
                columns={"date.start": "deadline"}
            )

        elif key == "df_ultima_edicao":
            dataframe_dict[key] = dataframe_dict[key].rename(
                columns={"last_edited_time": "ultima_edicao"}
            )

        elif key == "df_criado_em":
            dataframe_dict[key] = dataframe_dict[key].rename(
                columns={"created_time": "criado_em"}
            )

        elif key == "df_status":
            dataframe_dict[key] = dataframe_dict[key].rename(
                columns={"status.name": "status"}
            )

        elif key == "df_data_finalizado":
            dataframe_dict[key] = dataframe_dict[key].rename(
                columns={"date.start": "data_finalizado"}
            )

        elif key == "df_previsao":
            dataframe_dict[key] = dataframe_dict[key].rename(
                columns={"date.start": "inicio_previsao", "date.end": "final_previsao"}
            )

        elif key == "df_subarea":
            dataframe_dict[key] = dataframe_dict[key].rename(
                columns={"formula.string": "subarea"}
            )

        elif key == "df_area":
            dataframe_dict[key] = dataframe_dict[key].rename(
                columns={"formula.string": "area"}
            )

        elif key == "df_area_e_subarea_ou_squad":
            dataframe_dict[key] = dataframe_dict[key].rename(columns={"name": "squad"})

        elif key == "df_projeto":
            dataframe_dict[key] = dataframe_dict[key].rename(
                columns={"text.content": "projeto"}
            )

    return dataframe_dict


def merge_all_dataframes(dataframe_dict: dict) -> pd.DataFrame:
    """Função para mesclar todos os dataframes em um dataframe final

    Args:
        dataframe_dict (dict): Dicionário de dataframes, em que cada chave é uma coluna do dataframe original

    Returns:
        pd.DataFrame: Dataframe final com todos os dados mesclados
    """
    df_deadline = dataframe_dict["df_deadline"]["deadline"]
    df_ultima_edicao = dataframe_dict["df_ultima_edicao"]["ultima_edicao"]
    df_criado_em = dataframe_dict["df_criado_em"]["criado_em"]
    df_status = dataframe_dict["df_status"]["status"]
    df_data_finalizado = dataframe_dict["df_data_finalizado"]["data_finalizado"]
    df_previsao = dataframe_dict["df_previsao"][["inicio_previsao", "final_previsao"]]
    df_subarea = dataframe_dict["df_subarea"]["subarea"]
    df_area = dataframe_dict["df_area"]["area"]
    df_area_e_subarea_ou_squad_final = dataframe_dict["df_area_e_subarea_ou_squad"][
        "squad"
    ]
    df_projeto_final = dataframe_dict["df_projeto"]["projeto"]
    df_responsaveis = dataframe_dict["df_responsaveis"]["responsaveis"]
    df_sprints_priorizado = dataframe_dict["df_sprints_priorizado"]["sprints"]

    dataframes = [
        df_deadline,
        df_ultima_edicao,
        df_criado_em,
        df_status,
        df_data_finalizado,
        df_previsao,
        df_subarea,
        df_area,
        df_responsaveis,
        df_area_e_subarea_ou_squad_final,
        df_projeto_final,
        df_sprints_priorizado,
    ]

    # DataFrame inicial
    result = dataframes[0]

    # Mesclar todos os DataFrames na lista
    for df in dataframes[1:]:
        result = pd.merge(result, df, left_index=True, right_index=True, how="inner")

    data_cols = [
        "deadline",
        "ultima_edicao",
        "criado_em",
        "data_finalizado",
        "inicio_previsao",
        "final_previsao",
    ]

    # tratamento para as colunas de data
    # as colunas ultima_edicao e criado_em estão no formato de data e hora, então é necessário separar a data da hora
    for coluna in data_cols:
        if coluna == "ultima_edicao" or coluna == "criado_em":
            result[coluna] = (
                result[coluna].str.split("T").str[0]
            )  # Remove a parte de tempo
            result[coluna] = pd.to_datetime(result[coluna], format="%Y-%m-%d")
        else:
            result[coluna] = pd.to_datetime(result[coluna], format="%Y-%m-%d")
            result[coluna] = result[coluna].dt.date

    return result


def pipe_projetos_notion(data_interval_end: str) -> None:
    """Função para executar o pipe de projetos do Notion

    Args:
        data_interval_end (str): data da execução da DAG
    """

    base_projetos_bronze = leitura_bronze(data_interval_end)
    dataframe_dict = dataframe_por_coluna(base_projetos_bronze)
    dataframe_dict = tratando_df_area_subarea_squad(dataframe_dict)
    dataframe_dict = tratando_df_projeto(dataframe_dict)
    dataframe_dict = tratando_df_responsaveis(dataframe_dict)
    dataframe_dict = tratando_df_sprints(dataframe_dict)
    dataframe_dict = rename_columns(dataframe_dict)
    result = merge_all_dataframes(dataframe_dict)

    # colunas com lista de valores
    # utilizamos o explode para quebrar as listas em linhas, aumenta a quantidade de linhas mas muda o valor da coluna
    explode_responsaveis = result.explode("responsaveis")
    explode_sprints = explode_responsaveis.explode("sprints")

    result_final = explode_sprints

    # Salvando o resultado final no bucket
    bucket_gold = get_bucket_name("lake-gold")
    result_final.to_parquet(
        f"s3://{bucket_gold}/notion/api_notion/tecnologia/projetos/projetos.parquet",
        index=False,
    )
