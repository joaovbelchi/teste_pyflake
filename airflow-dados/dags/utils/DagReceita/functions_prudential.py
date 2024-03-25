from sqlalchemy import create_engine
import pandas as pd
from utils.secrets_manager import get_secret_value
import json


def transferencia_prudential_para_s3(s3_landing_path: str):
    """Faz a extração da base de dados prudential de um
    banco de dados RDS e realiza o armazenamento da base
    em um objeto do S3.

    Args:
        s3_landing_path (str): path onde o objeto será
        armazenado
    """

    # Obtem credenciais para acesso ao RDS
    credenciais = json.loads(get_secret_value("prd/rds/database/admin"))

    # Configs de acesso ao RDS
    db_port = "3306"
    db_name = "RECEITA"
    db_host = credenciais["host"]
    db_password = credenciais["password"]
    db_username = credenciais["username"]

    # Criando a string de conexão
    connection_str = (
        f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    # Criando a conexão usando SQLAlchemy
    engine = create_engine(connection_str)

    # Exemplo de consulta SQL
    sql_query = "SELECT * FROM RECEITA.PRUDENTIAL"

    # Executando a consulta e armazenando o resultado em um DataFrame do Pandas
    df_prudential = pd.read_sql(sql_query, engine)

    # Salvando o DataFrame na camada landing
    df_prudential.to_parquet(s3_landing_path)


def carrega_e_limpa_base_prudential(df_prudential: pd.DataFrame) -> pd.DataFrame:
    """Processa a base de dados prudential.

    Args:
        df_prudential (pd.DataFrame): DataFrame pandas com a base prudential

    Returns:
        pd.DataFrame: DataFrame pandas com a base prudential processada
    """

    # Seleciona colunas e aplica algumas regras de negócio
    columns = [
        "Data",
        "Produto",
        "Comissao_nova",
        "Comissao_renovacao",
        "Conta",
        "Segurado",
    ]
    df_prudential = df_prudential.loc[:, columns]
    df_prudential.loc[:, "Segurado"] = df_prudential["Segurado"].fillna("Sem nome")
    df_prudential.loc[:, "Produto"] = df_prudential["Produto"].fillna("Seguro de Vida")
    df_prudential.loc[:, "Conta"] = df_prudential["Conta"].fillna("0")
    df_prudential["Comissão"] = (
        df_prudential["Comissao_nova"] + df_prudential["Comissao_renovacao"]
    )
    df_prudential.drop(["Comissao_nova", "Comissao_renovacao"], axis=1, inplace=True)

    df_prudential["Categoria"] = "SEGURO DE VIDA"

    df_prudential["Tipo"] = "SEGUROS"

    df_prudential["Intermediador"] = "BTG PACTUAL"
    df_prudential["Fornecedor"] = "PRUDENTIAL"
    df_prudential["Financeira"] = "BTG PACTUAL CORRETORA DE SEGUROS"

    novo_nome_colunas = {"Produto": "Produto_1", "Segurado": "Produto_2"}

    df_prudential.rename(columns=novo_nome_colunas, inplace=True)

    return df_prudential
