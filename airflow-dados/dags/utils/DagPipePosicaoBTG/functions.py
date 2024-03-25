from utils.api_btg import (
    create_session,
    get_token_btg,
    get_position_partner,
    get_position_account,
    get_position_account_date,
)
import hubspot
import unicodedata
from utils.functions import leitura_bronze
import boto3
from utils.bucket_names import get_bucket_name
import io
import zipfile
import json
import pandas as pd
from typing import List, Dict, Tuple, Literal
import time
from dateutil.parser import isoparse
import requests
from deltalake.exceptions import TableNotFoundError
from deltalake import write_deltalake, DeltaTable
from utils.secrets_manager import get_secret_value
from utils.OneGraph.drive import OneGraphDrive
from utils.mergeasof import fill_retroativo, remover_antes_entrada, mergeasof_socios
from datetime import datetime, timedelta
import pytz
from utils.msteams import msteams_qualidade_dados
from dateutil.relativedelta import relativedelta
import numpy as np
from hubspot.crm.objects import BatchInputSimplePublicObjectBatchInput


def download_posicao_escritorio(s3_obj_key: str):
    """Usa o endpoint get-partner-position da API de
    Posição do BTG para realizar o download das posições
    de todas as contas do escritório. O resultado é
    escrito em camada Landing do Data Lake.

    Args:
        s3_obj_key (str): Chave do objeto do S3 que será
        criado com o resultado do download.
    """
    session = create_session()
    token = get_token_btg(session)
    position_bytes = get_position_partner(session, token)

    bucket = get_bucket_name("lake-landing")
    s3 = boto3.client("s3")
    s3.put_object(Body=position_bytes, Bucket=bucket, Key=s3_obj_key)


def read_obj_from_s3(bucket: str, object_key: str) -> bytes:
    """Realiza leitura de objeto do S3

    Args:
        bucket (str): Nome do bucket do S3.
        object_key (str): Chave do objeto do S3.

    Returns:
        bytes: Conteúdo do objeto em bytes.
    """
    s3_client = boto3.client("s3")
    s3_response_object = s3_client.get_object(Bucket=bucket, Key=object_key)
    return s3_response_object["Body"].read()


def unzip_and_load_to_df(data: bytes) -> pd.DataFrame:
    """Descompacta os arquivos JSON e os transforma em
    um pandas DataFrame.

    Args:
        data (bytes): Bytes de arquivo .zip contendo
        arquivos JSON

    Raises:
        Exception: Caso nenhum arquivo JSON seja encontrado no zip

    Returns:
        pd.DataFrame: DataFrame contendo cada arquivo JSON
        em uma linha.
    """
    json_data_list = []
    with io.BytesIO(data) as zip_io:
        with zipfile.ZipFile(zip_io, "r") as zip_ref:
            all_files = zip_ref.namelist()
            for file_name in all_files:
                if file_name.endswith(".json"):
                    with zip_ref.open(file_name) as json_file:
                        json_data = json.load(json_file)
                        json_data_list.append(json_data)

    if not json_data_list:
        raise Exception("Nenhum arquivo JSON encontrado no zip")

    return pd.DataFrame.from_records(data=json_data_list)


def leitura_posicao_escritorio(
    s3_bucket_landing: str, s3_object_key: str
) -> pd.DataFrame:
    """Realiza o processamento de dados de posição em formato JSON
    compactado para pandas DataFrame.

    Args:
        s3_bucket_landing (str): Nome do bucket da camada Landing do
        Data Lake
        s3_object_key (str): Chave do objeto do S3 que será
        processado.

    Raises:
        Exception: Caso nenhum arquivo JSON seja encontrado no zip

    Returns:
        pd.DataFrame: DataFrame contendo todas as posições do escritório.
    """

    # Funções Auxiliares
    def explode_and_normalize(
        df: pd.DataFrame, col: str, drop_empty: bool = True
    ) -> pd.DataFrame:
        """Executa as operações explode e json_normalize em coluna
        de um pandas DataFrame. Assim, células que contém records
        são expandidas para formato tabular.
        Args:
            df (pd.DataFrame): Pandas DataFrame.
            col (str): Coluna contendo strings em formato de record.
            drop_empty (bool, optional): Determina se linhas que contém
            valores nulos (ou que retornam falso) na célula de record
            são removidas ou mantidas no DataFrame.

        Returns:
            pd.DataFrame: DataFrame expandido.
        """
        if drop_empty:
            df = df[(df[col].apply(lambda x: bool(x))) & ~(df[col].isna())]
        df = df.explode([col], ignore_index=True)
        normalized = pd.json_normalize(df[col])
        new_columns = [
            i for i in normalized.columns if i not in ["AccountNumber", "PositionDate"]
        ]
        df[new_columns] = normalized[new_columns]
        df = df.drop(columns=[col])
        return df

    def transform(
        df: pd.DataFrame,
        mapping: Dict[str, Dict[str, str]],
        explode_cols: List[str] = [],
        keep_empty: List[str] = [],
        mapping_agg: Dict[str, str] = {},
        static_values: Dict[str, str] = {},
        duplicate_cols: Dict[str, str] = {},
    ) -> pd.DataFrame:
        """Executa as transformações desejadas em DataFrame de JSONs descompactados,
        de modo que seja retornado um DataFrame com colunas mapeadas e com agregações
        realizadas para um mercado ou sub-mercado. Os tratamentos executados por essa função
        são comuns para todos esses mercados/sub-mercados, mudando apenas alguns parâmetros
        de configuração, que são os parâmetros dessa função.

        Args:
            df (pd.DataFrame): DataFrame de JSONs descompactados.
            mapping (Dict[str, Dict[str, str]]): Mapeamento de coluna no DataFrame
            de origem para coluna e data type no DataFrame de destino. Exemplo:
            {"AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"}, ...}
            explode_cols (List[str], optional): Lista de colunas em que será
            executado o método explode de DataFrames do Pandas.
            keep_empty (List[str], optional): Por padrão, colunas explodidas não mantém
            registros que continham valores vazios para essas mesmas colunas. Se for necessário
            manter esses registros, basta adicionar o nome da coluna à lista
            mapping_agg (Dict[str, str], optional): Mapeamento de colunas em que devem
            ser aplicadas transformações de agregação. Exemplo: {"NumberOfShares":
            {"name": "quantidade", "dtype": "double[pyarrow]", "agg": "sum"}, ...}
            static_values (Dict[str, str], optional): Mapeamento de colunas para as quais
            deve ser atribuído um valor estático. Exemplo: {"mercado": "FUNDOS", "sub_mercado": "FN", ...}
            duplicate_cols (Dict[str, str], optional): Mapeamento de colunas a serem replicadas
            com outro nome. Exemplo: {"data_interface": "data_movimentacao", ...}

        Returns:
            pd.DataFrame: DataFrame após aplicação das transformações definidas.
        """

        # Explode e normalize
        for col in explode_cols:
            if not col in df.columns:
                print(f"Não houve posição de {col}. Retornando vazio")
                return pd.DataFrame()

            drop_empty = False if col in keep_empty else True
            df = explode_and_normalize(df, col, drop_empty=drop_empty)

        if df.empty:
            return pd.DataFrame()

        # Mapeamentos diretos entre colunas
        mapeamento_name = {k: v["name"] for k, v in mapping.items()}
        mapeamento_schema = {i["name"]: i["dtype"] for i in mapping.values()}
        df.rename(columns=mapeamento_name, inplace=True)

        for col, dtype in mapeamento_schema.items():
            if "timestamp" in dtype:
                df[col] = pd.to_datetime(
                    df[col], format="ISO8601", utc=True, errors="coerce"
                )
            elif "double" in dtype:
                df[col] = pd.to_numeric(df[col])

            df[col] = df[col].astype(dtype)

        # Filtra colunas
        df = df[list(mapeamento_name.values()) + list(mapping_agg.keys())]

        # Agrega colunas
        if mapping_agg:
            for i, j in mapping_agg.items():
                df[i] = df[i].astype(j["dtype"])
            mapeamento_agg = {i: j["agg"] for i, j in mapping_agg.items()}
            df = (
                df.groupby(list(mapeamento_name.values()), dropna=False)
                .agg(mapeamento_agg)
                .reset_index()
            )

            mapeamento_name = {i: j["name"] for i, j in mapping_agg.items()}
            df.rename(columns=mapeamento_name, inplace=True)

        # Atribuição de valores fixos
        for key, value in static_values.items():
            df[key] = value
            df[key] = df[key].astype("string[pyarrow]")

        # Replicação de colunas
        for key, value in duplicate_cols.items():
            df[value] = df[key]

        return df

    def process_InvestmentFund(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de InvestmentFunds

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["InvestmentFund", "Acquisition"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "Fund.DatePortfolio": {
                "name": "data_movimentacao",
                "dtype": "timestamp[us][pyarrow]",
            },
            "Fund.FundName": {"name": "produto", "dtype": "string[pyarrow]"},
            "Fund.FundCNPJCode": {"name": "cnpj_fundo", "dtype": "string[pyarrow]"},
            "Fund.FundCGECode": {"name": "cge_fundo", "dtype": "string[pyarrow]"},
            "ShareValue": {"name": "valor_exercicio", "dtype": "double[pyarrow]"},
        }
        mapping_agg = {
            "NumberOfShares": {
                "name": "quantidade",
                "dtype": "double[pyarrow]",
                "agg": "sum",
            },
            "GrossAssetValue": {
                "name": "valor_bruto",
                "dtype": "double[pyarrow]",
                "agg": "sum",
            },
            "IncomeTax": {"name": "valor_ir", "dtype": "double[pyarrow]", "agg": "sum"},
            "VirtualIOF": {
                "name": "valor_iof",
                "dtype": "double[pyarrow]",
                "agg": "sum",
            },
            "NetAssetValue": {
                "name": "valor_liquido",
                "dtype": "double[pyarrow]",
                "agg": "sum",
            },
        }
        static_values = {"mercado": "FUNDOS", "sub_mercado": "FN"}

        return transform(
            df=df,
            mapping=mapping,
            explode_cols=explode_cols,
            mapping_agg=mapping_agg,
            static_values=static_values,
        )

    def process_FixedIncome(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de FixedIncome

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["FixedIncome", "Acquisitions"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "AccountingGroupCode": {"name": "produto", "dtype": "string[pyarrow]"},
            "Ticker": {"name": "ativo", "dtype": "string[pyarrow]"},
            "Issuer": {"name": "emissor", "dtype": "string[pyarrow]"},
            "ReferenceIndexValue": {"name": "indexador", "dtype": "string[pyarrow]"},
            "Quantity": {"name": "quantidade", "dtype": "double[pyarrow]"},
            "InitialInvestmentValue": {
                "name": "valor_custo",
                "dtype": "double[pyarrow]",
            },
            "GrossValue": {"name": "valor_bruto", "dtype": "double[pyarrow]"},
            "IncomeTax": {"name": "valor_ir", "dtype": "double[pyarrow]"},
            "IOFTax": {"name": "valor_iof", "dtype": "double[pyarrow]"},
            "YieldToMaturity": {"name": "valor_taxa", "dtype": "double[pyarrow]"},
            "ReferenceIndexName": {
                "name": "valor_taxa_compra",
                "dtype": "string[pyarrow]",
            },
            "Price": {"name": "valor_exercicio", "dtype": "double[pyarrow]"},
            "NetValue": {"name": "valor_liquido", "dtype": "double[pyarrow]"},
            "AcquisitionDate": {
                "name": "data_aquisicao",
                "dtype": "timestamp[us][pyarrow]",
            },
            "MaturityDate": {
                "name": "data_vencimento",
                "dtype": "timestamp[us][pyarrow]",
            },
        }

        static_values = {"mercado": "RENDA FIXA", "sub_mercado": "RF"}

        duplicate_cols = {
            "valor_taxa": "valor_taxa_compra",
            "data_interface": "data_movimentacao",
        }

        return transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )

    def process_FixedIncomeStructuredNote(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de FixedIncomeStructuredNote

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["FixedIncomeStructuredNote"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "Ticker": {"name": "ativo", "dtype": "string[pyarrow]"},
            "Issuer": {"name": "emissor", "dtype": "string[pyarrow]"},
            "ReferenceIndexValue": {"name": "indexador", "dtype": "string[pyarrow]"},
            "Quantity": {"name": "quantidade", "dtype": "double[pyarrow]"},
            "InitialInvestmentValue": {
                "name": "valor_custo",
                "dtype": "double[pyarrow]",
            },
            "GrossValue": {"name": "valor_bruto", "dtype": "double[pyarrow]"},
            "IncomeTax": {"name": "valor_ir", "dtype": "double[pyarrow]"},
            "IOFTax": {"name": "valor_iof", "dtype": "double[pyarrow]"},
            "Yield": {"name": "valor_taxa", "dtype": "double[pyarrow]"},
            "ReferenceIndexName": {
                "name": "valor_taxa_indexador",
                "dtype": "string[pyarrow]",
            },
            "Price": {"name": "valor_exercicio", "dtype": "double[pyarrow]"},
            "NetValue": {"name": "valor_liquido", "dtype": "double[pyarrow]"},
            "MaturityDate": {
                "name": "data_vencimento",
                "dtype": "timestamp[us][pyarrow]",
            },
        }

        static_values = {
            "mercado": "RENDA FIXA",
            "sub_mercado": "COE",
            "produto": "COE",
        }

        duplicate_cols = {"data_interface": "data_movimentacao"}

        return transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )

    def process_CashCollateral(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de CashCollateral

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["Cash", "CashCollateral"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "CollateralDescription": {"name": "produto", "dtype": "string[pyarrow]"},
            "FinancialValue": {"name": "valor_bruto", "dtype": "double[pyarrow]"},
            "Custodian": {"name": "Custodian", "dtype": "string[pyarrow]"},
            "CustodianCode": {"name": "CustodianCode", "dtype": "string[pyarrow]"},
        }

        static_values = {"mercado": "DINHEIRO EM GARANTIA", "sub_mercado": "CM"}

        duplicate_cols = {
            "valor_bruto": "valor_liquido",
            "valor_liquido": "quantidade",
            "data_interface": "data_movimentacao",
        }

        df_CM = transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )

        if not df_CM.empty:
            df_CM["tipo"] = df_CM["Custodian"] + " - " + df_CM["CustodianCode"]
            df_CM.drop(columns=["Custodian", "CustodianCode"], inplace=True)

            df_CM = df_CM[
                ((df_CM["valor_bruto"] > 0) | (df_CM["valor_bruto"] < 0))
                & (df_CM["produto"] != "Bloqueio Judicial")
            ].reset_index(drop=True)

        return df_CM

    def process_CashInvested(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de CashInvested

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["Cash", "CashInvested"]
        keep_empty = ["CashInvested"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
        }

        mapping_agg = {
            "GrossValue": {
                "name": "valor_bruto",
                "dtype": "double[pyarrow]",
                "agg": "sum",
            },
            "NetValue": {
                "name": "valor_liquido",
                "dtype": "double[pyarrow]",
                "agg": "sum",
            },
            "CurrentAccount.Value": {
                "name": "CurrentAccount.Value",
                "dtype": "double[pyarrow]",
                "agg": "first",
            },
        }

        static_values = {
            "mercado": "CONTA CORRENTE",
            "sub_mercado": "CC",
            "produto": "CONTA CORRENTE",
            "valor_exercicio": "1",
        }

        duplicate_cols = {
            "valor_bruto": "quantidade",
            "data_interface": "data_movimentacao",
        }

        df_CASH = transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            mapping_agg=mapping_agg,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
            keep_empty=keep_empty,
        )

        if not df_CASH.empty:
            for col in ["valor_bruto", "valor_liquido", "quantidade"]:
                df_CASH[col] = df_CASH[col] + df_CASH["CurrentAccount.Value"]

            df_CASH["valor_exercicio"] = df_CASH["valor_exercicio"].astype(
                "double[pyarrow]"
            )

            df_CASH.drop(columns=["CurrentAccount.Value"], inplace=True)

        return df_CASH

    def process_PendingSettlements(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de PendingSettlements

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "Description": {"name": "produto", "dtype": "string[pyarrow]"},
            "SettlementDate": {
                "name": "data_vencimento",
                "dtype": "timestamp[us][pyarrow]",
            },
        }

        mapping_agg = {
            "FinancialValue": {
                "name": "valor_bruto",
                "dtype": "double[pyarrow]",
                "agg": "sum",
            },
            "TotalAmmount": {
                "name": "quantidade",
                "dtype": "string[pyarrow]",
                "agg": "size",
            },
        }

        static_values = {"mercado": "VALOR EM TRÂNSITO"}

        duplicate_cols = {
            "valor_bruto": "valor_exercicio",
            "valor_exercicio": "valor_liquido",
            "data_interface": "data_movimentacao",
        }

        PS_config = {
            "RF": ["PendingSettlements", "FixedIncome"],
            "RV": ["PendingSettlements", "Equities"],
            "FN": ["PendingSettlements", "InvestmentFund"],
            "PN": ["PendingSettlements", "Pension"],
            # "Others": ["PendingSettlements", "Others"],
            # "Derivative": ["PendingSettlements", "Derivative"],
        }
        df_PS = pd.DataFrame()
        for sub_mercado, explode_cols in PS_config.items():
            df_temp = transform(
                df.copy(),
                mapping=mapping,
                mapping_agg=mapping_agg,
                explode_cols=explode_cols,
                static_values=static_values,
                duplicate_cols=duplicate_cols,
            )
            if not df_temp.empty:
                df_temp["sub_mercado"] = sub_mercado
                df_temp["sub_mercado"] = df_temp["sub_mercado"].astype(
                    "string[pyarrow]"
                )
                df_PS = pd.concat(
                    [
                        df_PS,
                        df_temp,
                    ]
                )

        if not df_PS.empty:
            df_PS["quantidade"] = df_PS["quantidade"].astype("int32[pyarrow]")

        return df_PS

    def process_CryptoCoin(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de CryptoCoin

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["CryptoCoin", "Acquisition"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "Asset.Name": {"name": "produto", "dtype": "string[pyarrow]"},
            "InterfaceDate": {
                "name": "data_aquisicao",
                "dtype": "timestamp[us][pyarrow]",
            },
        }

        mapping_agg = {
            "Quantity": {
                "name": "quantidade",
                "dtype": "double[pyarrow]",
                "agg": "sum",
            },
            "CostBasis": {
                "name": "valor_custo",
                "dtype": "double[pyarrow]",
                "agg": "sum",
            },
            "GrossFinancial": {
                "name": "valor_bruto",
                "dtype": "double[pyarrow]",
                "agg": "sum",
            },
            "IncomeTax": {"name": "valor_ir", "dtype": "double[pyarrow]", "agg": "sum"},
            "IOFTax": {"name": "valor_iof", "dtype": "double[pyarrow]", "agg": "sum"},
            "PUClosing": {
                "name": "valor_preco_mercado",
                "dtype": "double[pyarrow]",
                "agg": "sum",
            },
            "Financial": {
                "name": "valor_liquido",
                "dtype": "double[pyarrow]",
                "agg": "sum",
            },
        }

        static_values = {"mercado": "CRIPTOATIVOS", "sub_mercado": "CRY"}

        duplicate_cols = {
            "produto": "ativo",
            "valor_preco_mercado": "valor_exercicio",
            "data_interface": "data_movimentacao",
        }

        return transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            mapping_agg=mapping_agg,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )

    def process_PensionInformations(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de PensionInformations

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["PensionInformations", "Positions"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "FundName": {"name": "produto", "dtype": "string[pyarrow]"},
            "CertificateName": {"name": "ativo", "dtype": "string[pyarrow]"},
            "FundCGECode": {"name": "cge_fundo", "dtype": "string[pyarrow]"},
            "NumberOfShares": {"name": "quantidade", "dtype": "double[pyarrow]"},
            "GrossAssetValue": {"name": "valor_bruto", "dtype": "double[pyarrow]"},
            "ShareValue": {"name": "valor_exercicio", "dtype": "double[pyarrow]"},
            "NetAssetValue": {"name": "valor_liquido", "dtype": "double[pyarrow]"},
            "IndividualNumberOfShares": {
                "name": "quantidade_contrato",
                "dtype": "double[pyarrow]",
            },
            "IndividualGrossAssetValue": {
                "name": "valor_contrato",
                "dtype": "double[pyarrow]",
            },
        }

        static_values = {"mercado": "PREVIDÊNCIA", "sub_mercado": "PP"}

        duplicate_cols = {"data_interface": "data_movimentacao"}

        df_PN = transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )

        if not df_PN.empty:
            df_PN["ativo"] = "CERTIFICADO PREV " + df_PN["ativo"]

        return df_PN

    def process_StockPositions(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de StockPositions

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["Equities", "StockPositions"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "Ticker": {"name": "produto", "dtype": "string[pyarrow]"},
            "Description": {"name": "tipo", "dtype": "string[pyarrow]"},
            "Quantity": {"name": "quantidade", "dtype": "double[pyarrow]"},
            "MarketPrice": {"name": "valor_preco_mercado", "dtype": "double[pyarrow]"},
            "IsFII": {"name": "is_fii", "dtype": "string[pyarrow]"},
            "AveragePrice.Price": {
                "name": "preco_medio",
                "dtype": "double[pyarrow]",
            }
        }

        static_values = {
            "mercado": "RENDA VARIÁVEL",
            "sub_mercado": "ACAO",
            "valor_ir": "0",
        }

        duplicate_cols = {
            "produto": "ativo",
            "valor_preco_mercado": "valor_exercicio",
            "data_interface": "data_movimentacao",
        }

        df_RV_ACAO = transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )

        if not df_RV_ACAO.empty:
            df_RV_ACAO["valor_bruto"] = (
                df_RV_ACAO["quantidade"] * df_RV_ACAO["valor_preco_mercado"]
            )
            df_RV_ACAO["valor_liquido"] = df_RV_ACAO["valor_bruto"]
            df_RV_ACAO["valor_ir"] = df_RV_ACAO["valor_ir"].astype("double[pyarrow]")

        #Popula o submercado de FII e dropa a coluna extra usada como flag
        df_RV_ACAO.loc[df_RV_ACAO['is_fii'] == 'true', 'sub_mercado'] = 'FII'
        df_RV_ACAO = df_RV_ACAO.drop(columns=['is_fii'])

        return df_RV_ACAO
    
    def process_Small_Caps(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de Small Caps

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["Equities", "PortfolioInvestments", "StockPositions"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "Ticker": {"name": "produto", "dtype": "string[pyarrow]"},
            "Description": {"name": "tipo", "dtype": "string[pyarrow]"},
            "Quantity": {"name": "quantidade", "dtype": "double[pyarrow]"},
            "MarketPrice": {"name": "valor_preco_mercado", "dtype": "double[pyarrow]"},
            "AveragePrice.Price": {
                "name": "preco_medio",
                "dtype": "double[pyarrow]",
            }
        }

        static_values = {
            "mercado": "RENDA VARIÁVEL",
            "sub_mercado": "ACAO",
            "valor_ir": "0",
        }

        duplicate_cols = {
            "produto": "ativo",
            "valor_preco_mercado": "valor_exercicio",
            "data_interface": "data_movimentacao",
        }

        df_small_caps = transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )

        if not df_small_caps.empty:
            df_small_caps["valor_bruto"] = (
                df_small_caps["quantidade"] * df_small_caps["valor_preco_mercado"]
            )
            df_small_caps["valor_liquido"] = df_small_caps["valor_bruto"]
            df_small_caps["valor_ir"] = df_small_caps["valor_ir"].astype("double[pyarrow]")

        return df_small_caps

    def process_OptionPositions(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de OptionPositions

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["Equities", "OptionPositions"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "Ticker": {"name": "produto", "dtype": "string[pyarrow]"},
            "OptionType": {"name": "tipo", "dtype": "string[pyarrow]"},
            "Quantity": {"name": "quantidade", "dtype": "double[pyarrow]"},
            "TotalValue": {"name": "valor_bruto", "dtype": "double[pyarrow]"},
            "StrikePrice": {"name": "valor_preco_compra", "dtype": "double[pyarrow]"},
            "MarketPremium": {"name": "valor_premio", "dtype": "double[pyarrow]"},
            "MaturityDate": {
                "name": "data_exercicio",
                "dtype": "timestamp[us][pyarrow]",
            },
            "AveragePrice.Price": {
                "name": "preco_medio",
                "dtype": "double[pyarrow]",
            }
        }

        static_values = {"mercado": "RENDA VARIÁVEL", "sub_mercado": "OPCAO"}

        duplicate_cols = {
            "produto": "ativo",
            "tipo": "tipo_opcao",
            "valor_preco_compra": "valor_exercicio",
            "valor_bruto": "valor_liquido",
            "data_interface": "data_movimentacao",
        }

        df_RV_OPCAO = transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )

        if not df_RV_OPCAO.empty:
            df_RV_OPCAO["tipo"] = df_RV_OPCAO["tipo"].str.upper()

        return df_RV_OPCAO

    def process_ForwardPositions(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de ForwardPositions

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["Equities", "ForwardPositions"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "Ticker": {"name": "produto", "dtype": "string[pyarrow]"},
            "Description": {"name": "emissor", "dtype": "string[pyarrow]"},
            "Quantity": {"name": "quantidade", "dtype": "double[pyarrow]"},
            "PLPrice": {"name": "valor_bruto", "dtype": "double[pyarrow]"},
            "StrikePrice": {"name": "valor_preco_mercado", "dtype": "double[pyarrow]"},
            "CostGrossPrice": {
                "name": "valor_preco_compra",
                "dtype": "double[pyarrow]",
            },
            "MaturityDate": {
                "name": "data_vencimento",
                "dtype": "timestamp[us][pyarrow]",
            },
        }

        static_values = {"mercado": "RENDA VARIÁVEL", "sub_mercado": "TERMO"}

        duplicate_cols = {
            "produto": "ativo",
            "valor_preco_mercado": "valor_exercicio",
            "valor_bruto": "valor_liquido",
            "data_interface": "data_movimentacao",
        }

        return transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )

    def process_StockLendingPositions(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de StockLendingPositions

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["Equities", "StockLendingPositions"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "Ticker": {"name": "produto", "dtype": "string[pyarrow]"},
            "LendingType": {"name": "tipo", "dtype": "string[pyarrow]"},
            "Quantity": {"name": "quantidade", "dtype": "double[pyarrow]"},
            "MarketPrice": {"name": "valor_preco_mercado", "dtype": "double[pyarrow]"},
            "TotalValue": {"name": "valor_bruto", "dtype": "double[pyarrow]"},
            "TransactionDate": {
                "name": "data_exercicio",
                "dtype": "timestamp[us][pyarrow]",
            },
            "MaturityDate": {
                "name": "data_vencimento",
                "dtype": "timestamp[us][pyarrow]",
            },
            "IRTax": {"name": "valor_ir", "dtype": "double[pyarrow]"},
        }

        static_values = {"mercado": "RENDA VARIÁVEL", "sub_mercado": "ALUGUEL"}

        duplicate_cols = {
            "produto": "ativo",
            "valor_bruto": "valor_liquido",
            "data_interface": "data_movimentacao",
        }

        return transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )

    def process_NDFPosition(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de NDFPosition

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["Derivative", "NDFPosition"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "ReferencedSecurity": {"name": "produto", "dtype": "string[pyarrow]"},
            "BuySell": {"name": "tipo_opcao", "dtype": "string[pyarrow]"},
            "GrossValue": {"name": "valor_bruto", "dtype": "double[pyarrow]"},
            "IncomeTax": {"name": "valor_ir", "dtype": "double[pyarrow]"},
            "IOFTax": {"name": "valor_iof", "dtype": "double[pyarrow]"},
            "CurrentSecurityPrice": {
                "name": "valor_exercicio",
                "dtype": "double[pyarrow]",
            },
            "InceptionDate": {
                "name": "data_aquisicao",
                "dtype": "timestamp[us][pyarrow]",
            },
        }

        static_values = {"mercado": "DERIVATIVOS", "sub_mercado": "NDF"}

        duplicate_cols = {
            "valor_bruto": "valor_liquido",
            "data_interface": "data_movimentacao",
        }

        return transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )
        
    def process_StructuredProducts(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de StructuredProducts

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["Equities", "StructuredProducts"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "ReferenceProductSymbol": {"name": "produto", "dtype": "string[pyarrow]"},
            "Name": {"name": "tipo_opcao", "dtype": "string[pyarrow]"},
            "Quantity": {"name": "quantidade", "dtype": "double[pyarrow]"},
            "GrossValue": {"name": "valor_bruto", "dtype": "double[pyarrow]"},
        }

        static_values = {"mercado": "DERIVATIVOS", "sub_mercado": "OF"}

        duplicate_cols = {
            "valor_bruto": "valor_liquido",
            "data_interface": "data_movimentacao",
        }

        df_estruturadas = transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )

        if not df_estruturadas.empty:
            df_estruturadas["tipo_opcao"] = df_estruturadas["tipo_opcao"].str.upper()
        else:
            return pd.DataFrame()
        
        df_estruturadas_mask = df_estruturadas.loc[~df_estruturadas["valor_bruto"].isin([0.0, 0])]

        return df_estruturadas_mask

    def process_CetipOptionPosition(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de CetipOptionPosition

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["Derivative", "CetipOptionPosition"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "Underlying": {"name": "produto", "dtype": "string[pyarrow]"},
            "OptionType": {"name": "tipo_opcao", "dtype": "string[pyarrow]"},
            "Quantity": {"name": "quantidade", "dtype": "double[pyarrow]"},
            "MarketValue": {"name": "valor_bruto", "dtype": "double[pyarrow]"},
            "MarketPremiumValue": {"name": "valor_premio", "dtype": "double[pyarrow]"},
            "StrikePrice": {"name": "valor_exercicio", "dtype": "double[pyarrow]"},
            "MaturityDate": {
                "name": "data_exercicio",
                "dtype": "timestamp[us][pyarrow]",
            },
        }

        static_values = {"mercado": "DERIVATIVOS", "sub_mercado": "OF"}

        duplicate_cols = {
            "valor_bruto": "valor_liquido",
            "data_interface": "data_movimentacao",
        }

        df_DT_OF = transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )

        if not df_DT_OF.empty:
            df_DT_OF["tipo_opcao"] = df_DT_OF["tipo_opcao"].str.upper()
        else:
            return pd.DataFrame()

        return df_DT_OF

    def process_BMFOptionPosition(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de BMFOptionPosition

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["Derivative", "BMFOptionPosition"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "SecurityDescription": {"name": "produto", "dtype": "string[pyarrow]"},
            "Ticker": {"name": "ativo", "dtype": "string[pyarrow]"},
            "OptionType": {"name": "tipo_opcao", "dtype": "string[pyarrow]"},
            "Quantity": {"name": "quantidade", "dtype": "double[pyarrow]"},
            "MarketValue": {"name": "valor_bruto", "dtype": "double[pyarrow]"},
            "MarketPremiumValue": {"name": "valor_premio", "dtype": "double[pyarrow]"},
            "MaturityDate": {
                "name": "data_exercicio",
                "dtype": "timestamp[us][pyarrow]",
            },
        }

        static_values = {"mercado": "DERIVATIVOS", "sub_mercado": "OPCAO"}

        duplicate_cols = {
            "valor_bruto": "valor_liquido",
            "quantidade": "quantidade_contrato",
            "valor_liquido": "valor_contrato",
            "data_interface": "data_movimentacao",
        }

        df_DT_OPCAO = transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )

        if not df_DT_OPCAO.empty:
            df_DT_OPCAO["tipo_opcao"] = df_DT_OPCAO["tipo_opcao"].str.upper()

        return df_DT_OPCAO

    def process_SwapPosition(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de SwapPosition

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["Derivative", "SwapPosition"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "IndexAsset": {"name": "IndexAsset", "dtype": "string[pyarrow]"},
            "IndexLiability": {"name": "IndexLiability", "dtype": "string[pyarrow]"},
            "PrincipalAmount": {"name": "quantidade", "dtype": "double[pyarrow]"},
            "TotalValue": {"name": "valor_bruto", "dtype": "double[pyarrow]"},
        }

        static_values = {"mercado": "DERIVATIVOS", "sub_mercado": "SW"}

        duplicate_cols = {
            "valor_bruto": "valor_liquido",
            "data_interface": "data_movimentacao",
        }

        df_DT_SW = transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )

        if not df_DT_SW.empty:
            df_DT_SW["produto"] = (
                df_DT_SW["IndexAsset"] + " / " + df_DT_SW["IndexLiability"]
            )
            df_DT_SW.drop(columns=["IndexAsset", "IndexLiability"], inplace=True)
            df_DT_SW["quantidade"] = df_DT_SW["quantidade"].astype("double[pyarrow]")

        return df_DT_SW

    def process_Commodity(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de Commodity

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        explode_cols = ["Commodity"]
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "Ticker": {"name": "produto", "dtype": "string[pyarrow]"},
            "MarketPrice": {"name": "valor_exercicio", "dtype": "double[pyarrow]"},
            "Quantity": {"name": "quantidade", "dtype": "double[pyarrow]"},
            "MarketValue": {"name": "valor_bruto", "dtype": "double[pyarrow]"},
        }

        static_values = {"mercado": "DERIVATIVOS", "sub_mercado": "COMMODITIES"}

        duplicate_cols = {
            "produto": "ativo",
            "valor_bruto": "valor_liquido",
            "data_interface": "data_movimentacao",
        }

        return transform(
            df,
            mapping=mapping,
            explode_cols=explode_cols,
            static_values=static_values,
            duplicate_cols=duplicate_cols,
        )

    def process_Precatory(df: pd.DataFrame) -> pd.DataFrame:
        """Transformações a serem aplicadas a posições de Precatory

        Args:
            df (pd.DataFrame): DataFrame antes das transformações

        Returns:
            pd.DataFrame: DataFrame após transformações.
        """
        mapping = {
            "AccountNumber": {"name": "carteira", "dtype": "string[pyarrow]"},
            "PositionDate": {
                "name": "data_interface",
                "dtype": "timestamp[us][pyarrow]",
            },
            "EndPositionValue": {"name": "valor_bruto", "dtype": "double[pyarrow]"},
        }

        duplicate_cols = {
            "valor_bruto": "valor_liquido",
            "valor_liquido": "quantidade",
        }

        static_values = {"mercado": "PRECATÓRIOS", "sub_mercado": "PRC"}

        df_prc = explode_and_normalize(df, "SummaryAccounts")
        df_prc = df_prc.loc[
            df_prc["MarketName"] == "Precatory",
            [
                "AccountNumber",
                "PositionDate",
                "EndPositionValue",
            ],
        ]

        return transform(
            df_prc,
            mapping=mapping,
            duplicate_cols=duplicate_cols,
            static_values=static_values,
        )

    # Processamento
    data = read_obj_from_s3(s3_bucket_landing, s3_object_key)
    df = unzip_and_load_to_df(data)
    return pd.concat(
        [
            process_InvestmentFund(df.copy()),
            process_FixedIncome(df.copy()),
            process_FixedIncomeStructuredNote(df.copy()),
            process_CashCollateral(df.copy()),
            process_CashInvested(df.copy()),
            process_PendingSettlements(df.copy()),
            process_CryptoCoin(df.copy()),
            process_PensionInformations(df.copy()),
            process_StockPositions(df.copy()),
            process_Small_Caps(df.copy()),
            process_OptionPositions(df.copy()),
            process_ForwardPositions(df.copy()),
            process_StockLendingPositions(df.copy()),
            process_NDFPosition(df.copy()),
            process_StructuredProducts(df.copy()),
            process_CetipOptionPosition(df.copy()),
            process_BMFOptionPosition(df.copy()),
            process_SwapPosition(df.copy()),
            process_Commodity(df.copy()),
            process_Precatory(df.copy()),
        ]
    ).reset_index(drop=True)


def get_contas_para_download(
    timestamp_dagrun: str,
    modo_execucao: Literal[
        "fechamento_mes_anterior", "execucao_normal"
    ] = "execucao_normal",
) -> List[str]:
    """Verifica quais contas devem ser buscadas nas chamadas à API de posição.
    Para o modo de execução normal, essa informação é obtida a partir dos
    resultados da base posicao_escritorio. Para o modo de execução de fechamento
    de mês, são coletadas as contas abertas no último dia do mês da base de contas.

    Args:
        timestamp_dagrun (str): String de timestamp de execução
        do pipeline em formato ISO.
        modo_execucao (Literal): Define se o modo de execução é de
        'fechamento_mes_anterior' ou 'execucao_normal'.

    Returns:
        List[str]: Lista contendo códigos de contas com posições
        desatualizadas
    """
    timestamp_dagrun = isoparse(timestamp_dagrun)
    if modo_execucao == "execucao_normal":
        bucket = get_bucket_name("lake-bronze")
        path = f"s3://{bucket}/btg/api/posicao_escritorio/"

        df_posicao = leitura_bronze(path, timestamp_dagrun)

        contas = (
            df_posicao.loc[
                df_posicao["data_interface"].dt.date < timestamp_dagrun.date()
            ]["carteira"]
            .unique()
            .tolist()
        )

    elif modo_execucao == "fechamento_mes_anterior":
        ts_mes_anterior = timestamp_dagrun.replace(day=1) - timedelta(days=1)

        bucket = get_bucket_name("lake-silver")
        dt = DeltaTable(f"s3://{bucket}/btg/contas/")
        df_contas = dt.to_pandas(
            filters=[
                ("date_escrita", "=", str(ts_mes_anterior.date())),
            ]
        )
        contas = df_contas["carteira"].str.zfill(9).unique().tolist()

    return contas


def download_iterativo_posicoes(
    session: requests.Session,
    token: str,
    contas_desatualizadas: List[str],
    modo_execucao: Literal["fechamento_mes_anterior", "execucao_normal"],
    timestamp_dagrun: str,
) -> List[str]:
    """Realiza o download de posições atualizadas de contas específicas.

    Args:
        session (requests.Session): Sessão HTTP
        token (str): Token para autorização na API do BTG
        contas_desatualizadas (List[str]): Lista de contas para as quais
        se deseja posições atualizadas
        modo_execucao (Literal): Define se o modo de execução é de
        'fechamento_mes_anterior' ou 'execucao_normal'.
        timestamp_dagrun (str): Timestamp lógico de execução do pipeline

    Raises:
        Exception: Se a response veio em um padrão inesperado
        Exception: Se o loop ficar travado na mesma conta por 5 iterações

    Returns:
        List[str]: Lista com respostas da API em formato JSON
    """
    results = []
    unauthorized_accounts = []
    for i in contas_desatualizadas:
        status_code = None
        counter = 0
        while status_code != 200:
            try:
                if modo_execucao == "execucao_normal":
                    rsp = get_position_account(session, token, i)
                else:
                    date = str(
                        (
                            isoparse(timestamp_dagrun).replace(day=1)
                            - timedelta(days=1)
                        ).date()
                    )
                    rsp = get_position_account_date(session, token, i, date)
                status_code = rsp.status_code
            except requests.exceptions.ConnectionError as e:
                print("Connection Error", e)
                time.sleep(30)

            if status_code == 200:
                results.append(rsp.text)

            elif status_code == 404:
                print(rsp.status_code, rsp.text)
                print(f"Erro estranho {i}")
                counter -= 1
                time.sleep(1)

            elif "not authorized for fetching account" in rsp.text:
                unauthorized_accounts.append(rsp.text)
                break

            elif "Authorization Error" in rsp.text:
                token = get_token_btg(session)
                print("Token BTG renovado")

            elif status_code == 429:
                print(
                    "Limite de requisições por minuto excedido. Aguardando 30 segundos"
                )
                time.sleep(30)

            else:
                print(rsp.status_code, rsp.text)
                time.sleep(30)

            counter += 1

            if counter >= 5:
                raise Exception(f"Número máximo de tentativas excedidas. {rsp.text}")

            time.sleep(0.5)

    if unauthorized_accounts:
        print("Contas não-autorizadas:", unauthorized_accounts)

    return results


def consolida_posicoes_em_zip(posicoes: List[str]) -> bytes:
    """Consolida lista de JSONs em bytes de arquivo ZIP

    Args:
        posicoes (List[str]): Lista de JSONs

    Returns:
        bytes: Bytes de arquivo ZIP
    """
    zip_data = io.BytesIO()

    with zipfile.ZipFile(zip_data, "w") as zip_file:
        for index, json_string in enumerate(posicoes):
            json_file_name = f"{index}.json"
            json_bytes = json_string.encode("utf-8")
            zip_file.writestr(json_file_name, json_bytes)

    return zip_data.getvalue()


def download_posicoes_por_conta(
    s3_obj_key: str,
    timestamp_dagrun: str,
    modo_execucao: Literal["fechamento_mes_anterior", "execucao_normal"],
):
    """Usa o endpoint get-position-by-account da API de Posição
    do BTG para realizar o download de posições que vieram desatualizadas
    na API get-partner-position. Os resultados são escritos na camada Landing
    do Data Lake.

    Args:
        s3_obj_key (str): Chave do objeto a ser gravado na camada Landing
        timestamp_dagrun (str): Timestamp lógico de execução do pipeline
        modo_execucao (Literal): Define se o modo de execução é de
        'fechamento_mes_anterior' ou 'execucao_normal'.
    """
    session = create_session()
    token = get_token_btg(session)
    contas = get_contas_para_download(timestamp_dagrun, modo_execucao=modo_execucao)
    posicoes = download_iterativo_posicoes(
        session, token, contas, modo_execucao, timestamp_dagrun
    )
    zip_content = consolida_posicoes_em_zip(posicoes)

    bucket = get_bucket_name("lake-landing")
    s3 = boto3.client("s3")
    s3.put_object(Body=zip_content, Bucket=bucket, Key=s3_obj_key)


def consolida_posicao_silver(
    timestamp_dagrun: str,
    modo_execucao: Literal["fechamento_mes_anterior", "execucao_normal"],
) -> str:
    """Consolida as posições retornadas pelos endpoints
    get-partner-position e get-position-by-account em base única
    na camada Silver do Data Lake. Posições que vieram desatualizadas
    no primeiro endpoint são substituídas pelas posições retornadas
    pelo segundo.

    Args:
        timestamp_dagrun (str): Timestamp lógico de execução do pipeline.
        modo_execucao (Literal): Define se o modo de execução é de
        'fechamento_mes_anterior' ou 'execucao_normal'.

    Returns:
        str: Data que foi atualizada (formato ISO)
    """
    timestamp_dagrun_parsed = isoparse(timestamp_dagrun)
    ts_ultimo_mes = timestamp_dagrun_parsed - relativedelta(months=1)
    bucket_bronze = get_bucket_name("lake-bronze")
    bucket_silver = get_bucket_name("lake-silver")

    # Leitura de posição consolidada atual
    partition_ts = (
        timestamp_dagrun_parsed if modo_execucao == "execucao_normal" else ts_ultimo_mes
    )
    partitions = [
        ("ano_particao", "=", str(partition_ts.year)),
        ("mes_particao", "=", str(partition_ts.month)),
    ]
    path_consolidada = f"s3://{bucket_silver}/btg/api/posicao_consolidada/"
    df_posicao = leitura_delta(path_consolidada, partitions)
    if not df_posicao.empty:
        df_posicao = df_posicao.loc[
            df_posicao["timestamp_dagrun"] != timestamp_dagrun_parsed, :
        ].copy()

    # Leitura posição por conta
    path_por_conta = f"s3://{bucket_bronze}/btg/api/posicao_por_conta/"
    df_posicao_por_conta = leitura_bronze(path_por_conta, timestamp_dagrun_parsed)
    df_posicao_por_conta["tabela_origem"] = "posicao_por_conta"

    # Leitura de posição escritório ou de fechamento
    df_final = pd.DataFrame()
    if modo_execucao == "execucao_normal":
        ts_atualizacao = isoparse(timestamp_dagrun)
        contas_desatualizadas = get_contas_para_download(timestamp_dagrun)
        path_escritorio = f"s3://{bucket_bronze}/btg/api/posicao_escritorio/"
        df_posicao_escritorio = leitura_bronze(path_escritorio, timestamp_dagrun_parsed)
        df_posicao_escritorio["tabela_origem"] = "posicao_escritorio"
        df_posicao_escritorio = df_posicao_escritorio.loc[
            ~df_posicao_escritorio["carteira"].isin(contas_desatualizadas)
        ].copy()
        df_final = pd.concat([df_posicao_escritorio, df_posicao_por_conta]).reset_index(
            drop=True
        )

    elif modo_execucao == "fechamento_mes_anterior":
        ts_fechamento_mes = timestamp_dagrun_parsed.replace(day=1) - timedelta(days=1)
        ts_atualizacao = df_posicao.loc[
            df_posicao["timestamp_dagrun"].dt.date <= ts_fechamento_mes.date()
        ]["timestamp_dagrun"].max()
        df_posicao_fechamento = df_posicao.loc[
            (df_posicao["timestamp_dagrun"] == ts_atualizacao)
            & (~df_posicao["carteira"].isin(df_posicao_por_conta["carteira"]))
        ].copy()
        df_posicao_fechamento["timestamp_dagrun"] = timestamp_dagrun_parsed
        df_posicao_fechamento["timestamp_dagrun"] = df_posicao_fechamento[
            "timestamp_dagrun"
        ].astype("datetime64[ns]")
        df_posicao = df_posicao.loc[~(df_posicao["timestamp_dagrun"] == ts_atualizacao)]
        df_final = pd.concat([df_posicao_por_conta, df_posicao_fechamento]).reset_index(
            drop=True
        )

        # Ajustando ano_particao e mes_particao
        df_final["ano_particao"] = str(ts_ultimo_mes.year)
        df_final["mes_particao"] = str(ts_ultimo_mes.month)

    # Concatenando informações a serem mantidas e informações novas
    df_final = pd.concat([df_posicao, df_final]).reset_index(drop=True)

    # Escrita últimos dois meses
    write_deltalake(
        path_consolidada,
        df_final,
        mode="overwrite",
        partition_by=[part[0] for part in partitions],
        partition_filters=partitions,
    )
    print(f"Overwrite realizado nas partições {partitions} da camada Silver")

    # Compactação da partição
    dt_posicao = DeltaTable(path_consolidada)
    dt_posicao.optimize.compact(partition_filters=partitions)
    print("Partição compactada")

    # Vacuum
    dt_posicao.vacuum(dry_run=False)
    print("Arquivos antigos apagados")

    # Retorna data que foi atualizada
    return str(ts_atualizacao.date())


def tratamento_posicao_silver(timestamp_dagrun: str, data_atualizacao: str):
    """Aplica regras de negócios sobre a base de posição
    consolidada e aplica um novo schema reduzido contendo
    apenas colunas de interesse.

    Args:
        timestamp_dagrun (str): Timestamp lógico de execução do pipeline.
        data_atualizacao (str): Data de posição a ser atualizada. Depende
        do modo de execução da DAG
    """

    timestamp_dagrun_parsed = isoparse(timestamp_dagrun)
    data_atualizacao_parsed = isoparse(data_atualizacao)
    bucket_silver = get_bucket_name("lake-silver")
    partitions = [
        ("ano_particao", "=", str(data_atualizacao_parsed.year)),
        ("mes_particao", "=", str(data_atualizacao_parsed.month)),
    ]

    path = f"s3://{bucket_silver}/btg/api/posicao_consolidada/"
    df = leitura_delta(path, partitions)
    df = df.loc[df["timestamp_dagrun"] == timestamp_dagrun_parsed]

    # Completa colunas de data_vencimento e emissor
    df["data_vencimento"] = df["data_vencimento"].combine_first(df["data_exercicio"])
    df["emissor"] = df["emissor"].combine_first(df["tipo"])

    # Altera mercado de Cetipados para RV
    df.loc[
        df["produto"].isin(
            [
                "BTG PACTUAL ENERGY DB FIP INFRAESTRUTURA",
                "FII BTG PACTUAL REAL ESTATE HEDGE FUND",
                "BTG INCORPORACAO FII",
                "BTG PACTUAL LOGCP FII",
                "RBR OPORTUNIDADES FII",
            ]
        ),
        "mercado",
    ] = "RENDA VARIÁVEL"

    # Cria coluna com data de execução
    df["date"] = data_atualizacao

    # Aplica schema reduzido
    rename_dict = {
        "carteira": "account",
        "mercado": "market",
        "sub_mercado": "sub_market",
        "produto": "product",
        "cnpj_fundo": "cnpj",
        "emissor": "issuer",
        "ativo": "stock",
        "data_vencimento": "maturity_date",
        "quantidade": "amount",
        "preco_medio": "average_price",
        "valor_bruto": "gross_value",
        "tipo_opcao": "option_type",
        "valor_ir": "ir",
        "valor_iof": "iof",
        "valor_liquido": "net_value",
        "date": "date",
        "data_interface": "data_interface",
        "indexador": "indexer",
        "valor_taxa_compra": "bought_coupon",
        "timestamp_dagrun": "timestamp_dagrun",
        "ano_particao": "ano_particao",
        "mes_particao": "mes_particao",
    }
    df.rename(columns=rename_dict, inplace=True)
    df = df.loc[:, list(rename_dict.values())]

    path_posicao_tratada = f"s3://{bucket_silver}/btg/api/posicao_tratada/"
    df_posicao_tratada = leitura_delta(path_posicao_tratada, partitions)
    if not df_posicao_tratada.empty:
        df_posicao_tratada = df_posicao_tratada.loc[
            df_posicao_tratada["timestamp_dagrun"] != timestamp_dagrun_parsed, :
        ].copy()

    df_final = pd.concat([df_posicao_tratada, df]).reset_index(drop=True)

    write_deltalake(
        path_posicao_tratada,
        df_final,
        mode="overwrite",
        partition_by=[part[0] for part in partitions],
        partition_filters=partitions,
    )
    print(f"Overwrite realizado nas partições {partitions} da camada Silver")

    # Compactação da partição
    dt_posicao = DeltaTable(path_posicao_tratada)
    dt_posicao.optimize.compact(partition_filters=partitions)
    print("Partição compactada")

    # Vacuum
    dt_posicao.vacuum(dry_run=False)
    print("Arquivos antigos apagados")


def leitura_delta(
    path: str, partitions: List[Tuple[str, str, str]] = None
) -> pd.DataFrame:
    """Realiza a leitura de uma tabela Delta para
    um pandas DataFrame. Se a tabela ainda não existir, ou estiver
    vazia, retorna um DataFrame vazio.

    Args:
        path (str): Path completo da tabela Delta
        partitions (List[Tuple[str, str, str]], optional): Lista de partições
        a serem lidas.

    Returns:
        pd.DataFrame: DataFrame contendo os dados da tabela.
    """
    try:
        dt = DeltaTable(path)
        df = dt.to_pandas(filters=partitions)

    except TableNotFoundError:
        df = pd.DataFrame()

    return df


def PL_silver(timestamp_dagrun: str, drive_id: str, data_atualizacao: str):
    """Calcula a base de PL a partir das bases de posição BTG e Offshore.
    Posição BTG é sempre a mais atual do dia, e Offshore é utilizado o último
    recebimento, que costuma ter de 1 a 2 meses de atraso. O último arquivo de
    Offshore é utilizado nos meses em que ainda não há dados.

    Args:
        timestamp_dagrun (str): Timestamp lógico de execução do pipeline.
        drive_id (str): ID do drive do OneDrive de onde será carregada
        a base de Offshore
        data_atualizacao (str): Data de posição a ser atualizada. Depende
        do modo de execução da DAG
    """
    # Leitura das posições novas
    timestamp_dagrun_parsed = isoparse(timestamp_dagrun)
    data_atualizacao_parsed = isoparse(data_atualizacao)
    bucket_silver = get_bucket_name("lake-silver")
    partitions = [
        ("ano_particao", "=", str(data_atualizacao_parsed.year)),
        ("mes_particao", "=", str(data_atualizacao_parsed.month)),
    ]
    path_posicao_novos = f"s3://{bucket_silver}/btg/api/posicao_tratada/"
    df_posicao_novos = leitura_delta(path_posicao_novos, partitions)
    df_posicao_novos = df_posicao_novos[
        df_posicao_novos["timestamp_dagrun"] == timestamp_dagrun_parsed
    ]

    # Geração do PL novo (sem offshore)
    df_pl_novos = df_posicao_novos
    rename_dict = {
        "account": "conta",
        "market": "mercado",
        "gross_value": "valor_bruto",
    }

    df_pl_novos.rename(columns=rename_dict, inplace=True)

    df_pl_novos = df_pl_novos.loc[:, list(rename_dict.values())]
    df_pl_novos = (
        df_pl_novos.groupby(["conta", "mercado"]).sum("valor_bruto").reset_index()
    )

    df_pl_novos["registro"] = data_atualizacao
    df_pl_novos["ano"] = data_atualizacao_parsed.year
    df_pl_novos["mes"] = data_atualizacao_parsed.month
    df_pl_novos["final_do_mes"] = True
    df_pl_novos["conta"] = df_pl_novos["conta"].astype("int64").astype("string")

    # Download do offshore
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)
    df_offshore = drive.download_file(
        ["One Analytics", "Banco de dados", "Histórico", "Base_OffShore.xlsx"],
        to_pandas=True,
    )

    data_maxima = df_offshore["Data"].max()

    df_offshore = df_offshore[
        (df_offshore["Data"].dt.year == data_maxima.year)
        & (df_offshore["Data"].dt.month == data_maxima.month)
    ]

    rename_dict = {
        "Acc Number": "conta",
        "AuM": "valor_bruto",
        "Data": "ano_mes_offshore",
    }

    df_offshore.rename(columns=rename_dict, inplace=True)
    df_offshore = df_offshore.loc[:, list(rename_dict.values())]

    df_offshore["conta"] = df_offshore["conta"].astype("string")
    df_offshore["registro"] = str(data_maxima.date())
    df_offshore["ano"] = data_maxima.year
    df_offshore["mes"] = data_maxima.month
    df_offshore["final_do_mes"] = True
    df_offshore["mercado"] = "Offshore"
    df_offshore["ano_mes_offshore"] = pd.to_datetime(
        df_offshore["ano_mes_offshore"]
    ).dt.to_period("M")
    df_offshore["ano_mes_offshore"] = df_offshore["ano_mes_offshore"].astype("string")

    # Leitura do PL atual
    path_PL = f"s3://{bucket_silver}/btg/api/PL/"
    df_pl_atual = leitura_delta(path_PL)

    # Concatena offshore ao PL
    df_pl_novos_com_offshore = pd.concat([df_pl_novos, df_offshore])
    df_pl_novos_com_offshore["ano"] = data_atualizacao_parsed.year
    df_pl_novos_com_offshore["mes"] = data_atualizacao_parsed.month
    df_pl_novos_com_offshore["registro"] = data_atualizacao

    # Cria mês atual do PL
    df_outros_meses = df_pl_atual.loc[
        ~(
            (df_pl_atual["mes"] == data_atualizacao_parsed.month)
            & (df_pl_atual["ano"] == data_atualizacao_parsed.year)
        ),
        :,
    ].copy()
    df_mes_atual = df_pl_atual.loc[
        (df_pl_atual["mes"] == data_atualizacao_parsed.month)
        & (df_pl_atual["ano"] == data_atualizacao_parsed.year)
        & ~(df_pl_atual["conta"].isin(df_pl_novos_com_offshore["conta"])),
        :,
    ].copy()
    df_mes_atual["final_do_mes"] = False
    df_mes_atual = pd.concat([df_mes_atual, df_pl_novos_com_offshore])

    # Atualiza offshore nos meses passados do PL
    df_outros_meses["inicio_mes_registro"] = (
        pd.to_datetime(df_outros_meses["registro"]).dt.to_period("M").dt.to_timestamp()
    )
    df_outros_meses["inicio_mes_offshore"] = (
        pd.to_datetime(df_outros_meses["ano_mes_offshore"])
        .dt.to_period("M")
        .dt.to_timestamp()
    )
    df_outros_meses = df_outros_meses[
        (
            df_outros_meses["inicio_mes_registro"]
            == df_outros_meses["inicio_mes_offshore"]
        )
        | df_outros_meses["ano_mes_offshore"].isna()
    ].drop(columns=["inicio_mes_registro", "inicio_mes_offshore"])

    # Lista contendo conjuntos de ano mês que devem ser atualizados
    ts_max_offshore = pd.to_datetime(
        df_outros_meses[df_outros_meses["mercado"] == "Offshore"]["registro"]
    ).max()
    ts_max_PL = pd.to_datetime(df_mes_atual["registro"]).max()
    lista_ano_mes = sorted(
        list(
            {
                (i.year, i.month)
                for i in pd.date_range(start=ts_max_offshore, end=ts_max_PL, freq="d")
            }
        )
    )[:-1]

    # Atualiza mês a mês
    for ano, mes in lista_ano_mes:
        df_outros_meses = df_outros_meses.loc[
            ~(
                (df_outros_meses["mercado"] == "Offshore")
                & (df_outros_meses["ano"] == ano)
                & (df_outros_meses["mes"] == mes)
            )
        ]
        registro_max = df_outros_meses[
            (df_outros_meses["ano"] == ano) & (df_outros_meses["mes"] == mes)
        ]["registro"].max()
        df_offshore_temp = df_offshore.copy()
        df_offshore_temp["ano"] = ano
        df_offshore_temp["mes"] = mes
        df_offshore_temp["registro"] = registro_max
        df_outros_meses = pd.concat([df_outros_meses, df_offshore_temp])

    df_final = (
        pd.concat([df_outros_meses, df_mes_atual])
        .sort_values(["registro", "conta"])
        .reset_index(drop=True)
    )
    df_final["ano"] = df_final["ano"].astype("int32")
    df_final["mes"] = df_final["mes"].astype("int32")

    path_PL = f"s3://{bucket_silver}/btg/api/PL/"
    write_deltalake(
        path_PL,
        df_final,
        mode="overwrite",
    )
    print(f"Overwrite realizado na camada Silver")

    # Compactação da partição
    dt_PL = DeltaTable(path_PL)
    dt_PL.optimize.compact()
    print("Partição compactada")

    # Vacuum
    dt_PL.vacuum(dry_run=False)
    print("Arquivos antigos apagados")


def PL_gold():
    """Utiliza as bases de PL, ContasHistorico e sócios para realização de
    uma operação de merge_as_of nos dados de PL. Escreve o resultado na camada
    Gold.
    """

    # Função que realiza a divisão da captação entre áreas.
    def atualizar_divisao_valores(
        base: pd.DataFrame, df_divisao_valores: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Atualiza a base de dados com as divisões de valores

        `Args`:
            base (pd.DataFrame): df_NNM ou df_PL ou df_Receita
            df_divisao_valores (pd.DataFrame): base de dados vinda do streamlit com as divisões de valores

        `Returns`:
            pd.DataFrame: retorna o dataframe da base correspondente com as divisões de valores atualizadas
        """

        # base -> NNM, PL, Receita
        # df_divisao_valores -> base que contem as linhas que serão divididas e suas divisões

        base["Percentual da divisao"] = 1
        base["Participação"] = base["Comercial"]

        # registros que vão ser retirados da base
        registros_antigos = df_divisao_valores.loc[
            df_divisao_valores["Linha original"] == True
        ]

        colunas_comuns = base.columns

        # iterar sobre cada registro e retira-lo da base
        for linha in registros_antigos.iterrows():
            linha = linha[1].to_frame().T
            filtro = base.isin(linha[colunas_comuns].to_dict(orient="list")).all(axis=1)
            indice_linha = base[filtro].index[0]
            base = base.drop(indice_linha)

        # registros que vao ser adicionados
        registros_novos = df_divisao_valores.loc[
            df_divisao_valores["Linha original"] == False
        ]

        # concatenar os registros novos com a base
        result = pd.concat([base, registros_novos[colunas_comuns]])

        # Ajusta as linhas na base de dados e corrige o indice
        result = result.sort_values("Data")
        result = result.reset_index(drop=True)

        return result

    bucket_silver = get_bucket_name("lake-silver")
    path = f"s3://{bucket_silver}/btg/api/PL/"
    pl_rename_cols = {
        "conta": "Conta",
        "mercado": "Mercado",
        "valor_bruto": "PL",
        "registro": "Data",
        "final_do_mes": "Final do mes",
    }
    df_pl_silver = DeltaTable(path).to_pandas(columns=list(pl_rename_cols.keys()))
    df_pl_silver.rename(columns=pl_rename_cols, inplace=True)
    df_pl_silver["Data"] = pd.to_datetime(df_pl_silver["Data"])

    df_advisors = pd.read_parquet(
        f"s3a://{bucket_silver}/api/btg/crm/ContasHistorico/",
        columns=[
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
    )

    bucket_gold = get_bucket_name("lake-gold")
    df_socios = pd.read_parquet(
        f"s3://{bucket_gold}/api/one/socios/socios.parquet",
        columns=["Nome_bd", "Data início", "Data Saída"],
    )

    df_pl_silver = df_pl_silver.sort_values(by="Data")
    df_advisors = df_advisors.sort_values(by="Data")

    df_pl_silver_back = pd.merge_asof(
        df_pl_silver, df_advisors, on="Data", by="Conta", direction="backward"
    )
    df_pl_silver_front = pd.merge_asof(
        df_pl_silver, df_advisors, on="Data", by="Conta", direction="forward"
    )
    df_pl = fill_retroativo(df_pl_silver_back, df_pl_silver_front)
    df_pl = df_pl.sort_values("Data").reset_index(drop=True)

    # Caso exista um valor definido para um sócio antes de sua entrada, essse valor é removido
    df_pl = remover_antes_entrada(df_pl, df_socios)

    # Cria uma coluna com Nome e conta para o BI
    df_pl["Nome e Conta"] = df_pl["Nome"].str.split(" ").str[0] + " - " + df_pl["Conta"]

    # Cria uma coluna definindo que a PL deste banco de dados é referente à Oneinvestimentos Assessoria de Investimentos (Classificação 0)
    df_pl["Negócio"] = "AAI"

    # Carrega o DataFrame com a indicação das linhas que precisam ser subistituiídas em função da divisão de PL entre áreas.
    df_divisao_valores = pd.read_parquet(
        f"s3://{bucket_gold}/api/one/classificacao_3/pl_divisao_valores.parquet"
    )

    # Realiza a divisão do PL entre áreas com base nas informações da base df_divisao_valores.
    df_pl = atualizar_divisao_valores(df_pl, df_divisao_valores)

    # Inclui ma base de PL as colunas de Área, Departamento e Unidade de Negócios para o comercial principal e para o comercial em Participação.
    # A inclusão desta informação é feita com o merge_asof
    df_pl = mergeasof_socios(
        df_pl,
        "Participação",
        "Data",
        {
            "area": "Área Participação",
            "business_unit": "Unidade de Negócios Participação",
            "department": "Departamento Participação",
        },
    )
    df_pl = mergeasof_socios(df_pl, "Comercial", "Data")

    # Salva na gold
    df_pl.to_parquet(f"s3a://{bucket_gold}/btg/api/PL/PL.parquet", index=False)


def validacoes_qualidade_landing_posicao(
    s3_bucket: str,
    s3_table_key: str,
    s3_partitions_key: str,
    s3_filename: str,
    webhook: str,
):
    """Realiza validações de qualidade nos dados de posição
    do BTG. Logs de qualidade e alertas são gerados. Em casos
    de falhas graves de qualidade, uma exceção sobe.

    Args:
        s3_bucket (str): Nome do bucket da camada landing.
        s3_table_key (str): Prefixo da chave completa do objeto.
        Vai até imediatamente antes do primeiro diretório de partição.
        s3_partitions_key (str): Parte da chave do objeto que contém as
        partições.
        s3_filename (str): Nome do arquivo na Landing com extensão.
        webhook (str): Webhook do canal de alertas de qualidade
        de dados no Teams.

    Raises:
        Exception: Se alguma métrica de qualidade estourar
    """
    # Carrega DataFrame
    landing_obj_key = f"{s3_table_key}/{s3_partitions_key}/{s3_filename}.zip"
    data = read_obj_from_s3(s3_bucket, landing_obj_key)
    df_cru = unzip_and_load_to_df(data)

    ## TESTE 1
    # Filtra TotalAmmount
    df_ta = df_cru.loc[:, ["AccountNumber", "TotalAmmount"]].copy()
    df_ta["TotalAmmount"] = df_ta["TotalAmmount"].astype("float64")
    valor_total = df_ta["TotalAmmount"].sum()

    # Carrega Posição
    df_pos = leitura_posicao_escritorio(s3_bucket, landing_obj_key)
    df_pos = df_pos.groupby("carteira")["valor_bruto"].sum().to_frame().reset_index()

    # Compara TotalAmmount com posição calculada
    df_merged = df_pos.merge(
        df_ta, left_on="carteira", right_on="AccountNumber", how="inner"
    )[["carteira", "valor_bruto", "TotalAmmount"]]
    df_merged["diff"] = df_merged["valor_bruto"] - df_merged["TotalAmmount"]
    df_merged = df_merged.loc[(df_merged["diff"] > 1) | (df_merged["diff"] < -1)]

    perc_valor = 0
    # Gera log com problemas de qualidade
    if df_merged.size > 0:
        # Calcula métricas
        contas_com_diff = df_merged["carteira"].to_list()
        diff_absoluta = df_merged["diff"].abs().sum()
        perc_valor = diff_absoluta / valor_total

        # Coleta last_modified do arquivo
        s3 = boto3.client("s3")
        last_modified = s3.get_object(
            Bucket=s3_bucket,
            Key=landing_obj_key,
        )["LastModified"].isoformat()

        # Gera log de qualidade
        log_qualidade_json = json.dumps(
            {
                "bucket": s3_bucket,
                "tabela": s3_table_key,
                "contas_inconsistentes": contas_com_diff,
                "diferenca_absoluta": round(diff_absoluta, 2),
                "percentual_da_diferenca": round((perc_valor * 100), 2),
                "chave_objeto_s3": landing_obj_key,
                "processado_em": last_modified,
                "criado_em": datetime.now(tz=pytz.utc).isoformat(),
            },
            ensure_ascii=False,
        )

        # Alerta e escreve log no S3
        qualidade_object_key = (
            f"qualidade_dados/posicao_total_por_conta/{s3_partitions_key}/"
            f"{s3_filename}.json"
        )
        s3.put_object(
            Body=log_qualidade_json,
            Bucket=s3_bucket,
            Key=qualidade_object_key,
        )
        msteams_qualidade_dados(log_qualidade_json, webhook)

    ## RAISES
    # Quebra se métricas forem ruins
    if perc_valor > 0.001:
        raise Exception(
            "A diferença entre TotalAmmounts e posições calculadas é maior do que 0,1%"
        )


def posicao_gold(timestamp_dagrun: str):
    """Gera base de Posição na Gold a partir da
    Silver.

    Args:
        timestamp_dagrun (str): Timestamp lógico de execução do pipeline.
    """
    bucket_silver = get_bucket_name("lake-silver")
    bucket_gold = get_bucket_name("lake-gold")

    path_gold = f"s3://{bucket_gold}/btg/posicao/"
    path_silver = f"s3://{bucket_silver}/btg/api/posicao_tratada/"

    ts_parsed = isoparse(timestamp_dagrun)

    # Leitura últimos meses na Silver
    ts_list = [ts_parsed - relativedelta(months=i) for i in range(0, 3)]
    partition_filters = [
        [("ano_particao", "=", str(ts.year)), ("mes_particao", "=", str(ts.month))]
        for ts in ts_list
    ]
    dt_silver = DeltaTable(path_silver)
    df = dt_silver.to_pandas(filters=partition_filters)

    # Filtra última posição de cada dia
    df["timestamp_dagrun_max"] = df.groupby("date")["timestamp_dagrun"].transform("max")
    df = df.loc[df["timestamp_dagrun"] == df["timestamp_dagrun_max"]]
    df = df.drop(columns=["timestamp_dagrun_max"])

    # Transforma conta em numérico
    df["account"] = df["account"].astype("int64").astype("string")
    
    #Dropa a coluna preco_medio
    df = df.drop(['average_price', 'option_type', 'sub_market'], axis=1)

    # Escrita na Gold
    for partitions in partition_filters:
        ano_particao = partitions[0][2]
        mes_particao = partitions[1][2]
        df_iter = df.loc[
            (df["ano_particao"] == ano_particao) & (df["mes_particao"] == mes_particao)
        ]

        write_deltalake(
            path_gold,
            df_iter,
            mode="overwrite",
            partition_by=[part[0] for part in partitions],
            partition_filters=partitions,
        )
        print(f"Overwrite realizado nas partições {partitions} da camada Gold")

        # Compactação da partição
        dt_gold = DeltaTable(path_gold)
        dt_gold.optimize.compact(partition_filters=partitions)
        print("Partição compactada")

        # Vacuum
        dt_gold.vacuum(dry_run=False)
        print("Arquivos antigos apagados")


def posicao_dia_a_dia_gold(timestamp_dagrun: str):
    """Gera base de Posição dia a dia na Gold a partir da
    Posição Gold.

    Args:
        timestamp_dagrun (str): Timestamp lógico de execução do pipeline.
    """
    bucket_gold = get_bucket_name("lake-gold")

    path_posicao = f"s3://{bucket_gold}/btg/posicao/"
    path_posicao_dd = f"s3://{bucket_gold}/btg/posicao_dia_a_dia/"

    ts_parsed = isoparse(timestamp_dagrun)

    # Leitura últimos meses na Gold
    ts_list = [ts_parsed - relativedelta(months=i) for i in range(0, 3)]
    dt_posicao = DeltaTable(path_posicao)
    df = dt_posicao.to_pandas(
        filters=[
            [("ano_particao", "=", str(ts.year)), ("mes_particao", "=", str(ts.month))]
            for ts in ts_list
        ]
    )

    # Agrega Posição por dia
    df = df.groupby("date")["gross_value"].sum().reset_index()

    # Leitura Posição Dia a Dia na Gold
    dt_posicao_dd = DeltaTable(path_posicao_dd)
    df_posicao_dd = dt_posicao_dd.to_pandas()

    # Remove registros que serão sobrescritos
    df_posicao_dd = df_posicao_dd.loc[
        pd.to_datetime(df_posicao_dd["date"]).dt.date
        < pd.Timestamp(ts_list[-1]).replace(day=1).date()
    ]

    # Concatena DataFrames
    df = pd.concat([df_posicao_dd, df]).sort_values("date").reset_index(drop=True)

    # Escrita na Gold
    write_deltalake(
        path_posicao_dd,
        df,
        mode="overwrite",
    )
    print(f"Overwrite realizado na camada Gold")

    # Compactação da partição
    dt_posicao_dd = DeltaTable(path_posicao_dd)
    dt_posicao_dd.optimize.compact()
    print("Partição compactada")

    # Vacuum
    dt_posicao_dd.vacuum(dry_run=False)
    print("Arquivos antigos apagados")


def define_modo_execucao(dt_interval_end: str):
    """Verifica se a DAG deve ser executada no modo de fechamento
    do mês anterior ou no modo de execução normal. Existe um Data
    Interval End específico por mês em que o modo deve ser de
    fechamento

    Args:
        dt_interval_end (str): Data Interval End da Dag Run
        em formato ISO

    Returns:
        str: Task ID da task que se deseja executar
        na sequência
    """

    dt_interval_end_parsed = isoparse(dt_interval_end)
    dt_interval_end_trunc = dt_interval_end_parsed.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    dt_1 = dt_interval_end_trunc.replace(day=1)
    dt_10 = dt_interval_end_trunc.replace(day=10)
    dt_20 = dt_interval_end_trunc.replace(day=20)
    data_fechamento = np.min(
        [i for i in pd.bdate_range(start=dt_1, end=dt_20) if i >= dt_10]
    )

    if (
        data_fechamento == dt_interval_end_trunc
        and dt_interval_end_parsed.hour == 15
        and dt_interval_end_parsed.minute == 30
        and dt_interval_end_parsed.second == 0
    ):
        return "fechamento_mes_anterior"

    return "execucao_normal"


def posicao_auxiliar_receita():
    """Processa e salva a base posicao_aux que é utilizada no processamento
    da base aai. Esta base possui uma relação entre o código de ativos
    e seus emissores. A cada processamento é carregada uma base histórica
    que é atualizada pelas informações mais recentes da base de posição.

    """

    bucket_silver = get_bucket_name("lake-silver")
    bucket_gold = get_bucket_name("lake-gold")

    columns = ["stock", "issuer"]

    data_hoje = datetime.today().date()
    mes_hoje = data_hoje.month
    ano_hoje = data_hoje.year

    df_posicao_aux_historico = pd.read_parquet(
        f"s3://{bucket_silver}/btg/onedrive/receita/posicao_auxiliar/"
    )

    df_posicao_aux_novo = DeltaTable(f"s3://{bucket_gold}/btg/posicao/").to_pandas(
        filters=[
            ("ano_particao", "=", str(ano_hoje)),
            ("mes_particao", "=", str(mes_hoje)),
        ]
    )

    df_posicao_aux_novo = (
        df_posicao_aux_novo.loc[:, columns]
        .rename({"stock": "Ativo", "issuer": "Emissor"}, axis=1)
        .astype({"Ativo": "str", "Emissor": "str"})
    )

    df_posicao_aux_novo["Ativo"] = (
        df_posicao_aux_novo["Ativo"].str.split("-").str[-1].str.split("*").str[-1]
    )

    df = pd.concat([df_posicao_aux_novo, df_posicao_aux_historico])

    df = df.reset_index(drop=True).drop_duplicates("Ativo")

    df.to_parquet(
        f"s3://{bucket_silver}/btg/onedrive/receita/posicao_auxiliar/posicao_auxiliar.parquet"
    )


def normaliza_caracteres_unicode(string: str) -> str:
    """Normaliza caracteres unicode para garantir que operações de junção por campos de texto não falhem.
    Uma boa referência para o tema pode ser encontrada aqui: https://learn.microsoft.com/pt-br/windows/win32/intl/using-unicode-normalization-to-represent-strings

    Args:
        string (str): String a ser normalizada.

    Returns:
        str: String após normalização.
    """
    return (
        unicodedata.normalize("NFKD", string)
        .encode("ASCII", "ignore")
        .decode()
        .strip()
        .replace("/", "_")
        .replace("&", "_")
        .replace(" ", "_")
    )


def auc_dia_dia_completo(timestamp_dagrun: str):
    """Gera base de auc dia dia completo na Gold a partir da posicao Gold e PL Gold
    para extrair offshore.

    Args:
        timestamp_dagrun (str): Timestamp lógico de execução do pipeline.
    """
    bucket_silver = get_bucket_name("lake-silver")
    bucket_gold = get_bucket_name("lake-gold")

    df_pl_offhshore = pd.read_parquet(f"s3a://{bucket_gold}/btg/api/PL/")
    dt_gold_posicao = DeltaTable(f"s3a://{bucket_gold}/btg/posicao")
    df_socios = pd.read_parquet(
        f"s3://{bucket_gold}/api/one/socios/socios.parquet",
        columns=["Nome_bd", "Data início", "Data Saída"],
    )

    df_advisors = pd.read_parquet(
        f"s3a://{bucket_silver}/api/btg/crm/ContasHistorico/",
        columns=[
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
    )

    # Leitura últimos meses na posicao gold e pl gold
    ts_parsed = isoparse(timestamp_dagrun)
    ts_parsed_anterior = ts_parsed - relativedelta(months=1)
    datas_para_atualizacao = [ts_parsed_anterior, ts_parsed]

    for data in datas_para_atualizacao:
        print(data.year, data.month, "iniciou")
        df_auc_dia_dia = dt_gold_posicao.to_pandas(
            filters=[
                ("ano_particao", "=", str(data.year)),
                ("mes_particao", "=", str(data.month)),
            ]
        )
        print(data.year, data.month, "lido")
        df_auc_dia_dia = (
            df_auc_dia_dia.groupby(["account", "market", "date"])["gross_value"]
            .sum()
            .reset_index(drop=False)
        )
        df_auc_dia_dia = df_auc_dia_dia.rename(
            columns={
                "account": "Conta",
                "market": "Mercado",
                "gross_value": "PL",
                "date": "Data",
            }
        )
        df_auc_dia_dia["Data"] = pd.to_datetime(df_auc_dia_dia["Data"])
        df_auc_dia_dia = df_auc_dia_dia.sort_values("Data")

        df_auc_dia_dia.loc[:, "Mercado"] = df_auc_dia_dia["Mercado"].replace(
            "DERIVATIVOS", "RENDA VARIÁVEL"
        )
        # Merge as of
        df_auc_dia_dia_back = pd.merge_asof(
            df_auc_dia_dia, df_advisors, on="Data", by="Conta", direction="backward"
        )
        df_auc_dia_dia_front = pd.merge_asof(
            df_auc_dia_dia, df_advisors, on="Data", by="Conta", direction="forward"
        )
        df_auc_dia_dia = fill_retroativo(df_auc_dia_dia_back, df_auc_dia_dia_front)

        df_auc_dia_dia = df_auc_dia_dia.sort_values("Data").reset_index(drop=True)

        # Caso exista um valor definido para um sócio antes de sua entrada, essse valor é removido
        df_auc_dia_dia = remover_antes_entrada(df_auc_dia_dia, df_socios)

        # Cria uma coluna definindo que a PL deste banco de dados é referente à Oneinvestimentos Assessoria de Investimentos (Classificação 0)
        df_auc_dia_dia["Negócio"] = "AAI"
        df_auc_dia_dia["Participação"] = df_auc_dia_dia["Comercial"]
        df_auc_dia_dia = mergeasof_socios(df_auc_dia_dia, "Comercial", "Data").drop(
            ["Área", "Unidade de Negócios"], axis=1
        )

        # Filtrando offhsore na data da partição
        df_pl_particao_offshore = df_pl_offhshore[
            (df_pl_offhshore["Mercado"] == "Offshore")
            & (df_pl_offhshore["Final do mes"] == True)
            & (
                (df_pl_offhshore["Data"].dt.year == data.year)
                & (df_pl_offhshore["Data"].dt.month == data.month)
            )
        ]

        df_pl_particao_offshore.loc[
            df_pl_particao_offshore["Mercado"] == "Offshore", "Mercado"
        ] = "OFFSHORE"

        df_pl_particao_offshore = df_pl_particao_offshore.loc[
            :,
            [
                "Conta",
                "Mercado",
                "Data",
                "PL",
                "Comercial",
                "Operacional RF",
                "Operacional RV",
                "Grupo familiar",
                "Nome",
                "Id GF",
                "Origem do cliente",
                "Id Conta",
                "Contato/Empresa",
                "Origem 1",
                "Origem 2",
                "Origem 3",
                "Origem 4",
                "Negócio",
                "Participação",
                "Departamento",
            ],
        ].copy()

        # Replicando valor de offshore para todos os dias que temos de posicao onshore
        datas_para_completar = list(df_auc_dia_dia["Data"].unique())
        df_pl_particao_offshore["Data"] = [datas_para_completar] * len(
            df_pl_particao_offshore
        )
        df_pl_particao_offshore = df_pl_particao_offshore.explode("Data")

        # Unindo onshore com offshore
        df_auc_dia_dia_novo = pd.concat([df_auc_dia_dia, df_pl_particao_offshore])

        df_auc_dia_dia_novo["Data"] = pd.to_datetime(df_auc_dia_dia_novo["Data"])
        df_auc_dia_dia_novo["ano_particao"] = df_auc_dia_dia_novo["Data"].dt.year
        df_auc_dia_dia_novo["mes_particao"] = df_auc_dia_dia_novo["Data"].dt.month

        df_auc_dia_dia_novo = df_auc_dia_dia_novo.sort_values("Data")

        df_auc_dia_dia_novo = df_auc_dia_dia_novo.loc[
            :,
            [
                "Conta",
                "Mercado",
                "Data",
                "PL",
                "Comercial",
                "Operacional RF",
                "Operacional RV",
                "Grupo familiar",
                "Nome",
                "Id GF",
                "Origem do cliente",
                "Id Conta",
                "Contato/Empresa",
                "Origem 1",
                "Origem 2",
                "Origem 3",
                "Origem 4",
                "Negócio",
                "Participação",
                "Departamento",
                "ano_particao",
                "mes_particao",
            ],
        ].copy()

        # Salvando na gold
        write_deltalake(
            f"s3a://{bucket_gold}/btg/auc_dia_dia_completo/",
            df_auc_dia_dia_novo,
            mode="overwrite",
            partition_by=["ano_particao", "mes_particao"],
            partition_filters=[
                ("ano_particao", "=", str(data.year)),
                ("mes_particao", "=", str(data.month)),
            ],
        )
        print(data.year, data.month, "atualizado")


def atualiza_auc_contas_crm(timestamp_dagrun: str):
    """Atualiza auc das contas no Hubspot com referencia do pl gold.

    Args:
        timestamp_dagrun (str): Timestamp lógico de execução de atualização.
    """
    bucket_gold = get_bucket_name("lake-gold")

    # Lendo base PL  mais recente
    timestamp_dagrun = pd.to_datetime(timestamp_dagrun).strftime("%Y-%m-%d")

    df_pl = pd.read_parquet(f"s3a://{bucket_gold}/btg/api/PL/PL.parquet")
    print("auc_lido")
    # Data máxima de pl
    max_date = df_pl["Data"].max()
    # Filtrando data máxima de pl
    df_pl = df_pl.loc[df_pl["Data"] == max_date]
    # Agrupando por conta
    df_pl = df_pl.groupby("Conta")["PL"].sum().reset_index()

    # Fazendo requisições no crm para adquirir o id de cada conta
    token = json.loads(get_secret_value("prd/apis/hubspot"))["token"]
    headers = {
        "authorization": f"Bearer {token}",
        "content-type": "application/json",
        "accept": "application/json",
    }
    client = hubspot.Client.create(access_token=token)
    # Definindo objeto accounts para atualização
    account_object_type = {"code": "2-7418328", "name": "accounts"}
    # Lendo todas as contas do crm
    df_contas_crm = client.crm.objects.get_all(
        account_object_type["code"], properties=["account_conta_btg"]
    )

    # Adicionando id(correto) na conta no base pl
    dict_contas_crm = {
        conta.properties["account_conta_btg"].strip(): conta.id
        for conta in df_contas_crm
    }
    df_pl["id"] = df_pl["Conta"].replace(dict_contas_crm)
    print("contas_crm_lidas")

    # Criando filtro para atualização em massa
    filtro_para_atualizazao = []
    for row in df_pl.itertuples():
        filtro_para_atualizazao.append(
            {"pl": row.PL, "id": row.id, "properties": {"pl": row.PL}}
        )
    # Atualizar no crm o PL
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
