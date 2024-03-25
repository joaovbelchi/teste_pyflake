import pandas as pd
import json
from utils.secrets_manager import get_secret_value
from utils.OneGraph.drive import OneGraphDrive
import os
from utils.DagReceita.functions_aai import carrega_e_limpa_base_aai
from utils.DagReceita.functions_corban import carrega_e_limpa_base_corban


def semanal_vazia(drive_id: str, onedrive_path: list) -> str:
    """Esta função verifica se a base de dados semanal está vazia
    e retorna o nome da task que deverá ser iniciada dependendo da
    condição verificada.

    Args:
        drive_id (str): Id do drive de onde a base será carregada
        onedrive_path (list): Lista com os nomes das pastas que precisam
        ser acessadas para obtenção do arquivo que se deseja
        carregar.

    Returns:
        str: Nome da task que deverá ser iniciada após validação
    """

    # Carrega a base semanal do OneDrive.
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)
    df = drive.download_file(path=onedrive_path, to_pandas=True)

    # Realiza a validação.
    if df.empty:
        # Caso a base semanal esteja vazia o trecho abaixo de código
        # irá transformar a base semanal_processada em um DataFrame
        # vazio para que ele não seja incluído na tabela final
        # da receita
        df = pd.DataFrame()
        df.to_parquet(
            "s3://prd-dados-lake-silver-oneinvest/btg/onedrive/receita/semanal_processada/semanal_processada.parquet"
        )
        return "skip_semanal"
    else:
        return "transfer_semanal_landing"


def carrega_e_limpa_base_semanal(df_semanal: pd.DataFrame) -> pd.DataFrame:
    """Esta função limpa e carrega a base semanal

    Args:
        path (str): Caminho para a base semanal.
        query_operacoes (str): Query para extração da base Operações de Câmbio do RDS.
        df_posicao (pd.DataFrame): Data Frame com a base posição tratada.

    Returns:
        pd.DataFrame: Data Frame com a base Semanal tratada.
    """

    column_types = {
        "Conta": "str",
        "Categoria": "str",
        "Produto": "str",
        "Código/CNPJ": "str",
        "Comissão Líquida": "float",
        "Data ajustada": "str",
        "Data Receita": "str",
        "Cliente": "str",
        "Assessor Principal": "str",
        "Ativo": "str",
        "Tipo Receita": "str",
        "Receita Bruta": "float",
        "Receita Líquida": "float",
        "Comissão Bruta": "float",
        "Tipo da receita": "str",
        "Código Produto": "str",
        "Receita": "float",
        "CGE AA": "str",
        "Comissão": "float",
        "Código Assessor": "str",
        "Tipo": "str",
        "Fornecedor": "str",
        "Intermediador": "str",
        "Financeira": "str",
        "Mes": "str",
        "BASE": "str",
        "ano_particao": "str",
        "mes_particao": "str",
    }

    df_semanal = df_semanal.astype(column_types)

    # ICATU
    filtro = df_semanal["BASE"] == "ICATU"
    df_semanal_icatu = df_semanal.loc[
        filtro,
        [
            "Data ajustada",
            "Código/CNPJ",
            "Conta",
            "Comissão Líquida",
            "Categoria",
            "Tipo",
            "Intermediador",
            "Produto",
            "Fornecedor",
            "Financeira",
            "ano_particao",
            "mes_particao",
        ],
    ].copy()
    df_semanal_icatu["Data ajustada"] = pd.to_datetime(
        df_semanal_icatu["Data ajustada"]
    )
    df_semanal_icatu = df_semanal_icatu.rename(
        {
            "Produto": "Produto_1",
            "Código/CNPJ": "Produto_2",
            "Comissão Líquida": "Comissão",
            "Data ajustada": "Data",
        },
        axis=1,
    )

    # PREV BTG
    filtro = df_semanal["BASE"] == "PREV BTG"
    df_semanal_prev_btg = df_semanal.loc[
        filtro,
        [
            "Data ajustada",
            "Código/CNPJ",
            "Conta",
            "Comissão Líquida",
            "Categoria",
            "Tipo",
            "Intermediador",
            "Produto",
            "Fornecedor",
            "Financeira",
            "ano_particao",
            "mes_particao",
        ],
    ].copy()
    df_semanal_prev_btg["Data ajustada"] = pd.to_datetime(
        df_semanal_prev_btg["Data ajustada"]
    )
    df_semanal_prev_btg = df_semanal_prev_btg.rename(
        {
            "Produto": "Produto_1",
            "Código/CNPJ": "Produto_2",
            "Comissão Líquida": "Comissão",
            "Data ajustada": "Data",
        },
        axis=1,
    )

    # FUNDS
    filtro = df_semanal["BASE"] == "FUNDS"
    df_semanal_funds = df_semanal.loc[
        filtro,
        [
            "Data ajustada",
            "Código/CNPJ",
            "Conta",
            "Comissão Líquida",
            "Categoria",
            "Tipo",
            "Intermediador",
            "Produto",
            "Fornecedor",
            "Financeira",
            "ano_particao",
            "mes_particao",
        ],
    ].copy()
    df_semanal_funds["Data ajustada"] = pd.to_datetime(
        df_semanal_funds["Data ajustada"]
    )
    df_semanal_funds = df_semanal_funds.rename(
        {
            "Produto": "Produto_1",
            "Código/CNPJ": "Produto_2",
            "Comissão Líquida": "Comissão",
            "Data ajustada": "Data",
        },
        axis=1,
    )

    # Semanal para CORBAN
    filtro = df_semanal["BASE"] == "CORBAN"
    df_semanal_corban = df_semanal.loc[
        filtro,
        [
            "Conta",
            "Categoria",
            "Comissão Líquida",
            "Tipo Receita",
            "Código/CNPJ",
            "Data ajustada",
            "Ativo",
            "Receita Bruta",
            "ano_particao",
            "mes_particao",
        ],
    ].copy()

    df_semanal_corban = df_semanal_corban.rename(
        {"Comissão Líquida": "Comissão", "Data ajustada": "Data Receita"}, axis=1
    )

    if not df_semanal_corban.empty:
        df_semanal_corban["atualizacao_incremental"] = "Nao"
        df_semanal_corban = carrega_e_limpa_base_corban(
            df_corban=df_semanal_corban, semanal=True
        )
        df_semanal_corban.reset_index(inplace=True, drop=True)
    else:
        # Cria um DF vazio para garantir que as colunas da base semanal sejam sempre iguais.
        df_semanal_corban = pd.DataFrame(
            [],
            columns=[
                "Conta",
                "Categoria",
                "Produto_1",
                "Produto_2",
                "Comissão",
                "Data",
                "Tipo",
                "Fornecedor",
                "Intermediador",
                "Financeira",
                "ano_particao",
                "mes_particao",
            ],
        )

    # Semanal para AAI
    filtro = df_semanal["BASE"] == "AAI"
    df_semanal_aai = df_semanal.loc[
        filtro,
        [
            "Conta",
            "Categoria",
            "Produto",
            "Ativo",
            "Código/CNPJ",
            "Comissão Líquida",
            "Data ajustada",
            "Tipo Receita",
            "ano_particao",
            "mes_particao",
        ],
    ].copy()

    df_semanal_aai = df_semanal_aai.rename(
        {"Comissão Líquida": "Comissão", "Data ajustada": "Ajuste data2"}, axis=1
    )

    if not df_semanal_aai.empty:
        df_semanal_aai["atualizacao_incremental"] = "Nao"
        df_semanal_aai = carrega_e_limpa_base_aai(df_aai=df_semanal_aai)
        df_semanal_aai.reset_index(inplace=True, drop=True)
    else:
        # Cria um DF vazio para garantir que as colunas da base semanal sejam sempre iguais.
        df_semanal_aai = pd.DataFrame(
            [],
            columns=[
                "Conta",
                "Categoria",
                "Produto_1",
                "Produto_2",
                "Comissão",
                "Data",
                "Tipo",
                "Fornecedor",
                "Intermediador",
                "Financeira",
                "ano_particao",
                "mes_particao",
            ],
        )

    df_semanal = pd.concat(
        [
            df_semanal_icatu,
            df_semanal_prev_btg,
            df_semanal_funds,
            df_semanal_aai,
            df_semanal_corban,
        ],
        ignore_index=True,
    )

    df_semanal["Data"] = pd.to_datetime(df_semanal["Data"])
    df_semanal = df_semanal.drop(
        columns=["ano_particao", "mes_particao", "atualizacao_incremental"]
    )

    return df_semanal
