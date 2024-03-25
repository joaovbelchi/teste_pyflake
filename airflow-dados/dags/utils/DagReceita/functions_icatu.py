import pandas as pd
from utils.DagReceita.functions import (
    carrega_e_limpa_base_previdencia,
    carrega_e_limpa_base_guia_previdencia,
)


def carrega_e_limpa_base_icatu(df_icatu: pd.DataFrame = None) -> pd.DataFrame:
    """Recebe a base de dados icatu e realiza o processamento e a aplicação
    de regras de negócio.

    Args:
        df_icatu (pd.DataFrame):  Objeto do tipo DataFrame com a base de dados
        icatu.

    Returns:
        pd.DataFrame: Objeto do tipo DataFrame com a base de dados
        icatu processada.
    """
    columns = [
        "Conta",
        "Comissao Liquida",
        "Data da Posicao",
        "Documento Fundo",
        "Certificado",
        "ano_particao",
        "mes_particao",
        "atualizacao_incremental",
    ]

    column_types = {
        "Conta": "str",
        "Documento Fundo": "str",
        "Certificado": "str",
        "Comissao Liquida": "float",
        "ano_particao": "str",
        "mes_particao": "str",
        "atualizacao_incremental": "str",
    }

    df_icatu = (
        df_icatu.loc[:, columns]
        .astype(column_types)
        .rename(
            columns={
                "Conta Corrente": "Conta",
                "Comissao Liquida": "Comissão",
                "Data da Posicao": "Data",
                "Documento Fundo": "Produto_2",
            },
        )
        .copy()
    )

    df_icatu["Data"] = pd.to_datetime(df_icatu["Data"])

    df_icatu["Produto_2"] = df_icatu["Produto_2"].str.split(".").str[0]
    df_icatu["Certificado"] = (
        df_icatu["Certificado"]
        .str.split(",")
        .str[0]
        .str.split(".")
        .str[0]
        .str.lstrip("0")
    )

    # Carrega e limpa a base previdencia
    df_previdencia = carrega_e_limpa_base_previdencia()

    df_previdencia = df_previdencia.sort_values(
        by="Categoria", na_position="last"
    ).drop_duplicates(subset="Certificado", keep="first")

    # Carrega e limpa a base guia previdencia
    df_guia_previdencia = carrega_e_limpa_base_guia_previdencia()

    # Em um número de certificado terá previdências de apenas uma categoria (PGBL ou VGBL).
    # O trecho abaixo irá extrair da base previdência esta informação que será a
    # coluna Categoria da base.
    df_icatu = df_icatu.merge(
        df_previdencia[["Certificado", "Categoria"]], on=["Certificado"], how="left"
    ).drop("Certificado", axis=1)
    df_icatu["Categoria"] = df_icatu["Categoria"].fillna("NAO IDENTIFICADO")

    # Extrai o fornecedor da base Prev_guide, caso não seja encontrado nesta
    # base o fornecedor será não identificado.
    df_icatu = df_icatu.merge(
        df_guia_previdencia[["CNPJ", "Fornecedor"]],
        left_on="Produto_2",
        right_on="CNPJ",
        how="left",
    ).drop("CNPJ", axis=1)

    df_icatu["Fornecedor"] = (
        df_icatu["Fornecedor"].str.upper().fillna("NAO IDENTIFICADO")
    )

    # Definições de negócio que serão comuns para toda a base de dados.
    df_icatu["Tipo"] = "PREVIDENCIA PRIVADA"
    df_icatu["Intermediador"] = "ICATU"
    df_icatu["Produto_1"] = "ADMINISTRAÇAO"
    df_icatu["Financeira"] = "BTG PACTUAL"

    df_icatu = df_icatu.loc[
        :,
        [
            "Conta",
            "Comissão",
            "Data",
            "Produto_2",
            "ano_particao",
            "mes_particao",
            "atualizacao_incremental",
            "Categoria",
            "Tipo",
            "Intermediador",
            "Produto_1",
            "Fornecedor",
            "Financeira",
        ],
    ]

    return df_icatu
