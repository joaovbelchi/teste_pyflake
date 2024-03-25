import pandas as pd


def carrega_e_limpa_base_offshore_us(df_offshore_us: pd.DataFrame):
    """Recebe a base de dados offshore us e realiza o processamento e a aplicação
    de regras de negócio.

    Args:
        df_offshore_us (pd.DataFrame): Objeto do tipo DataFrame com a base de dados
        offshore us.

    Returns:
        pd.DataFrame:  Objeto do tipo DataFrame com a base de dados
        offshore us processada.
    """
    column_types = {
        "Conta": "str",
        "Categoria": "str",
        "Produto": "str",
        "Ativo": "str",
        "Comissão real": "float",
    }

    # Garante que não haverá valores nulos na coluna conta
    df_offshore_us["Conta"] = df_offshore_us["Conta"].fillna("0")

    # Carrega e trata offshore us
    df_offshore_us = (
        df_offshore_us.loc[
            :,
            [
                "Data Receita",
                "Conta",
                "Categoria",
                "Produto",
                "Ativo",
                "Comissão real",
                "atualizacao_incremental",
                "ano_particao",
                "mes_particao",
            ],
        ]
        .astype(column_types)
        .rename(
            {
                "Data Receita": "Data",
                "Conta": "Conta",
                "Categoria": "Categoria",
                "Produto": "Produto_1",
                "Ativo": "Produto_2",
                "Comissão real": "Comissão",
            },
            axis=1,
        )
    )

    # Realiza tratamentos na coluna Conta
    df_offshore_us["Conta"] = df_offshore_us["Conta"].str.upper()

    # Definições de negócio que serão comuns para toda a base de dados.
    df_offshore_us["Tipo"] = "OFFSHORE"
    df_offshore_us["Categoria"] = "US"
    df_offshore_us["Intermediador"] = "BTG Pactual"
    df_offshore_us["Fornecedor"] = "NAO IDENTIFICADO"
    df_offshore_us["Financeira"] = "BTG Pactual"

    return df_offshore_us
