import pandas as pd


def carrega_e_limpa_avenue(df_avenue: pd.DataFrame) -> pd.DataFrame:
    """Recebe a base de dados avenue e realiza o processamento e a aplicação
    de regras de negócio.
    Args:
        avenue (pd.DataFrame): Objeto do tipo DataFrame com a base de dados
        avenue.

    Returns:
        pd.DataFrame:  Objeto do tipo DataFrame com a base de dados
        avenue processada.
    """
    df_avenue = df_avenue.loc[
        :,
        [
            "Conta",
            "Data",
            "Comissão",
            "Tipo",
            "ano_particao",
            "mes_particao",
            "atualizacao_incremental",
        ],
    ]

    df_avenue = df_avenue.rename(columns={"Tipo":"Categoria"})
    df_avenue["Categoria"] = df_avenue["Categoria"].str.upper()
    df_avenue["Tipo"] = "CAMBIO"
    df_avenue["Produto_1"] = "CAMBIO"
    df_avenue["Produto_2"] = "CAMBIO"
    df_avenue["Fornecedor"] = "AVENUE"
    df_avenue["Intermediador"] = "AVENUE"
    df_avenue["Financeira"] = "AVENUE"

    return df_avenue
