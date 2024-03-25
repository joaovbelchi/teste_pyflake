import pandas as pd


def carrega_e_limpa_base_extras(df_extras: pd.DataFrame) -> pd.DataFrame:
    """Recebe a base de dados extras e realiza o processamento e a aplicação
    de regras de negócio.

    Args:
        df_extras (pd.DataFrame): Objeto do tipo DataFrame com a base de dados
        extras.

    Returns:
        pd.DataFrame: Objeto do tipo DataFrame com a base de dados
        extras processada.
    """

    column_types = {
        "Conta": "str",
        "Tipo": "str",
        "Categoria": "str",
        "Produto_1": "str",
        "Produto_2": "str",
        "Comissão": "float",
        "Fornecedor": "str",
        "Intermediador": "str",
        "Financeira": "str",
    }

    columns = [
        "Conta",
        "Tipo",
        "Categoria",
        "Produto_1",
        "Produto_2",
        "Comissão",
        "Data",
        "Fornecedor",
        "Intermediador",
        "Financeira",
    ]

    df_extras["Data"] = pd.to_datetime(df_extras["Data"])

    df_extras = df_extras.loc[:, columns].astype(column_types).copy()

    return df_extras
