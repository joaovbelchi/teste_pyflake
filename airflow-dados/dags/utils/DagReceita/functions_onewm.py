import pandas as pd


def carrega_e_limpa_base_onewm(df_onewm: pd.DataFrame) -> pd.DataFrame:
    """Recebe a base de dados onewm e realiza o processamento e a aplicação
    de regras de negócio.

    Args:
        df_onewm (pd.DataFrame):  Objeto do tipo DataFrame com a base de dados
        onewm.

    Returns:
        pd.DataFrame: Objeto do tipo DataFrame com a base de dados
        onewm processada.
    """

    column_types = {
        "Conta": "str",
        "Categoria": "str",
        "Produto_2": "str",
        "Comissão": "float",
        "Data": "datetime64[ns]",
    }

    df_onewm = (
        df_onewm.loc[:, column_types.keys()]
        .astype(column_types)
        .rename({"Categoria": "Produto_1"}, axis=1)
    )

    # Definições de negócio que serão comuns para toda a base de dados.
    df_onewm["Tipo"] = "VALORES MOBILIARIOS"
    df_onewm["Categoria"] = "FUNDOS GESTORA"
    df_onewm["Intermediador"] = "BTG PACTUAL"
    df_onewm["Fornecedor"] = "ONE WM"
    df_onewm["Financeira"] = "BTG PACTUAL"

    return df_onewm
