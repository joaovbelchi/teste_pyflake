import pandas as pd


def leitura_cayman_landing(path: str) -> pd.DataFrame:
    """Função específica para leitura da base Offshore Cayman salva em
    xlsx.

    Args:
        path (str): URI do objeto no S3

    Returns:
        pd.DataFrame: Base de dados em um DataFrame pandas.
    """

    df = pd.read_excel(path)

    df = df.dropna(subset="Conta")

    return df


def carrega_e_limpa_base_offshore_cy(df_offshore_cy: pd.DataFrame):
    """Recebe a base de dados offshore cy e realiza o processamento e a aplicação
    de regras de negócio.

    Args:
        df_offshore_cy (pd.DataFrame): Objeto do tipo DataFrame com a base de dados
        offshore cy.

    Returns:
        pd.DataFrame:  Objeto do tipo DataFrame com a base de dados
        offshore cy processada.
    """

    # Carrega e trata offshore cayman
    column_types = {
        "Conta": "str",
        "Categoria": "str",
        "Produto": "str",
        "Ativo": "str",
        "Comissão real": "float",
        "Data Receita": "str",
    }

    columns = [
        "Data Receita",
        "Conta",
        "Categoria",
        "Produto",
        "Ativo",
        "Comissão real",
        "atualizacao_incremental",
        "ano_particao",
        "mes_particao",
    ]

    df_offshore_cy = (
        df_offshore_cy.loc[:, columns]
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

    df_offshore_cy["Data"] = pd.to_datetime(df_offshore_cy["Data"], dayfirst=True)

    # Definições de negócio que serão comuns para toda a base de dados.
    df_offshore_cy["Tipo"] = "OFFSHORE"
    df_offshore_cy["Categoria"] = "CAYMAN"
    df_offshore_cy["Intermediador"] = "BTG Pactual"
    df_offshore_cy["Fornecedor"] = "NAO IDENTIFICADO"
    df_offshore_cy["Financeira"] = "BTG Pactual"

    return df_offshore_cy
