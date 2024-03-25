import pandas as pd


def carrega_e_limpa_base_ouribank(df_ouribank: pd.DataFrame) -> pd.DataFrame:
    """Recebe a base de dados ouribank e realiza o processamento e a aplicação
    de regras de negócio.

    Args:
        df_ouribank (pd.DataFrame): Objeto do tipo DataFrame com a base de dados
        ouribank.

    Returns:
        pd.DataFrame:  Objeto do tipo DataFrame com a base de dados
        ouribank processada.
    """
    column_names = {
        "Operação": "Categoria",
        "Comissao": "Comissão",
        "Data_liquidacao": "Data",
        "Moeda": "Produto_2",
    }

    # Seleciona as colunas e ajusta os tipos de dados
    df_ouribank = df_ouribank.loc[
        :,
        [
            "Conta",
            "Comissao",
            "Operação",
            "Data_liquidacao",
            "Moeda",
            "ano_particao",
            "mes_particao",
            "atualizacao_incremental",
        ],
    ].rename(column_names, axis=1)

    # Definições de negócio que serão comuns para toda a base de dados.
    df_ouribank["Tipo"] = "CAMBIO"
    df_ouribank["Produto_1"] = "CAMBIO"

    df_ouribank["Fornecedor"] = "OURIBANK"
    df_ouribank["Intermediador"] = "OURIBANK"
    df_ouribank["Financeira"] = "OURIBANK"

    # Renomeia as categorias
    df_ouribank["Categoria"] = (
        df_ouribank["Categoria"]
        .str.upper()
        .replace({"COMPRA": "RECEBIMENTO", "VENDA": "ENVIO"})
    )

    return df_ouribank
