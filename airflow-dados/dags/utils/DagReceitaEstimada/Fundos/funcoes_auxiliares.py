import os
import pandas as pd

from utils.DagReceitaEstimada.funcoes_auxiliares import (
    send_email_outlook,
    real_br_money_mask,
)


def tratamento_fund_guide(df: pd.DataFrame) -> pd.DataFrame:
    """Aplicar tratamentos necessários no fund guide

    Args:
        df (pd.DataFrame): fund guide original

    Returns:
        pd.DataFrame: fund guide após os dataframes
    """
    # Desconsiderar registros sem Margem de Adm
    df = df.replace("-", 0)
    df["Margem Adm"] = df["Margem Adm"].astype(float)

    # Renomear colunas
    df.rename({"Margem Adm": "receita_estimada"}, axis=1, inplace=True)

    # Padronizar o CNPJ
    df["CNPJ"] = (
        df["CNPJ"]
        .astype(str)
        .str.replace(".", "")
        .str.replace("-", "")
        .str.replace("/", "")
    )

    return df


def fundos_sem_margem(df: pd.DataFrame, **kwargs):
    """Enviar e-mail informando sobre fundos que não possuem a informação da Margem de Adminsitração

    Args:
        df (pd.DataFrame): dataframe contendo as posições em fundos
    """
    # Identificar fundos que não possuem a margem de administração preenchida
    df = df[df["_merge"] == "left_only"]

    # Filtrar os registros mais recentes
    df = df[df["date"] == df["date"].max()]

    # Agrupar por fundo e somar o valor alocado
    df = df.groupby(["product", "cnpj"], as_index=False)["gross_value"].sum()

    df = df.sort_values("gross_value", ascending=False)

    # Converter o valor alocado para o padrão brasileiro
    df["gross_value"] = df["gross_value"].apply(lambda x: f"R$ {real_br_money_mask(x)}")

    # Renomear as colunas
    df.rename(
        {"product": "Fundo", "cnpj": "CNPJ", "gross_value": "Valor Total"},
        axis=1,
        inplace=True,
    )

    # Enviar e-mail caso algum fundo não tenha a margem de administração preenchida
    if len(df) > 0:
        path_html = os.path.join(
            os.environ.get("BASE_PATH_DAGS"),
            "utils",
            "DagReceitaEstimada",
            "templates",
        )

        template = open(
            os.path.join(path_html, "fundos_sem_margem.html"),
            encoding="utf-8",
        ).read()

        template = template.replace("{FUNDOS_SEM_MARGEM}", df.to_html(index=False))

        send_email_outlook(template=template, **kwargs)
