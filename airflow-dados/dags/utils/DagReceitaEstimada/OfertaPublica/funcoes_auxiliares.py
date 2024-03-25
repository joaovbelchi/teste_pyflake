import pandas as pd
from datetime import datetime
import os

from utils.DagReceitaEstimada.funcoes_auxiliares import (
    send_email_outlook,
    col_to_datetime,
)


def tratamento_ipo(df: pd.DataFrame) -> pd.DataFrame:
    """Função para tratar a base de IPO

    Args:
        df (pd.DataFrame): base de IPO original (histórico)

    Returns:
        pd.DataFrame: base de IPO tratada
    """
    # Transformar o tipo das colunas
    df["account"] = df["account"].astype(str)
    df["stock"] = df["stock"].astype(str).str.strip()
    df["booking_status"] = df["booking_status"].astype(str).str.upper()

    # Identificar e converter colunas de data
    datetime_columns = [
        "reservation_initial_date",
        "reservation_final_date",
        "liquidation_date",
        "book_building_date",
    ]

    df = col_to_datetime(df, datetime_columns)

    # Filtrar apenas reservas que foram confirmadas
    df = df[(df["booking_status"].isin(["RESERVADA", "LIQUIDADA"]))]

    return df


def ofertas_ausentes(df: pd.DataFrame, max_date: pd.Timestamp, **kwargs):
    """Função para informar sobre ofertas que ainda não estão na planilha ROA - IPO

    Args:
        df (pd.DataFrame): dataframe contendo as emissões
        max_date (pd.Timestamp): data do último relatório de receita contendo emissões
    """
    today = datetime.now()

    # Filtrar emissões que ainda não possuem a informação de ROA
    ofertas_ausentes = df[
        ((df["_merge"] == "left_only") | (df["ROA Bruto"].isna()))
        & (df["liquidation_date"] > max_date)
    ]

    # Remover registros duplicados
    cols = ["stock", "reservation_initial_date", "reservation_final_date"]
    ofertas_ausentes = ofertas_ausentes.drop_duplicates(cols)[cols]

    # Renomear as colunas
    ofertas_ausentes.rename(
        {
            "stock": "Ativo",
            "reservation_initial_date": "Data Inicial de Reserva",
            "reservation_final_date": "Data Final de Reserva",
        },
        axis=1,
        inplace=True,
    )

    # Filtrar ofertas em que a data de liquidação é mais de 30 dias depois de hoje
    conferir_data = df[
        ((df["Data Receita"] - today).dt.days > 30) & (df["Data Receita"].notnull())
    ]

    # Manter apenas 1 registro por reserva
    conferir_data = conferir_data.sort_values("Data Receita")
    conferir_data = conferir_data.drop_duplicates(["stock"], keep="last")[
        ["stock", "Data Receita"]
    ]

    # Renomear as colunas
    conferir_data.rename(
        {"stock": "Ativo"},
        axis=1,
        inplace=True,
    )

    # Enviar e-mail caso algum dos dataframes possua 1 ou mais registros
    if len(ofertas_ausentes) > 0 or len(conferir_data) > 0:
        path_html = os.path.join(
            os.environ.get("BASE_PATH_DAGS"),
            "utils",
            "DagReceitaEstimada",
            "templates",
        )

        template = open(
            os.path.join(path_html, "ofertas_ausentes.html"),
            encoding="utf-8",
        ).read()

        template = template.replace(
            "{OFERTAS_AUSENTES}", ofertas_ausentes.to_html(index=False)
        )

        template = template.replace(
            "{CONFERENCIA_DATA}", conferir_data.to_html(index=False)
        )

        send_email_outlook(template=template, **kwargs)
