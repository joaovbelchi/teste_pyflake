import pandas as pd


def padroniza_posicao(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma base de posição nova no schema que
    era utilizado anteriormente.

    Args:
        df (pd.DataFrame): Base de posição nova (camada Gold)

    Returns:
        pd.DataFrame: Base de posição no schema antigo
    """
    df["maturity_date"] = df["maturity_date"].dt.date
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["data_interface"] = df["data_interface"].dt.date

    return df
