import pandas as pd
from utils.bucket_names import get_bucket_name


def remover_antes_entrada(df, df_contrata_red):
    for linha in df_contrata_red.itertuples():
        df.loc[
            (df["Comercial"] == linha.Nome_bd)
            & ((df["Data"] > linha._3) | (df["Data"] < linha._2)),
            "Comercial",
        ] = "One Investimentos"
        df.loc[
            (df["Operacional RF"] == linha.Nome_bd)
            & ((df["Data"] > linha._3) | (df["Data"] < linha._2)),
            "Operacional RF",
        ] = "One Investimentos"
        df.loc[
            (df["Operacional RV"] == linha.Nome_bd)
            & ((df["Data"] > linha._3) | (df["Data"] < linha._2)),
            "Operacional RV",
        ] = "Home Broker"
    return df


def fill_retroativo(df, temp=None, use_temp=True, origem=True):
    df["Grupo familiar"] = df.groupby("Conta", dropna=False)["Grupo familiar"].fillna(
        method="bfill"
    )
    df["Grupo familiar"] = df.groupby("Conta", dropna=False)["Grupo familiar"].fillna(
        method="ffill"
    )

    df["Id GF"] = df.groupby("Conta", dropna=False)["Id GF"].fillna(method="bfill")
    df["Id GF"] = df.groupby("Conta", dropna=False)["Id GF"].fillna(method="ffill")

    df["Comercial"] = df.groupby("Conta", dropna=False)["Comercial"].fillna(
        method="bfill"
    )
    df["Comercial"] = df.groupby("Conta", dropna=False)["Comercial"].fillna(
        method="ffill"
    )

    df["Operacional RF"] = df.groupby("Conta", dropna=False)["Operacional RF"].fillna(
        method="bfill"
    )
    df["Operacional RF"] = df.groupby("Conta", dropna=False)["Operacional RF"].fillna(
        method="ffill"
    )

    df["Operacional RV"] = df.groupby("Conta", dropna=False)["Operacional RV"].fillna(
        method="bfill"
    )
    df["Operacional RV"] = df.groupby("Conta", dropna=False)["Operacional RV"].fillna(
        method="ffill"
    )

    df["Nome"] = df.groupby("Conta", dropna=False)["Nome"].fillna(method="bfill")
    df["Nome"] = df.groupby("Conta", dropna=False)["Nome"].fillna(method="ffill")

    if origem == True:
        df["Origem do cliente"] = df.groupby("Conta", dropna=False)[
            "Origem do cliente"
        ].fillna(method="bfill")

        df["Origem do cliente"] = df.groupby("Conta", dropna=False)[
            "Origem do cliente"
        ].fillna(method="ffill")

        df["Origem 1"] = df.groupby("Conta", dropna=False)["Origem 1"].fillna(
            method="bfill"
        )
        df["Origem 1"] = df.groupby("Conta", dropna=False)["Origem 1"].fillna(
            method="ffill"
        )

        df["Origem 2"] = df.groupby("Conta", dropna=False)["Origem 2"].fillna(
            method="bfill"
        )
        df["Origem 2"] = df.groupby("Conta", dropna=False)["Origem 2"].fillna(
            method="ffill"
        )

        df["Origem 3"] = df.groupby("Conta", dropna=False)["Origem 3"].fillna(
            method="bfill"
        )
        df["Origem 3"] = df.groupby("Conta", dropna=False)["Origem 3"].fillna(
            method="ffill"
        )

        df["Origem 4"] = df.groupby("Conta", dropna=False)["Origem 4"].fillna(
            method="bfill"
        )
        df["Origem 4"] = df.groupby("Conta", dropna=False)["Origem 4"].fillna(
            method="ffill"
        )

        if use_temp == True:
            df["Origem do cliente"] = df["Origem do cliente"].fillna(
                temp["Origem do cliente"]
            )
            df["Origem 1"] = df["Origem 1"].fillna(temp["Origem 1"])
            df["Origem 2"] = df["Origem 2"].fillna(temp["Origem 2"])
            df["Origem 3"] = df["Origem 3"].fillna(temp["Origem 3"])
            df["Origem 4"] = df["Origem 4"].fillna(temp["Origem 4"])

        df["Origem do cliente"].fillna("Relacionamento Pessoal", inplace=True)
        df["Origem 1"].fillna("Não identificado", inplace=True)
        df["Origem 2"].fillna("Não identificado", inplace=True)
        df["Origem 3"].fillna("Não identificado", inplace=True)
        df["Origem 4"].fillna("Não identificado", inplace=True)

    if use_temp == True:
        df["Grupo familiar"] = df["Grupo familiar"].fillna(temp["Grupo familiar"])
        df["Comercial"] = df["Comercial"].fillna(temp["Comercial"])
        df["Operacional RF"] = df["Operacional RF"].fillna(temp["Operacional RF"])
        df["Operacional RV"] = df["Operacional RV"].fillna(temp["Operacional RV"])
        df["Id GF"] = df.groupby("Conta", dropna=False)["Id GF"].fillna(temp["Id GF"])
        df["Nome"] = df.groupby("Conta", dropna=False)["Nome"].fillna(temp["Nome"])

    df["Comercial"].fillna("One Investimentos", inplace=True)
    df["Operacional RF"].fillna("One Investimentos", inplace=True)
    df["Operacional RV"].fillna("Home Broker", inplace=True)
    df["Grupo familiar"] = df["Grupo familiar"].fillna(df["Conta"])
    df["Id GF"] = df["Id GF"].fillna("CC_" + df["Conta"])
    df["Nome"] = df["Nome"].fillna("?")
    return df


def mergeasof_socios(
    df: pd.DataFrame,
    name_column: str,
    date_column: str,
    colunas_novas: dict = None,
) -> pd.DataFrame:
    """Esta função preenche de forma histórica a área, departamento e unidade de negócio dos profissionais
    associados à One Investimentos.

    Args:
        df (pd.DataFrame): Data Frame onde se deseja incluir as colunas Área, Departamento e unidade de Negócios dos
        profissionais cadastrados na base sócios da One Investimentos.
        name_column (str): Nome da coluna no Data Frame df onde se encontram os nome dos profissionais da One Investimentos.
        date_column (str): Nome da coluna do Data Frame df onde se encontram as datas dos registros da base df.
        colunas_novas (dict, optional): Dicionário com o nome das colunas que serão incluídas. Este dicionário deverá.
        mapear os seguintes nomes: Área, Unidade de Negócios, Departamento, Team Leader e Business Leader. Apenas as colunas
        aqui selecionadas serão adicionadas ao resutlado desta função. Defaults to None.

    Returns:
        pd.DataFrame: Data Frame com as colunas Área, Unidade de Negócio e Departamento adicionadas.
    """

    colunas_iniciais = df.columns.to_list()

    bucket_lake_silver = get_bucket_name("lake-silver")
    # Carrega a base Socios
    df_socios_silver = pd.read_parquet(
        f"s3://{bucket_lake_silver}/api/one/socios/socios.parquet"
    )

    # Trata alguns erros da base socios
    df_socios_silver["department"] = df_socios_silver["department"].replace(
        {"GROWTH ": "GROWTH"}
    )

    # Para os registros One Investimentos e One Corportate a data de registro é o start date.
    df_socios_silver["register"] = df_socios_silver["register"].fillna(
        df_socios_silver["start_date"]
    )

    # Ordena os valores para o merge asof.
    df_socios_silver = df_socios_silver.sort_values("register")
    df = df.sort_values(date_column)

    # Realiza o merge asof foward e backward
    df_temp = pd.merge_asof(
        df,
        right=df_socios_silver[
            [
                "signature_name",
                "business_unit",
                "department",
                "area",
                "team_leader",
                "business_leader",
                "register",
            ]
        ],
        left_on=date_column,
        right_on="register",
        left_by=name_column,
        right_by="signature_name",
        direction="forward",
    ).drop(["register", "signature_name"], axis=1)

    df = pd.merge_asof(
        df,
        right=df_socios_silver[
            [
                "signature_name",
                "business_unit",
                "department",
                "area",
                "team_leader",
                "business_leader",
                "register",
            ]
        ],
        left_on=date_column,
        right_on="register",
        left_by=name_column,
        right_by="signature_name",
        direction="backward",
    ).drop(["register", "signature_name"], axis=1)

    # Realiza o fill para registros Nans
    df["business_unit"] = df.groupby(name_column, dropna=False)["business_unit"].fillna(
        method="bfill"
    )
    df["business_unit"] = df.groupby(name_column, dropna=False)["business_unit"].fillna(
        method="ffill"
    )
    df["department"] = df.groupby(name_column, dropna=False)["department"].fillna(
        method="bfill"
    )
    df["department"] = df.groupby(name_column, dropna=False)["department"].fillna(
        method="ffill"
    )
    df["area"] = df.groupby(name_column, dropna=False)["area"].fillna(method="bfill")
    df["area"] = df.groupby(name_column, dropna=False)["area"].fillna(method="ffill")

    df["team_leader"] = df.groupby(name_column, dropna=False)["team_leader"].fillna(
        method="bfill"
    )
    df["team_leader"] = df.groupby(name_column, dropna=False)["team_leader"].fillna(
        method="ffill"
    )

    df["business_leader"] = df.groupby(name_column, dropna=False)[
        "business_leader"
    ].fillna(method="bfill")
    df["business_leader"] = df.groupby(name_column, dropna=False)[
        "business_leader"
    ].fillna(method="ffill")

    # Para os casos em que o merge asof backward não encoutro utilizamos o merge asof forward
    df["business_unit"] = df["business_unit"].fillna(df_temp["business_unit"])
    df["department"] = df["department"].fillna(df_temp["department"])
    df["area"] = df["area"].fillna(df_temp["area"])
    df["team_leader"] = df["team_leader"].fillna(df_temp["team_leader"])
    df["business_leader"] = df["business_leader"].fillna(df_temp["business_leader"])

    # Quando o campo Unidade de Negócio ou área não for conhecido preenche com Não identificado
    df["business_unit"] = df["business_unit"].fillna("Não identificado")
    df["area"] = df["area"].fillna("Não identificado")

    # Quando o comercial for One Investimentos o departamento será Growth
    filtro = df[name_column] == "One Investimentos"
    df.loc[filtro, "department"] = "GROWTH"

    # Quando o comercial for Não contatar o departamento será Growth
    filtro = df[name_column] == "Não contatar"
    df.loc[filtro, "department"] = "GROWTH"

    # Quando o comercial for One Corporate o departamento será Corporate
    filtro = df[name_column] == "One Corporate"
    df.loc[filtro, "department"] = "CORPORATE"

    # Quando o campo departamento não for conhecido preenche com Não identificado
    df["department"] = df["department"].fillna("Não identificado")

    # Para o caso específico da comercial abaixo as colunas departamento, area e unidade de operação
    # serão ajustados
    filtro = df[name_column] == "Lara Rates"
    df.loc[filtro, "department"] = "GROWTH"
    df.loc[filtro, "area"] = "COMERCIAL"
    df.loc[filtro, "business_unit"] = "NOVA LIMA"

    filtro = df[name_column] == "Pedro Araújo"
    df.loc[filtro, "department"] = "GROWTH"
    df.loc[filtro, "area"] = "COMERCIAL"

    # Caso a variável colunas_novas esteja preenchida é feito o rename conforme variável
    # e selecionadas apenas as colunas que foram indicadas.
    if colunas_novas is not None:
        colunas_selecionadas = colunas_iniciais + list(colunas_novas.keys())
        df = df.loc[:, colunas_selecionadas].copy()
        df = df.rename(colunas_novas, axis=1)
    else:
        colunas_novas = {
            "department": "Departamento",
            "area": "Área",
            "business_unit": "Unidade de Negócios",
            "business_leader": "Business Leader",
            "team_leader": "Team Leader",
        }
        colunas_selecionadas = colunas_iniciais + list(colunas_novas.keys())
        df = df.loc[:, colunas_selecionadas].copy()
        df = df.rename(
            colunas_novas,
            axis=1,
        )

    return df
