import pandas as pd
import json
from dateutil import parser
import pandas_market_calendars as mcal
from datetime import datetime

from utils.OneGraph.drive import OneGraphDrive
from utils.OneGraph.exceptions import DriveItemNotFoundException
from utils.secrets_manager import get_secret_value


def ultimo_dia_util() -> pd.Timestamp:
    """Função para identificar o último dia útil em relação a hoje (D-1)

    Returns:
        pd.Timestamp: último dia útil (D-1)
    """
    # Definir o dia atual
    today = datetime.now().date()
    today_str = today.strftime("%Y-%m-%d")

    # Gerar o caledário da BMF
    bmf_calendar = mcal.get_calendar("BMF")

    # Manter apenas o ano atual
    # Manter o ano atual completo para identificar as semanas
    bmf_calendar = bmf_calendar.schedule(
        start_date=f"{today.year}-01-01",
        end_date=f"{today.year}-12-31",
    )

    bmf_calendar = (
        bmf_calendar[["market_open"]]
        .reset_index(drop=True)
        .rename(columns={"market_open": "Dt_liquidacao"})
    )

    bmf_calendar["Dt_liquidacao"] = bmf_calendar["Dt_liquidacao"].map(
        lambda x: x.strftime("%Y-%m-%d")
    )

    # Identificar o index do dia atual
    idx = bmf_calendar[bmf_calendar["Dt_liquidacao"] == today_str].index[0]

    # Filtrar D-1
    ultimo_dia = bmf_calendar[bmf_calendar.index == idx - 1].iloc[0]["Dt_liquidacao"]

    return pd.to_datetime(ultimo_dia)


def last_modified_date_onedrive(drive_id: str, onedrive_path: str) -> pd.Timestamp:
    """Identificar a última alteração do arquivo no OneDrive

    Args:
        drive_id (str): ID do drive do Onedrive onde reside o arquivo
        onedrive_path (str): caminho em que o arquivo foi salvo no OneDrive

    Returns:
        pd.Timestamp: data da última alteração do arquivo
    """
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    try:
        rsp = drive.get_drive_item_metadata(path=onedrive_path)
    except DriveItemNotFoundException:
        print(f"Nenhum arquivo encontrado para o path '{onedrive_path}'")

    last_modified = parser.parse(rsp.json()["lastModifiedDateTime"])

    return pd.to_datetime(last_modified.date())


def calculo_diferenca(
    df_hoje: pd.DataFrame, df_ontem: pd.DataFrame, group_cols: list
) -> pd.DataFrame:
    """Função para calcular a diferença de valores entre D-1 e hoje

    Args:
        df_hoje (pd.DataFrame): dataframe com a receita estimada hoje
        df_ontem (pd.DataFrame): dataframe com a receita estimada em D-1
        group_cols (list): colunas para agrupar e calcular as diferenças

    Returns:
        pd.DataFrame: dataframe contendo a diferença de valores entre D-1 e hoje
    """
    # Agrupar o dataframe de hoje de acordo com as colunas especificadas
    df_hoje_grouped = df_hoje.groupby(group_cols, as_index=False)["Receita"].sum()

    # Agrupar o dataframe de ontem de acordo com as colunas especificadas
    df_ontem_grouped = df_ontem.groupby(group_cols, as_index=False)["Receita"].sum()

    # Unir os dataframes para identificar as diferenças
    df_grouped = df_hoje_grouped.merge(
        df_ontem_grouped,
        on=group_cols,
        how="outer",
        indicator=True,
        suffixes=("_hoje", "_ontem"),
    )

    # Calcular as diferenças
    df_grouped["Dif"] = df_grouped["Receita_hoje"] - df_grouped["Receita_ontem"]
    df_grouped["% Dif"] = df_grouped["Dif"] / df_grouped["Receita_hoje"]

    return df_grouped


def diferenca_entre_dias(
    df_hoje: pd.DataFrame, df_ontem: pd.DataFrame, lista_cols: list, limite_dif: float
) -> pd.DataFrame:
    """Função para calcular a diferença de estimativa entre D-1 e hoje

    Args:
        df_hoje (pd.DataFrame): dataframe contendo a receita estimada hoje
        df_ontem (pd.DataFrame): dataframe contendo a receita estimada em D-1
        lista_cols (list): lista de colunas para utilizar no groupby
        limite_dif (float): limite "aceitável" de diferença percentual entre os dias

    Returns:
        pd.DataFrame: dataframe contendo as diferenças entre os dias
    """
    df_estimado_result = pd.DataFrame()

    # Iterar sobre cada item da lista de colunas para identificar
    # se há diferença significativa no tipo, categoria, produto, ..., em questão
    for item in range(len(lista_cols)):
        # Separar a lista de colunas até o item atual
        group_cols = lista_cols[: item + 1]

        # Calcular a diferença entre ontem e hoje no item (Tipo, Categoria, Produto, ...)
        df = calculo_diferenca(df_hoje, df_ontem, group_cols)

        # Se a diferença for MENOR do que o limite estabelecido
        # NÃO é necessário detalhar a validação
        df_estimado = df[(df["% Dif"] <= limite_dif)]
        df_estimado_result = pd.concat([df_estimado_result, df])

        # Se a diferença for MAIOR do que o limite estabelecido, menor que zero, ou apareceu apenas ontem
        # é necessário datelhar a validação para os itens que excederam o limite
        df_estimado_verificar = df[
            (df["% Dif"] > limite_dif)
            | (df["% Dif"] < 0)
            | (df["_merge"] == "right_only")
        ]

        # Se não houver nada mais para validar
        if len(df_estimado_verificar) == 0:
            break

        # Caso algum item tenha excedido o limite
        else:
            # Identificar quais são esses itens
            valores_validados = df_estimado[lista_cols[item]].to_list()

            # Se tiver algum item que já foi validado, desconsiderá-lo
            if len(valores_validados) > 0:
                df_hoje = df_hoje[~df_hoje[lista_cols[item]].isin(valores_validados)]
                df_ontem = df_ontem[~df_ontem[lista_cols[item]].isin(valores_validados)]

    df_estimado_result.drop("_merge", axis=1, inplace=True)

    return df_estimado_result


def _color_if_even(s):
    return ["background-color: PeachPuff" if val % 2 == 0 else "" for val in s.index]


def table_style(df: pd.DataFrame) -> str:
    """Função para estilizar e tranformar o dataframe em HTML

    Args:
        df (pd.DataFrame): dataframe que será transformado

    Returns:
        str: HTML referente ao dataframe
    """
    # Definir o estilo do dataframe
    style = [
        {
            "selector": "td, th",
            "props": [
                ("padding", "4px"),
                ("text-align", "left"),
                ("font-size", "13px"),
            ],
        },
        {
            "selector": "th",
            "props": [
                ("background", "#F58220"),
                ("color", "#ffffff"),
                ("text-align", "center"),
            ],
        },
    ]

    # Estilizar e renderizar o dataframe
    tabela = (
        df.reset_index(drop=True)
        .style.hide()
        .set_table_styles(style)
        .set_table_attributes(
            'style="border-collapse: collapse; border: 1px solid lightgrey; margin-left: auto; margin-right: auto;"'
        )
        .apply(_color_if_even, subset=df.columns.tolist())
        .to_html()
    )

    return tabela
