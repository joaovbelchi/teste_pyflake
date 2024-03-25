import pandas as pd
import re
import numpy as np
from datetime import datetime

from utils.bucket_names import get_bucket_name

bucket_lake_gold = get_bucket_name("lake-gold")


def top_picks(drive: str, df_movimentacao: pd.DataFrame) -> pd.DataFrame:
    """Função para identificar ordens de Top Picks, visto que a corretagem é diferente

    Args:
        drive (str): ID do drive do Onedrive onde reside o arquivo
        df_movimentacao (pd.DataFrame): dataframe de movimentações

    Returns:
        pd.DataFrame: dataframe de movimentações identificando Top Picks
    """
    # Ler a base de sócios para identificar os Traders
    df_contrata = pd.read_parquet(f"s3://{bucket_lake_gold}/api/one/socios/")

    # Definir quem são os traders
    operacionais_rv = df_contrata[
        (df_contrata["Área"].astype(str).str.contains("RV"))
        & (df_contrata["sub_area"] != "INTELIG. ESTRATÉGICA")
        & (df_contrata["Data Saída"].isna())
        & (df_contrata["Data início"] <= datetime.now())
    ]["Nome_bd"].to_list()

    path = ["One Analytics", "Inteligência Operacional", "4 - Base - Receita"]

    # Ler o arquivo contendo o histórico de Top Picks de cada trader
    df_top_picks = pd.DataFrame()
    for trader in operacionais_rv:
        path_trader = path + [
            f"Operação - {trader}",
            "Outputs",
            "operacoes_top_picks.xlsx",
        ]

        try:
            df = drive.download_file(path_trader, to_pandas=True)

            df_top_picks = pd.concat([df_top_picks, df])

        except:
            print(f"{trader} ainda não possui pasta")

    # Tratar as colunas
    df_top_picks["Conta"] = df_top_picks["Conta"].astype(str)
    df_top_picks["Data"] = pd.to_datetime(df_top_picks["REGISTRO"].dt.date)

    # Remover registros duplicados
    df_top_picks = df_top_picks.drop_duplicates(
        ["Ativo", "C/V", "Quantidade", "Preço", "Conta", "Validade", "Data"]
    )

    # Ler o arquivo de Net New Money para não mudar a corretagem de forma equivocada
    # visto que não é permitido alterar a corretagem de clientes que trocaram de assessoria nos últimos 6 meses
    path_nnm = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "csv_pbi",
        "Net New Money Detalhado.csv",
    ]

    kwargs = {"sep": ";", "decimal": ","}

    df_nnm = drive.download_file(path_nnm, to_pandas=True, **kwargs)

    # Identificar clientes que trocaram de assessoria (entre escritórios do BTG)
    df_stvm_btg = df_nnm[df_nnm["Tipo"] == "TRANSFERENCIA_ENTRADA"][
        ["Conta", "Data"]
    ].drop_duplicates()

    # Unir histórico de Top Picks com base de clientes que trocaram de assessoria
    df_top_picks = df_top_picks.merge(
        df_stvm_btg, on="Conta", how="left", suffixes=("", "_stvm")
    )

    # Identificar as operações de Top Picks que ocorreram após 6 meses da troca de assessoria
    df_top_picks["Days Dif"] = (
        df_top_picks["Data"] - pd.to_datetime(df_top_picks["Data_stvm"])
    ).dt.days

    df_top_picks = df_top_picks[
        (df_top_picks["Days Dif"].isna()) | (df_top_picks["Days Dif"] > 180)
    ]

    # Unir movimentações com o histórico de Top Picks
    df_movimentacao = df_movimentacao.merge(
        df_top_picks,
        left_on=["account", "stock", "purchase_date"],
        right_on=["Conta", "Ativo", "Data"],
        how="left",
        suffixes=("_mov", "_top_picks"),
    )

    # Mudar o repasse das operações de Top Picks
    df_movimentacao.loc[
        (df_movimentacao["Conta"].notnull()) & (df_movimentacao["Tipo Ordem"].isna()),
        "receita_estimada",
    ] = 0.0035

    # Preencher as colunas
    df_movimentacao.loc[
        (df_movimentacao["Conta"].notnull()) & (df_movimentacao["Tipo Ordem"].isna()),
        "Tipo Ordem",
    ] = "Top Picks"

    return df_movimentacao


def carteiras_recomendadas(drive: str, df_movimentacao: pd.DataFrame) -> pd.DataFrame:
    """Identificar carteiras recomendadas, visto que apesar de não ter a ordem o repasse é como se fosse mesa

    Args:
        drive (str): ID do drive do Onedrive onde reside o arquivo
        df_movimentacao (pd.DataFrame): dataframe de movimentação

    Returns:
        pd.DataFrame: dataframe de movimentação com as carteiras recomendadas indentificadas
    """
    # Ler o arquivos de carteiras recomendadas
    path_carteiras_recomendadas = [
        "One Analytics",
        "Banco de dados",
        "Novos",
        "Planilhas fora dos Novos",
        "carteiras_recomendadas (Mensal).xlsx",
    ]

    df_carteiras_recomendadas = drive.download_file(
        path_carteiras_recomendadas, to_pandas=True
    )

    # Unir base de movimentação com carteiras recomendadas
    df_movimentacao = df_movimentacao.merge(
        df_carteiras_recomendadas,
        left_on="stock",
        right_on="Ativo",
        how="left",
        suffixes=("", "_cart_recomendada"),
    )

    # Identificar quantos ativos da movimentação estão nas carteiras recomendadas
    df_count = df_movimentacao.groupby(
        ["account", "purchase_date", "launch"], as_index=False
    )["Cart. Recomendada"].value_counts()

    # Identificar as movimentações de carteiras recomendadas
    df_movimentacao = df_movimentacao.merge(
        df_count,
        on=["account", "purchase_date", "launch", "Cart. Recomendada"],
        how="left",
    )

    df_movimentacao.loc[
        (df_movimentacao["count"] >= 10), "Tipo Ordem"
    ] = "Carteira Recomendada"

    df_movimentacao = df_movimentacao.sort_values("Tipo Ordem")

    # Remover duplicadas priorizando as linhas que possuem o tipo da ordem,
    # por isso o keep=first após o sort_values
    df_movimentacao = df_movimentacao.drop_duplicates(
        [
            "account",
            "launch",
            "stock",
            "purchase_date",
            "maturity_date",
            "amount",
            "gross_value",
            "ID",
            "receita_estimada",
        ],
        keep="first",
    )

    return df_movimentacao


def estruturadas(drive: str, df_movimentacao: pd.DataFrame) -> pd.DataFrame:
    """Identifcar compra e venda de ativos relacionados a estruturadas

    Args:
        drive (str): ID do drive do Onedrive onde reside o arquivo
        df_movimentacao (pd.DataFrame): dataframe de movimentação

    Returns:
        pd.DataFrame: dataframe de movimentação identificando estruturadas
    """
    # Ler a base de estruturadas do Histórico para identificar as operações
    # Definir o caminho do arquivo
    path_estruturadas = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "dados_estruturadas.csv",
    ]

    kwargs = {"sep": ";", "decimal": ","}

    # Ler o conteúdo do arquivo e transformar em dataframe
    df_estruturadas = drive.download_file(
        path=path_estruturadas, to_pandas=True, **kwargs
    )

    df_estruturadas["Conta"] = df_estruturadas["Conta"].astype(str)
    df_estruturadas["Data de Início"] = pd.to_datetime(
        df_estruturadas["Data de Início"]
    )
    df_estruturadas["Data de Reserva"] = pd.to_datetime(
        df_estruturadas["Data de Reserva"]
    )
    df_estruturadas["Data de Fixing"] = pd.to_datetime(
        df_estruturadas["Data de Fixing"]
    )

    # Identificar operações de compra de ativos relacionado a estruturadas
    # Remover opções duplicadas
    df_estruturadas_compra = df_estruturadas.drop_duplicates(
        ["Conta", "Nome do Produto", "Ativo", "Data de Início"]
    )

    df_movimentacao = df_movimentacao.merge(
        df_estruturadas_compra[["Conta", "Ativo", "Data de Reserva"]],
        left_on=["account", "stock", "purchase_date"],
        right_on=["Conta", "Ativo", "Data de Reserva"],
        how="left",
        suffixes=("", "_estruturadas_compra"),
    )

    df_movimentacao.loc[
        (df_movimentacao["launch"] == "COMPRA")
        & (df_movimentacao["Conta_estruturadas_compra"].notnull()),
        "Tipo Ordem",
    ] = "Estruturada (Compra de Ativo)"

    # Identificar operações de venda de ativos relacionado a estruturadas
    # Remover opções duplicadas
    df_estruturadas_venda = df_estruturadas.drop_duplicates(
        ["Conta", "Nome do Produto", "Ativo", "Data de Fixing"]
    )

    df_movimentacao = df_movimentacao.merge(
        df_estruturadas_venda[["Conta", "Ativo", "Data de Fixing"]],
        left_on=["account", "stock", "purchase_date"],
        right_on=["Conta", "Ativo", "Data de Fixing"],
        how="left",
        suffixes=("", "_estruturadas_venda"),
    )

    df_movimentacao.loc[
        (df_movimentacao["launch"] == "VENDA")
        & (df_movimentacao["Conta_estruturadas_venda"].notnull()),
        "Tipo Ordem",
    ] = "Estruturada (Venda de Ativo)"

    return df_movimentacao


def derivativos(df_ordens: pd.DataFrame, df_movimentacao: pd.DataFrame) -> pd.DataFrame:
    """Identificar movimentações de derivativos

    Args:
        df_ordens (pd.DataFrame): dataframe de ordens
        df_movimentacao (pd.DataFrame): dataframe de movimentações

    Returns:
        pd.DataFrame: dataframe de movimentação identificando derivativos
    """
    # Agrupar a base de ordens
    df_ordens_grouped = df_ordens.groupby(
        [
            "account",
            "stock_aux",
            "launch",
            "Login",
            "purchase_date",
            "Tipo Ordem",
        ],
        as_index=False,
        dropna=False,
    ).agg(
        {
            "Receita": "sum",
            "amount": "sum",
            "amount operação": "sum",
        }
    )

    # Identificar o ativo principal, sem considerar "extensões" do ticker
    df_movimentacao["stock_aux"] = df_movimentacao["stock"].astype(str).str[:4]

    df_ordens_grouped = df_ordens_grouped.sort_values("purchase_date")
    df_movimentacao = df_movimentacao.sort_values("purchase_date")

    # Unir movimentações com ordens
    df_movimentacao = pd.merge_asof(
        df_movimentacao,
        df_ordens_grouped,
        on="purchase_date",
        by=["account", "stock_aux", "launch"],
        direction="forward",
        suffixes=("", "_derivativos"),
        tolerance=pd.Timedelta(days=3),
    )

    # Calcular a receita proporconal, de acordo com o total de ordens na operação
    df_movimentacao["Receita_aux"] = df_movimentacao["Receita_derivativos"] * abs(
        df_movimentacao["amount"] / df_movimentacao["amount operação_derivativos"]
    )

    # Identificar ordens HB
    df_movimentacao.loc[
        (df_movimentacao["account"] == df_movimentacao["Login_derivativos"])
        & (df_movimentacao["Login"].isna())
        & (df_movimentacao["Login_derivativos"].notnull()),
        "Tipo Ordem",
    ] = "HB"

    df_movimentacao.loc[
        (df_movimentacao["account"] == df_movimentacao["Login_derivativos"])
        & (df_movimentacao["Login"].isna())
        & (df_movimentacao["Login_derivativos"].notnull()),
        "Receita",
    ] = df_movimentacao["Receita_aux"]

    return df_movimentacao


def ordens_vac(df_movimentacao: pd.DataFrame, df_ordens: pd.DataFrame) -> pd.DataFrame:
    """Identificar ordens VAC ou VAD, visto que podem estar com datas diferentes nas bases

    Args:
        df_movimentacao (pd.DataFrame): dataframe de movimentações
        df_ordens (pd.DataFrame): dataframe de ordens

    Returns:
        pd.DataFrame: dataframe de movimentação identificando ordens VAC ou VAD
    """
    # Calcular a quantidade acumulada no mês por tipo de ordem
    df_ordens["Quantidade Acumulada"] = df_ordens.groupby(
        ["account", "stock", "launch", "Mês"], as_index=False
    )["amount operação"].cumsum()

    # Identificar o mês da operação
    df_movimentacao["Mês"] = df_movimentacao["purchase_date"].dt.to_period("M")

    # Calcular a quantidade acumulada no mês por tipo de ordem
    df_movimentacao["Quantidade Acumulada"] = df_movimentacao.groupby(
        ["account", "stock", "launch", "Mês"], as_index=False
    )["amount"].cumsum()

    # Ordenar dataframes para o merge_asof
    df_movimentacao = df_movimentacao.sort_values("Quantidade Acumulada")
    df_ordens = df_ordens.sort_values("Quantidade Acumulada")

    df_movimentacao = pd.merge_asof(
        df_movimentacao,
        df_ordens,
        on="Quantidade Acumulada",
        by=["account", "stock", "launch", "Mês"],
        direction="forward",
        suffixes=("", "_ordens"),
    )

    # Se a ordem for VAC ou VAD pode ser executada em dias diferentes,
    # por isso é necessário identificar essas ordens e preencher suas respectivas informações
    df_movimentacao.loc[
        (df_movimentacao["Login"].isna())
        & (df_movimentacao["Validade_ordens"].isin(["VAC", "VAD"])),
        "Tipo Ordem",
    ] = df_movimentacao["Tipo Ordem_ordens"]

    # A receita é calculada
    df_movimentacao.loc[
        (df_movimentacao["Validade_ordens"].isin(["VAC", "VAD"])), "Receita"
    ] = df_movimentacao["Receita_ordens"] * abs(
        df_movimentacao["amount"] / df_movimentacao["amount operação_ordens"]
    )

    return df_movimentacao


def tratamento_corretagem(df: pd.DataFrame) -> pd.DataFrame:
    """Função para tratar a base de corretagem

    Args:
        df (pd.DataFrame): base de corretagem original

    Returns:
        pd.DataFrame: base de corretagem tratada
    """
    # Renomear as colunas
    df.rename({"CONTA_CLIENTE": "account"}, axis=1, inplace=True)

    # Tratar as colunas
    df["account"] = df["account"].astype(str)
    df["receita_estimada"] = df["NOME_GRUPO_CORRETAGEM"].str.extract("HB_([^/]+)_MESA")
    df["receita_estimada"] = df["receita_estimada"].str.split("_").str[-1]
    df["receita_estimada"] = df["receita_estimada"].str.split("%").str[0]

    # Demais formatos da coluna de corretagem
    df.loc[
        (df["NOME_GRUPO_CORRETAGEM"] == "pAdrao")
        | (df["NOME_GRUPO_CORRETAGEM"].isna()),
        "receita_estimada",
    ] = 0.5

    df.loc[
        (df["NOME_GRUPO_CORRETAGEM"].astype(str).str.contains("%MESA")),
        "receita_estimada",
    ] = (
        df["NOME_GRUPO_CORRETAGEM"]
        .str.split("_")
        .str[-1]
        .str.split("%MESA")
        .str[0]
        .str.replace(",", ".")
        .str.strip()
    )

    df["receita_estimada"] = df["receita_estimada"].astype(float)

    df["receita_estimada"] = df["receita_estimada"] / 100

    # Grupo de corretagem para ordens HB
    df["receita_estimada_hb"] = df["NOME_GRUPO_CORRETAGEM"].str.extract("([^/]+)HB")
    df["receita_estimada_hb"] = (
        df["receita_estimada_hb"]
        .str.split("_")
        .str[0]
        .str.split("%")
        .str[0]
        .str.replace(",", ".")
        .str.strip()
        .astype(float)
    )

    return df


def tratamento_ordens(df: pd.DataFrame) -> pd.DataFrame:
    """Função para tratar base de ordens

    Args:
        df (pd.DataFrame): base de ordens original

    Returns:
        pd.DataFrame: base de ordens tratada
    """
    # Tratar as colunas
    df["Login"] = df["Login"].astype(str)
    df["Conta"] = df["Conta"].astype(str)
    df["Qt. Executada"] = df["Qt. Executada"].astype(float)
    df["Ativo"] = df["Ativo"].astype(str)
    df["Direção"] = df["Direção"].astype(str)
    df["Data/Hora"] = pd.to_datetime(df["Data/Hora"].astype(str).str.split(" ").str[0])

    # Selecionar colunas que serão utilizadas
    df = df[
        [
            "Data/Hora",
            "Direção",
            "Login",
            "Conta",
            "Ativo",
            "Status",
            "Qt. Executada",
            "Até a Data",
            "Validade",
            "Plataforma",
        ]
    ]

    # Renomear as colunas
    df.columns = [
        "purchase_date",
        "launch",
        "Login",
        "account",
        "stock",
        "Estado",
        "amount",
        "Até a Data",
        "Validade",
        "Plataforma",
    ]

    df = df[(df["amount"].notnull())]

    # Para não superestimar a receita estou considerando que as ordens SEM login são HB,
    # visto que na minha validação todas realmente eram
    df["Login"] = df["Login"].replace("nan", np.nan)
    df["Login"] = df["Login"].fillna(df["account"])

    # Identficar se o login é um CPF, se for indica que a ordem é HB
    df["login_is_number"] = df["Login"].apply(lambda x: re.findall("\d+", str(x)))
    df["len_login"] = df["login_is_number"].apply(
        lambda x: len(x[0]) if len(x) >= 1 else np.nan
    )

    # Tratar as colunas
    # Se o login possuir 11 números, indica que é uma ordem HB
    df.loc[
        (
            ((df["Login"] == df["account"]) | (df["len_login"] == 11))
            & (df["Plataforma"].astype(str).str.upper() != "SYSAPP")
        )
        | (df["Plataforma"] == "HOMEBROKER"),
        "Tipo Ordem",
    ] = "HB"

    df["launch"] = (
        df["launch"].astype(str).str.upper().replace({"C": "COMPRA", "V": "VENDA"})
    )

    # Identificar o mês da ordem
    df["Mês"] = pd.to_datetime(df["purchase_date"]).dt.to_period("M")

    df = df.reset_index(drop=True)

    # Padronizar o nome dos ativos, caso tenha algum com F no final (fracionário)
    df.loc[df["stock"].str[-1] == "F", "stock"] = (
        df["stock"].str.rsplit("F", n=1).str[0]
    )

    df["stock_aux"] = df["stock"].astype(str).str[:4]

    return df


def receita_estimada_hb(df: pd.DataFrame, df_grouped_day: pd.DataFrame) -> pd.DataFrame:
    """Função para calcular a receita estimada de ordens HB, ou seja, enviadas pelo próprio cliente

    Args:
        df (pd.DataFrame): dataframe contendo as ordens e o tipo de cada uma
        df_grouped_day (pd.DataFrame): dataframe contendo o número de ordens enviadas por dia por cada cliente

    Returns:
        pd.DataFrame: dataframe contendo a estimativa de receita com ordens HB
    """
    filtro = df["Tipo Ordem"] == "HB"

    # Cálculo da Receita gerada por HB
    # Caso tenha realizado mais de 500 ordens no mês
    df.loc[filtro, "Receita Total"] = (
        (4.50 * 10)
        + (4.00 * 30)
        + (3.5 * 35)
        + (1.2 * 425)
        + (0.25 * (df["N° Ordens"] - 500))
    )

    # Caso tenha realizado entre 75 e 500 ordens no mês
    df.loc[
        filtro & (df["N° Ordens"] < 501),
        "Receita Total",
    ] = (
        (4.50 * 10) + (4.00 * 30) + (3.5 * 35) + (1.2 * (df["N° Ordens"] - 75))
    )

    # Caso tenha realizado entre 40 e 75 ordens no mês
    df.loc[
        filtro & (df["N° Ordens"] < 76),
        "Receita Total",
    ] = (
        (4.50 * 10) + (4.00 * 30) + (3.5 * (df["N° Ordens"] - 40))
    )

    # Caso tenha realizado entre 10 e 40 ordens no mês
    df.loc[
        filtro & (df["N° Ordens"] < 41),
        "Receita Total",
    ] = (
        4.50 * 10
    ) + (4.00 * (df["N° Ordens"] - 10))

    # Caso tenha realizado menos de 10 ordens no mês
    df.loc[
        filtro & (df["N° Ordens"] < 11),
        "Receita Total",
    ] = 4.50 * (df["N° Ordens"])

    # Merge para ponderar a receita intraday
    df = df.merge(
        df_grouped_day,
        on=["account", "stock", "launch", "purchase_date"],
        how="left",
    )

    df = df.drop_duplicates(["purchase_date", "account", "launch", "stock"])

    # Ponderar a receita total entre as operações
    df["Receita"] = df["Receita Total"] * (df["N° Ordens Operação"] / df["N° Ordens"])

    df = df.drop(
        [
            "Estado",
            "N° Ordens",
            "amount total",
            "Receita Total",
            "N° Ordens Operação",
        ],
        axis=1,
    )

    return df


def receita_estimada_rv(df: pd.DataFrame) -> pd.DataFrame:
    """Função para calcular a receita estimada com ordens que NÃO são HB, geralmente enviadas pela mesa

    Args:
        df (pd.DataFrame): dataframe contendo as ordens e o tipo de cada uma

    Returns:
        pd.DataFrame: dataframe contendo a receita estimada com as ordens
    """
    # Cálculo da receita estimada
    # Caso a conta não esteja na planilha de corretagem
    df["receita_estimada"] = df["receita_estimada"].fillna(0.005)

    # LFTS11 possui uma corretagem menor
    df.loc[(df["stock"] == "LFTS11"), "receita_estimada"] = 0.001

    df.loc[
        (df["Tipo Ordem"] != "HB") & (df["receita_estimada"] == 0.079),
        "Receita Aux",
    ] = 7.9

    # Receita gerada considerando repasse para a One e impostos
    df["repasse"] = df["receita_estimada"] * 0.85 * 0.9335

    df.loc[df["Tipo Ordem"] != "HB", "Receita"] = abs(df["gross_value"] * df["repasse"])

    df.loc[(df["Tipo Ordem"] != "HB"), "Receita"] = df["Receita"]

    # Receita bruta mínima de mesa é R$ 40.00
    # Calcular a receita bruta
    df["Receita Bruta"] = abs(df["gross_value"] * df["receita_estimada"])

    # Calcular o valor mínimo considerando impostos e repasse
    df.loc[(df["Receita Bruta"] < 40) & (df["Tipo Ordem"] != "HB"), "Receita"] = (
        40.00 * 0.85 * 0.9335
    )

    # Substituir receita de ordens HB de clientes que não pagam corretagem
    df.loc[
        (df["Tipo Ordem"] == "HB") & (df["receita_estimada_hb"] == 0),
        "Receita",
    ] = 0

    return df


def remover_emissoes(df_ipo_raw: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """Função para remover emissões da base de movimentações e evitar que os registros fiquem duplicados

    Args:
        df_ipo_raw (pd.DataFrame): dataframe de emissões
        df (pd.DataFrame): dataframe de movimentações

    Returns:
        pd.DataFrame: dataframe contendo apenas 1 registro por operação
    """
    # Merge para identificar ofertas
    df_ipo_raw["account"] = df_ipo_raw["account"].astype(str)
    df_ipo_raw["liquidation_date"] = pd.to_datetime(df_ipo_raw["liquidation_date"])

    df["liquidation_date"] = pd.to_datetime(df["liquidation_date"])

    df = df.merge(
        df_ipo_raw,
        left_on=["account", "liquidation_date", "gross_value"],
        right_on=["account", "liquidation_date", "value_obtained"],
        how="left",
        suffixes=("", "_ipo"),
        indicator=True,
    )

    # Remover movimentações equivalentes a reservas em ofertas,
    # ou seja, retirar o que está no dataframe de ipo (both ou right_only)
    df = df[df["_merge"] == "left_only"]

    return df
