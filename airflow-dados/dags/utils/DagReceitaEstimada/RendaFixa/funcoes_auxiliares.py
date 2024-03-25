import pandas as pd
from datetime import datetime
import pyarrow.parquet as pq
import pyarrow as pa
import os

from utils.bucket_names import get_bucket_name

from utils.DagReceitaEstimada.funcoes_auxiliares import (
    business_days,
    send_email_outlook,
)

IMPOSTO = 0.9535

# Definir o schema de cada RUN
SCHEMA_CREDITO_BANCARIO = pa.schema(
    [
        ("emissor", "string"),
        ("produto", "string"),
        ("prazo", "float64"),
        ("vencimento", "string"),
        ("indexador", "string"),
        ("taxa_maxima", "float64"),
        ("taxa_sugerida", "float64"),
        ("spread_portal", "float64"),
        ("receita_estimada", "float64"),
        ("aplicacao_minima", "float64"),
        ("rating", "string"),
        ("juros", "string"),
        ("timestamp_dagrun", "string"),
        ("timestamp_escrita_bronze", "string"),
    ]
)

SCHEMA_CREDITO_CORPORATIVO = pa.schema(
    [
        ("emissor", "string"),
        ("setor", "string"),
        ("codigo", "string"),
        ("vencimento", "string"),
        ("rating", "string"),
        ("agencia", "string"),
        ("indexador", "string"),
        ("taxa_maxima", "float64"),
        ("taxa_sugerida", "float64"),
        ("taxa_bruta_equivalente", "string"),
        ("receita_estimada", "float64"),
        ("duration", "float64"),
        ("investidor_qualificado", "string"),
        ("amortizacao", "string"),
        ("juros", "string"),
        ("quantidade_indicativa", "float64"),
        ("relatorio", "string"),
        ("timestamp_dagrun", "string"),
        ("timestamp_escrita_bronze", "string"),
    ]
)

SCHEMA_TITULO_PUBLICO = pa.schema(
    [
        ("vencimento", "string"),
        ("taxa_sugerida", "float64"),
        ("spread_portal", "float64"),
        ("receita_estimada", "float64"),
        ("produto", "string"),
        ("timestamp_dagrun", "string"),
        ("timestamp_escrita_bronze", "string"),
    ]
)


def leitura_run(path: str, schema: pa.lib.Schema) -> pd.DataFrame:
    """Função para ler e tratar o RUN

    Args:
        path (str): caminho do RUN em questão no s3
        schema (pa.lib.Schema): schema do RUN para identificar os tipos

    Returns:
        pd.DataFrame: dataframe com os dados do RUN
    """
    # Definir variaveis para leitura dos arquivos do RUN
    today = datetime.now().date()
    last_month = today - pd.DateOffset(months=1)

    # A base do RUN passou a ser salva no s3 a partir de novembro de 2023
    last_month = last_month if last_month >= datetime(2023, 11, 1) else today

    # Definir parâmetros para ler o RUN
    read_params = {
        last_month.month: last_month.year,
        today.month: today.year,
    }

    bucket_lake_bronze = get_bucket_name("lake-bronze")

    # Ler o RUN para os períodos especificados
    df_result = pd.DataFrame()
    for mes, ano in read_params.items():
        try:
            df = pq.read_table(
                source=f"s3://{bucket_lake_bronze}/{path}/ano_particao={ano}/mes_particao={mes}"
            )

            df = df.cast(schema).to_pandas()

            df["vencimento"] = pd.to_datetime(df["vencimento"])
            df["timestamp_dagrun"] = pd.to_datetime(df["timestamp_dagrun"])

            df_result = pd.concat([df_result, df])
        except:
            pass

    # Remover ativos duplicados
    df_result = df_result.sort_values("timestamp_dagrun")
    df_result = df_result.drop_duplicates()

    return df_result


def titulos_publicos(
    df_movimentacao_raw: pd.DataFrame,
    df_receita_raw: pd.DataFrame,
    max_date: pd.Timestamp,
) -> pd.DataFrame:
    """Calcular a receita estimada a partir das movimentações de títulos públicos

    Args:
        df_movimentacao_raw (pd.DataFrame): dataframe de movimentações
        df_receita_raw (pd.DataFrame): dataframe com o ROA dos produtos
        max_date (pd.Timestamp): data do útlimo relatório de receita contendo Renda Fixa

    Returns:
        pd.DataFrame: dataframe contendo a receita estimada para as movimentações de títulos públicos
    """
    # Filtrar compras de Títulos Púlbicos através do BTG, ou seja, que não foram pelo Tesouro Direto
    df_movimentacao = df_movimentacao_raw[
        (df_movimentacao_raw["issuer"] == "BACEN-BANCO CENTRAL DO BRASIL - RJ")
        & (df_movimentacao_raw["launch"].astype(str).str.contains("COMPRA"))
        & (df_movimentacao_raw["purchase_date"] > max_date)
        & (~df_movimentacao_raw["product"].astype(str).str.contains("TESOURO DIRETO"))
    ]

    # Identificar o mês e ano do vencimento para o merge_asof
    df_movimentacao["mes_vencimento"] = df_movimentacao["maturity_date"].dt.to_period(
        "M"
    )

    # Base de receita
    df_receita_raw["vencimento"] = pd.to_datetime(df_receita_raw["vencimento"])
    df_receita_raw["timestamp_dagrun"] = pd.to_datetime(
        df_receita_raw["timestamp_dagrun"]
    )

    # Filtrar linhas com dados da base de receita
    df_receita = df_receita_raw[df_receita_raw["vencimento"].notnull()]

    # Identificar o mês e ano do vencimento para o merge_asof
    df_receita["mes_vencimento"] = df_receita["vencimento"].dt.to_period("M")

    df_receita["stock"] = (
        df_receita["produto"].astype(str).str.replace("-", "").str.split(" ").str[0]
    )

    # Ordenar os dataframes para o merge_asof
    df_movimentacao = df_movimentacao.sort_values("purchase_date")
    df_receita = df_receita.sort_values("timestamp_dagrun")

    df_movimentacao["purchase_date"] = pd.to_datetime(
        df_movimentacao["purchase_date"].dt.date
    )
    df_receita["timestamp_dagrun"] = pd.to_datetime(
        df_receita["timestamp_dagrun"].dt.date
    )

    # Utilizei o ativo e o mês de vencimento, pois é a informação que vem no RUN
    df_result = pd.merge_asof(
        df_movimentacao,
        df_receita,
        left_on="purchase_date",
        right_on="timestamp_dagrun",
        by=["stock", "mes_vencimento"],
        direction="nearest",
    )

    # Cálculo da Receita estimada
    # Metade One, metade BTG
    df_result["repasse"] = df_result["receita_estimada"] * 0.5 * IMPOSTO
    df_result["Receita"] = df_result["gross_value"] * df_result["repasse"]

    # Preencher as colunas
    # Se o dataframe tiver dados, preencher com valores
    if len(df_result) > 0:
        df_result.loc[(df_result["Receita"].notnull()), "Tipo Receita"] = (
            "Receita Estimada"
        )
        df_result["Tipo"] = "VALORES MOBILIARIOS"
        df_result["Categoria"] = "RENDA FIXA"

    # Caso contrário, apenas criar as colunas
    else:
        df_result["Tipo Receita"] = None
        df_result["Tipo"] = None
        df_result["Categoria"] = None

    return df_result


def credito_corporativo(
    df_movimentacao_raw: pd.DataFrame,
    df_ipo_raw: pd.DataFrame,
    df_receita: pd.DataFrame,
    max_date: pd.Timestamp,
) -> pd.DataFrame:
    """Calcular a receita estimada a partir das movimentações de crédito corporativo

    Args:
        df_movimentacao_raw (pd.DataFrame): dataframe de movimentações
        df_ipo_raw (pd.DataFrame): dataframes com as reservas de emissões
        df_receita (pd.DataFrame): dataframe com o ROA dos produtos
        max_date (pd.Timestamp): data do último relatório de receita contendo Renda Fixa

    Returns:
        pd.DataFrame: dataframe contendo a receita estimada com as movimentações de crédito corporativo
    """
    # Filtrar as compras de crédito corporativo
    df_movimentacao = df_movimentacao_raw[
        (df_movimentacao_raw["product"].isin(["CRA", "CRI", "DEBÊNTURE"]))
        & (df_movimentacao_raw["launch"].astype(str).str.contains("COMPRA"))
        & (df_movimentacao_raw["purchase_date"] > max_date)
    ]

    # Merge para identificar ofertas
    df_ipo_raw["account"] = df_ipo_raw["account"].astype(str)
    df_ipo_raw["liquidation_date"] = pd.to_datetime(df_ipo_raw["liquidation_date"])

    # Identificar operações em comum entre a base de movimentação e de IPO
    df_movimentacao = df_movimentacao.merge(
        df_ipo_raw,
        left_on=["account", "purchase_date", "gross_value"],
        right_on=["account", "liquidation_date", "value_obtained"],
        how="left",
        suffixes=("", "_ipo"),
        indicator=True,
    )

    # Remover movimentações equivalentes a reservas em ofertas,
    # ou seja, retirar o que está no dataframe de ipo (both ou right_only)
    df_movimentacao = df_movimentacao[df_movimentacao["_merge"] == "left_only"]

    # Remover prefixo do nome do ativo
    df_movimentacao["stock"] = (
        df_movimentacao["stock"].astype(str).str.split("-").str[-1]
    )

    # Ordernar o dataframe para o merge_asof
    df_movimentacao = df_movimentacao.sort_values("purchase_date")

    # Base de receita
    df_receita = df_receita.sort_values("timestamp_dagrun")

    df_movimentacao["purchase_date"] = pd.to_datetime(
        df_movimentacao["purchase_date"].dt.date
    )
    df_receita["timestamp_dagrun"] = pd.to_datetime(
        df_receita["timestamp_dagrun"].dt.date
    )

    # merge_asof para identificar o ROA do produto na data da compra
    df_result = pd.merge_asof(
        df_movimentacao,
        df_receita,
        left_on="purchase_date",
        right_on="timestamp_dagrun",
        left_by=["stock"],
        right_by=["codigo"],
        direction="nearest",
    )

    # Padronizar o valor do repasse
    df_result["repasse"] = df_result["receita_estimada"]

    df_result.loc[df_result["repasse"].astype(str).str.contains("de 0"), "repasse"] = (
        df_result["receita_estimada"]
        .astype(str)
        .str.split("de 0")
        .str[-1]
        .str.split("% a.a.")
        .str[0]
    ).astype(float) / 100

    # Calcular a receita estimada
    # Metade One, metade BTG
    df_result["repasse"] = df_result["repasse"] * 0.5 * IMPOSTO
    df_result["Receita"] = df_result["gross_value"] * df_result["repasse"]

    # Preencher as colunas
    df_result.loc[df_result["Receita"].notnull(), "Tipo Receita"] = "Receita Estimada"
    df_result["Tipo"] = "VALORES MOBILIARIOS"
    df_result["Categoria"] = "RENDA FIXA"

    return df_result


def credito_bancario(
    df_movimentacao_raw: pd.DataFrame,
    df_receita: pd.DataFrame,
    df_emissores: pd.DataFrame,
    max_date: datetime,
    **kwargs,
) -> pd.DataFrame:
    """Calcular a receita estimada com as movimentações de crédito bancário

    Args:
        df_movimentacao_raw (pd.DataFrame): dataframe de movimentação
        df_receita (pd.DataFrame): dataframe com o ROA dos produtos
        df_emissores (pd.DataFrame): dataframe com o De-Para dos emissores
        max_date (datetime): data do último relatório de receita contendo Renda Fixa

    Returns:
        pd.DataFrame: dataframe contendo a receita estimada com as movimentações de crédito bancário
    """
    df_emissores["De"] = df_emissores["De"].astype(str).str.upper()
    df_emissores["Para"] = df_emissores["Para"].astype(str).str.strip()

    # Filtrar compras de crédito bancário
    df_movimentacao = df_movimentacao_raw[
        (df_movimentacao_raw["product"].isin(["LCA", "LCI", "CDB", "LF", "LFSN"]))
        & (df_movimentacao_raw["launch"].astype(str).str.contains("COMPRA"))
        & (df_movimentacao_raw["purchase_date"] > max_date)
    ]

    # Remover CDB Liquidez Diária, visto que o cálculo da receita é diferente
    df_movimentacao = df_movimentacao[
        ~(
            (df_movimentacao["issuer"].astype(str).str.contains("BTG"))
            & (df_movimentacao["amount"] > df_movimentacao["gross_value"])
        )
    ]

    # Remover registros duplicados
    # Algumas movimentações possuem 2 registros,
    # e na minha validação todos os registros duplicados possuem a palavra "DEFINITIVA" na coluna "launch".
    # Além disso, o código do ativo (stock) muda de um registro para o outro,
    # por isso não é possível usá-lo no drop_duplicates
    df_movimentacao = df_movimentacao[
        ~(
            (
                df_movimentacao.duplicated(
                    [
                        "account",
                        "product",
                        "issuer",
                        "coupon",
                        "purchase_date",
                        "gross_value",
                    ],
                    keep=False,
                )
            )
            & (df_movimentacao["launch"].astype(str).str.contains("DEFINITIVA"))
        )
    ]

    # Merge para padronizar o nome dos emissores
    df_movimentacao["issuer"] = df_movimentacao["issuer"].astype(str).str.upper()
    df_movimentacao = df_movimentacao.merge(
        df_emissores, left_on="issuer", right_on="De", how="left", indicator=True
    )
    df_movimentacao["Para"] = df_movimentacao["Para"].fillna(df_movimentacao["issuer"])

    # Identificar emissores que ainda não possuem correspondência na base de emissores
    emissores_ausentes = df_movimentacao[df_movimentacao["_merge"] == "left_only"]
    emissores_ausentes = emissores_ausentes.drop_duplicates("issuer")[["issuer"]]
    emissores_ausentes.rename({"issuer": "Emissor"}, axis=1, inplace=True)

    df_movimentacao.drop("_merge", axis=1, inplace=True)

    # Calcular o prazo da operação para juntar com a receita
    df_movimentacao["prazo"] = (
        df_movimentacao["maturity_date"] - df_movimentacao["purchase_date"]
    ).dt.days

    # Base de receita
    # Padronizar o emissor da base de receita de credito bancario
    df_receita["emissor"] = df_receita["emissor"].astype(str).str.upper()
    df_receita = df_receita.merge(
        df_emissores, left_on="emissor", right_on="De", how="left", indicator=True
    )
    df_receita["Para"] = df_receita["Para"].fillna(df_receita["emissor"])

    # Identificar emissores que ainda não possuem correspondência na base de emissores
    emissores_ausentes_run = df_receita[df_receita["_merge"] == "left_only"]
    emissores_ausentes_run = emissores_ausentes_run.drop_duplicates("emissor")[
        ["emissor"]
    ]
    emissores_ausentes_run.rename({"emissor": "Emissor"}, axis=1, inplace=True)

    emissores_ausentes = pd.concat([emissores_ausentes, emissores_ausentes_run])

    # Enviar e-mail caso o dataframe possua um ou mais registros
    if len(emissores_ausentes) > 0:
        path_html = os.path.join(
            os.environ.get("BASE_PATH_DAGS"),
            "utils",
            "DagReceitaEstimada",
            "templates",
        )

        template = open(
            os.path.join(path_html, "emissores_ausentes.html"),
            encoding="utf-8",
        ).read()

        template = template.replace(
            "{EMISSORES_AUSENTES}", emissores_ausentes.to_html(index=False)
        )

        send_email_outlook(template=template, **kwargs)

    # Padronizar os indexadores
    indexadores = {"CDIE": "CDI", "%CDI": "CDI", "CDI+": "CDI"}

    df_movimentacao["indexer"] = df_movimentacao["indexer"].replace(indexadores)
    df_receita["indexador"] = df_receita["indexador"].replace(indexadores)

    # Ordernar os dataframes para o merge_asof
    df_movimentacao = df_movimentacao.sort_values("purchase_date")
    df_receita = df_receita.sort_values("timestamp_dagrun")

    df_movimentacao["purchase_date"] = pd.to_datetime(
        df_movimentacao["purchase_date"].dt.date
    )
    df_receita["timestamp_dagrun"] = pd.to_datetime(
        df_receita["timestamp_dagrun"].dt.date
    )

    # Tratar as colunas para o merge_asof
    df_movimentacao["prazo"] = df_movimentacao["prazo"].astype(float)
    df_receita["prazo"] = df_receita["prazo"].astype(float)

    # Merge_asof para identificar o ROA das movimentações envolvendo credito bancario
    # "Para" refere-se ao nome padronizado do emissor e prazo é o prazo da operação em dias
    df_result = pd.merge_asof(
        df_movimentacao,
        df_receita,
        left_on="purchase_date",
        right_on="timestamp_dagrun",
        left_by=["product", "Para", "indexer", "prazo"],
        right_by=["produto", "Para", "indexador", "prazo"],
        direction="nearest",
    )

    # Separar ativos que não possuem prazo OU possuem receita estimada
    df_com_receita = df_result.loc[
        (df_result["prazo"].isna()) | (df_result["receita_estimada"].notnull())
    ]

    df_com_receita.rename(
        {"receita_estimada": "receita_estimada_x"}, axis=1, inplace=True
    )

    # Ordernar os dataframes para o merge_asof
    df_result = df_result.sort_values("prazo")
    df_receita = df_receita.sort_values("prazo")

    # Separar ativas que possuem prazo e não possuem receita estimada
    temp = df_result.loc[
        (df_result["prazo"].notnull()) & (df_result["receita_estimada"].isna())
    ]

    # Merge_asof para identificar o ROA das movimentações envolvendo credito bancario
    # e que possuem prazos divergentes entre as bases
    # "Para" refere-se ao nome padronizado do emissor e prazo é o prazo da operação em dias
    df_result = pd.merge_asof(
        temp,
        df_receita,
        on="prazo",
        left_by=["product", "Para", "indexer"],
        right_by=["produto", "Para", "indexador"],
        direction="nearest",
    )

    # Unir produtos com e sem prazo
    df_result = pd.concat([df_result, df_com_receita])

    df_result = df_result.reset_index(drop=True)

    # Unificar as colunas de receita estimada
    # Fiz dessa forma, porque é preciso identificar qual a taxa de compra mais próxima ao valor do RUN
    df_result["Dif_x"] = abs((df_result["coupon"] / 100) - df_result["taxa_sugerida_x"])
    df_result["Dif_y"] = abs((df_result["coupon"] / 100) - df_result["taxa_sugerida_y"])

    df_result["Dif_x"] = df_result["Dif_x"].fillna(0)
    df_result["Dif_y"] = df_result["Dif_y"].fillna(0)

    df_result["receita_estimada_x"] = df_result["receita_estimada_x"].fillna(
        df_result["receita_estimada_y"]
    )
    df_result["receita_estimada_y"] = df_result["receita_estimada_y"].fillna(
        df_result["receita_estimada_x"]
    )

    df_result.loc[df_result["Dif_x"] >= df_result["Dif_y"], "receita_estimada"] = (
        df_result["receita_estimada_y"]
    )
    df_result.loc[df_result["Dif_x"] < df_result["Dif_y"], "receita_estimada"] = (
        df_result["receita_estimada_x"]
    )

    # Calcular o número de dias úteis entre a data de movimentação e o fim do mês
    df_result["Periodo"] = df_result["purchase_date"].apply(
        lambda x: business_days(x, final=datetime.now())
    )

    df_result = df_result.reset_index(drop=True)

    # Calcular a receita estimada de credito bancario
    # Ativos emitidos pelo BTG
    df_result["repasse"] = df_result["receita_estimada"] * IMPOSTO

    # Demais emissores
    # Metade One, metade BTG
    df_result.loc[df_result["Para"] != "BANCO BTG PACTUAL", "repasse"] = (
        df_result["receita_estimada"] * 0.5 * IMPOSTO
    )

    df_result["Receita"] = df_result["gross_value"] * df_result["repasse"]

    # Preencher as colunas
    df_result.loc[df_result["Receita"].notnull(), "Tipo Receita"] = "Receita Estimada"
    df_result["Tipo"] = "VALORES MOBILIARIOS"
    df_result["Categoria"] = "RENDA FIXA"

    return df_result


def coe(df_movimentacao_raw: pd.DataFrame, max_date: pd.Timestamp) -> pd.DataFrame:
    """Calcular a receita estimada a partir das movimentações de COE

    Args:
        df_movimentacao_raw (pd.DataFrame): dataframe de movimentação
        max_date (pd.Timestamp): data do último relatório de receita contendo Renda Fixa

    Returns:
        pd.DataFrame: dataframe contendo a receita estimada a partir das movimentações de COE
    """
    # Filtrar compras de COE
    df_movimentacao = df_movimentacao_raw[
        (df_movimentacao_raw["sub_market"] == "COE")
        & (df_movimentacao_raw["launch"].astype(str).str.contains("COMPRA"))
        & (df_movimentacao_raw["purchase_date"] > max_date)
    ]

    # Calcular a receita estimada
    df_movimentacao["repasse"] = 0.055
    df_movimentacao["Receita"] = (
        df_movimentacao["gross_value"] * df_movimentacao["repasse"] * 0.5 * IMPOSTO
    )

    # Preencher as colunas
    df_movimentacao["Tipo Receita"] = "Receita Estimada"
    df_movimentacao["Tipo"] = "VALORES MOBILIARIOS"
    df_movimentacao["Categoria"] = "RENDA FIXA"

    return df_movimentacao


def cdb_liquidez_diaria(df_posicao: pd.DataFrame) -> pd.DataFrame:
    """Calcular a receita estimada com CDBs de Liquidez Diária, visto que esse tipo de produto gera receita diariamente

    Args:
        df_posicao (pd.DataFrame): daaframe de posição

    Returns:
        pd.DataFrame: datarame com a receita estimada dos CDBs de Liquidez Diária
    """
    # Filtrar os CDBs emitidos pelo BTG que possuem Liquidez Diária
    df_result = df_posicao[
        (df_posicao["issuer"].astype(str).str.contains("BTG"))
        & (df_posicao["product"] == "CDB")
        & (df_posicao["amount"] > df_posicao["gross_value"])
    ]

    # Calcular a receita estimada
    df_result["receita_estimada"] = ((1 + 0.00075) ** (1 / 252)) - 1
    df_result["repasse"] = df_result["receita_estimada"] * IMPOSTO
    df_result["Receita"] = df_result["gross_value"] * df_result["repasse"]

    # Renomear as colunas
    df_result.rename({"data_interface": "purchase_date"}, axis=1, inplace=True)

    # Agrupar para manter apenas 1 registro por data por produto
    agg = {
        "Receita": "sum",
        "gross_value": "mean",
        "receita_estimada": "mean",
        "repasse": "mean",
        "amount": "mean",
    }

    df_result = df_result.groupby(
        ["account", "stock", "maturity_date", "issuer", "product", "purchase_date"],
        as_index=False,
    ).agg(agg)

    # Preencher as colunas
    df_result["Tipo Receita"] = "Receita Estimada"
    df_result["Tipo"] = "VALORES MOBILIARIOS"
    df_result["Categoria"] = "RENDA FIXA"

    return df_result


def compromissadas(df_posicao: pd.DataFrame) -> pd.DataFrame:
    """Calcular a receita estimada com compromissadas, visto que esse tipo de produto gera receita diariamente

    Args:
        df_posicao (pd.DataFrame): daaframe de posição

    Returns:
        pd.DataFrame: datarame com a receita estimada dos compromissadas
    """
    # Extrair o CDI histórico
    url_cdi = "http://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json"
    df_cdi = pd.read_json(url_cdi)

    # Tratar as colunas
    df_cdi["data_interface"] = pd.to_datetime(df_cdi["data"], format="%d/%m/%Y")
    df_cdi["CDI"] = df_cdi["valor"] / 100

    df_cdi = df_cdi[["data_interface", "CDI"]]

    # Filtrar apenas compromissadas
    df_posicao = df_posicao[(df_posicao["stock"].astype(str).str.contains("COMP*"))]

    # Unir posição com o histórico do CDI
    df_posicao = df_posicao.merge(df_cdi, on="data_interface", how="left")

    df_result = df_posicao.sort_values("data_interface")
    df_result["CDI"] = df_result["CDI"].ffill()

    # Calcular a estimativa de receita
    df_result["receita_estimada"] = df_result["CDI"] * 0.025
    df_result["repasse"] = df_result["receita_estimada"] * IMPOSTO
    df_result["Receita"] = df_result["gross_value"] * df_result["repasse"]

    # Renomear as colunas
    df_result.rename({"data_interface": "purchase_date"}, axis=1, inplace=True)

    # Agrupar para manter apenas 1 registro por data por período
    agg = {
        "Receita": "sum",
        "gross_value": "mean",
        "receita_estimada": "mean",
        "repasse": "mean",
        "amount": "mean",
    }
    df_result = df_result.groupby(
        ["account", "stock", "maturity_date", "issuer", "product", "purchase_date"],
        as_index=False,
    ).agg(agg)

    # Preencher as colunas
    df_result["Tipo Receita"] = "Receita Estimada"
    df_result["Tipo"] = "VALORES MOBILIARIOS"
    df_result["Categoria"] = "RENDA FIXA"

    return df_result
