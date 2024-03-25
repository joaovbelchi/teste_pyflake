import boto3
import pandas as pd
import unicodedata
from datetime import datetime
from deltalake import write_deltalake, DeltaTable
from utils.bucket_names import get_bucket_name


def normaliza_caracteres_unicode(string: str) -> str:
    """Normaliza caracteres unicode para garantir que operações de junção por campos de texto não falhem.
    Uma boa referência para o tema pode ser encontrada aqui: https://learn.microsoft.com/pt-br/windows/win32/intl/using-unicode-normalization-to-represent-strings

    Args:
        string (str): String a ser normalizada.

    Returns:
        str: String após normalização.
    """
    return (
        unicodedata.normalize("NFKD", string)
        .encode("ASCII", "ignore")
        .decode()
        .strip()
        .replace("/", "_")
        .replace("&", "_")
        .replace(" ", "_")
        .lower()
    )


def obter_particao_mais_recente(bucket_name: str, prefixo: str) -> datetime:
    """Identificar ano e mês da partição mais recenta da base em questão

    Args:
        bucket_name (str): bucket onde o arquivo está salvo
        prefixo (str): path do arquivo

    Returns:
        datetime: ano e mês da partição mais recente
    """
    # Inicialize o cliente S3
    s3 = boto3.client("s3")

    # Liste os objetos no bucket com o prefixo fornecido
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefixo)

    # Extraia os nomes dos objetos
    objetos = response.get("Contents", [])

    # Se não houver objetos, retorne None
    if not objetos:
        return None

    # Obtenha a string de data da particao mais recente
    data_particao_maxima = [
        (
            objeto["Key"].split("/")[3].split("ano_particao=")[-1],
            objeto["Key"].split("/")[4].split("mes_particao=")[-1],
        )
        for objeto in objetos
    ][-1]

    # Definir como data
    particao_mais_recente = datetime.strptime(
        f"{data_particao_maxima[0]}-{data_particao_maxima[1]}", "%Y-%m"
    )

    return particao_mais_recente


def receita_estimada() -> pd.DataFrame:
    """Ler, padronizar colunas e filtrar base de receita estimada

    Returns:
        pd.DataFrame: dataframe contendo apenas receita estimada
    """
    bucket_lake_gold = get_bucket_name("lake-gold")

    # Ler base de receita estimada mais recente
    df_receita_estimada = pd.read_parquet(
        f"s3://{bucket_lake_gold}/one/receita/estimativa/receita_estimada.parquet"
    )

    # Padronizar nome das colunas
    df_receita_estimada.columns = [
        normaliza_caracteres_unicode(col) for col in df_receita_estimada.columns
    ]

    df_receita_estimada["data"] = pd.to_datetime(df_receita_estimada["data"])

    # Manter apenas receita estimada
    df_receita_estimada = df_receita_estimada.loc[
        df_receita_estimada["tipo_receita"] == "Receita Estimada"
    ]

    return df_receita_estimada


def base_para_roa(
    datas_para_atualizacao: list,
    data_particao_mais_recente: datetime,
    df_receita_completa: pd.DataFrame,
    group_cols: list,
    agg_cols: list,
    select_cols: list,
    path_save: str,
):
    """Gerar base de ROA contendo a Receita e o AuC dia a dia

    Args:
        datas_para_atualizacao (list): data que deverão ser atualizadas
        data_particao_mais_recente (datetime): data da última atualização da base de receita
        df_receita_completa (pd.DataFrame): base de receita
        group_cols (list): colunas que serão utilizadas no groupby
        agg_cols (list): colunas que serão utilizadas na função de agregação do groupby
        select_cols (list): colunas que serão mantidas na dataframe final
        path_save (str): local onde a base será salva
    """
    print("datas_para_atualizacao", datas_para_atualizacao)

    for data in datas_para_atualizacao:
        print(data.year, data.month, "iniciou")

        # Conferir se existe receita no mês em questão
        if data > data_particao_mais_recente:
            print(f"Sem particao de receita na data {data}")

        else:
            df_receita = df_receita_completa.loc[
                (df_receita_completa["data"].dt.year == data.year)
                & (df_receita_completa["data"].dt.month == data.month)
            ]

            # Padronizar a coluna Tipo
            df_receita["tipo"] = df_receita["tipo"].replace(
                "PREVIDENCIA PRIVADA", "PREVIDÊNCIA"
            )

            # Definições de mercado
            # Tirar o que é bônus para não inflar o ROA
            df_receita_grouped_raw = df_receita[
                ~df_receita["categoria"].isin(["BONUS"])
            ]

            # Tudo o que não tem PL, manter o tipo como "NAO POSSUI PL"
            # Definir tipos com PL e categorias sem PL
            tipos_com_pl = ["PREVIDÊNCIA", "VALORES MOBILIARIOS", "OFFSHORE"]
            categorias_sem_pl = ["NAO IDENTIFICADO", "ERRO OPERACIONAL", "CUSTOS"]

            # Definir Mercado para tipos e categorias que não possuem PL
            df_receita_grouped_raw.loc[
                (~df_receita_grouped_raw["tipo"].isin(tipos_com_pl))
                | (
                    (df_receita_grouped_raw["tipo"] == "VALORES MOBILIARIOS")
                    & (df_receita_grouped_raw["categoria"].isin(categorias_sem_pl))
                ),
                "tipo",
            ] = "NAO POSSUI PL"

            # Identificar mercado a partir do tipo e categoria da receita
            # mercado -> [tipo, categorias]
            mercados = {
                "RENDA VARIÁVEL": [
                    "VALORES MOBILIARIOS",
                    ["RENDA VARIAVEL", "CLUBE DE INVESTIMENTO", "DERIVATIVOS"],
                ],
                "CRIPTOATIVOS": ["VALORES MOBILIARIOS", ["CRIPTOMOEDA"]],
                "RENDA FIXA": ["VALORES MOBILIARIOS", ["RENDA FIXA"]],
                "FUNDOS": ["VALORES MOBILIARIOS", ["FUNDOS", "FUNDOS GESTORA"]],
                "CONTA CORRENTE": ["VALORES MOBILIARIOS", ["SALDO REMUNERADO"]],
            }

            for mercado, filtros in mercados.items():
                # Definir tipo e categoria
                tipo = filtros[0]
                categoria = filtros[1]

                # Definir o mercado para o tipo e categoria especificados
                df_receita_grouped_raw.loc[
                    (df_receita_grouped_raw["tipo"] == tipo)
                    & (df_receita_grouped_raw["categoria"].isin(categoria)),
                    "tipo",
                ] = mercado

            df_receita_grouped_raw = df_receita_grouped_raw.rename(
                {"tipo": "mercado"}, axis=1
            )
            df_receita_grouped_raw["negocio"] = "AAI"

            # Agrupar receita por dia
            df_receita_grouped = (
                df_receita_grouped_raw.groupby(
                    group_cols,
                    dropna=False,
                )[agg_cols]
                .sum()
                .reset_index(drop=False)
            )

            bucket_lake_gold = get_bucket_name("lake-gold")

            # Ler df_auc_dia_dia_gold
            dt_auc_dia_dia_gold = DeltaTable(
                f"s3a://{bucket_lake_gold}/btg/auc_dia_dia_completo"
            )
            df_auc_dia_dia_gold = dt_auc_dia_dia_gold.to_pandas(
                filters=[
                    ("ano_particao", "=", int(data.year)),
                    ("mes_particao", "=", int(data.month)),
                ]
            )

            # Padronizar nomes de coluanas
            df_auc_dia_dia_gold = df_auc_dia_dia_gold.rename(
                columns=lambda x: normaliza_caracteres_unicode(
                    x.lower().replace(" ", "_")
                )
            )

            # Criar indicadores nas bases de Receita e AuC
            df_auc_dia_dia_gold["indicador_auc_receita"] = "auc"
            df_receita_grouped["indicador_auc_receita"] = "receita"

            # Concatenar Receita e AuC
            df_base_roa = pd.concat([df_auc_dia_dia_gold, df_receita_grouped])

            # Criar colunas de partição
            df_base_roa["data"] = pd.to_datetime(df_base_roa["data"])
            df_base_roa["ano_particao"] = df_base_roa["data"].dt.year
            df_base_roa["mes_particao"] = df_base_roa["data"].dt.month

            # Selecionar colunas
            df_base_roa = df_base_roa.loc[
                :,
                select_cols,
            ].copy()

            # Preencher nulos nas colunas que contém Receita ou AuC (numéricas)
            colunas_float = df_base_roa.columns[
                df_base_roa.columns.str.lower().str.contains("receita|pl")
            ].tolist()

            for coluna in colunas_float:
                df_base_roa[coluna] = df_base_roa[coluna].fillna(0)

            # Ordenando colunas
            df_base_roa = df_base_roa.sort_values("data").reset_index(drop=True)

            # Salvando na gold
            write_deltalake(
                path_save,
                df_base_roa,
                mode="overwrite",
                partition_by=["ano_particao", "mes_particao"],
                partition_filters=[
                    ("ano_particao", "=", str(data.year)),
                    ("mes_particao", "=", str(data.month)),
                ],
            )

            # Compactação da partição
            df_base_roa = DeltaTable(path_save)
            df_base_roa.optimize.compact()
            print("Partição compactada")

            # Vacuum
            df_base_roa.vacuum(dry_run=False)
            print("Arquivos antigos apagados")
