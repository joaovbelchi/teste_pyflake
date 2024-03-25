import pandas as pd
import numpy as np
import boto3
import joblib
from deltalake import DeltaTable
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from io import BytesIO
import logging
import time
from utils.bucket_names import get_bucket_name

from sklearn.cluster import KMeans
from sklearn.pipeline import Pipeline
from sklearn.decomposition import PCA
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import MinMaxScaler, PowerTransformer, StandardScaler


# FUNÇÕES DE PROCESSAMENTO #################


class DatesError(Exception):
    pass


def check_datas(
    data_min,
    data_max,
    data_inicio,
    data_base,
):
    if isinstance(data_min, pd.Timestamp):
        data_min = data_min.date()
    if isinstance(data_max, pd.Timestamp):
        data_max = data_max.date()

    check_inicio = (data_min <= data_inicio) and (data_max >= data_inicio)
    check_final = (data_min <= data_base) and (data_max >= data_base)

    erros = []

    if check_inicio == False:
        data_in = data_inicio.strftime("%Y-%m-%d")
        data_min2 = data_min.strftime("%Y-%m-%d")
        data_max2 = data_max.strftime("%Y-%m-%d")
        erros.append(
            f"Data de início ({data_in}) não está entre os dados encontrados, que vão de {data_min2} a {data_max2}"
        )

    if check_final == False:
        data_fim = data_base.strftime("%Y-%m-%d")
        data_min2 = data_min.strftime("%Y-%m-%d")
        data_max2 = data_max.strftime("%Y-%m-%d")
        erros.append(
            f"Data base ({data_fim}) não está entre os dados encontrados, que vão de {data_min2} a {data_max2}"
        )

    if len(erros) > 0:
        raise DatesError("\n".join(erros))


def leitura_base_b3_landing(s3_path: str) -> pd.DataFrame:
    """Esta função carrega as bases de fundos listados
    que estão salvos na landing e realiza alguns tratamentos
    para permitir o carregamento e a definição do schema
    das bases.

    Args:
        s3_path (str): Caminha onde a base está salva

    Returns:
        pd.DataFrame: Base de dados em um objeto DataFrame
    """
    print(s3_path)
    df = (
        pd.read_csv(s3_path, sep=";", encoding="latin1")
        .reset_index()
        .rename(
            columns={
                "index": "razao_social",
                "Razão Social": "fundo",
                "Fundo": "segmento",
                "Segmento": "codigo",
            }
        )
        .drop("Código", axis=1)
    )

    return df


def bronze_2_silver(path_bronze_s3: str, path_silver_s3: str):
    """Esta função carrega as bases de fii, fip e etf da camanda
    bronze e faz a escrita da base na camada silver apenas com a
    data mais recente.

    Args:
        path_bronze_s3 (str): path para a base na camada bronze
        path_silver_s3 (str): path para a base na camada silver
    """

    df = DeltaTable(path_bronze_s3).to_pandas()

    filtro = df["timestamp_escrita_bronze"] == df["timestamp_escrita_bronze"].max()

    df = df.loc[filtro, :]

    df.to_csv(path_silver_s3, sep=";")


def trigger_glue(date_interval_end: str, glue_job: str, ti):
    """Esta função realiza o trigger de um job no AWS Glue.

    Args:
        date_interval_end (str): Data que a DAG foi executada.
        glue_job (str): Nome do Job no Glue.
    """
    client = boto3.client("glue", region_name="us-east-1")

    date_interval_end = date_interval_end.split("T")[0]
    date_interval_end = datetime.strptime(date_interval_end, "%Y%m%d").strftime(
        "%Y-%m-%d"
    )

    run_info = client.start_job_run(
        JobName=glue_job, Arguments={"--date_interval_end": date_interval_end}
    )

    run_id = run_info["JobRunId"]

    key = glue_job + "_run_id"
    ti.xcom_push(key=key, value=run_id)


def processing_aa(date_interval_end: str):
    """Função que realiza o processamento dos dados para previsão do modelo de Asset Allocation.

    Args:
        date_interval_end (str): Data que o pipeline está sendo executado
    """
    bucket_name_gold = get_bucket_name("lake-gold")
    bucket_name_silver = get_bucket_name("lake-silver")

    path_dados_aa = f"s3://{bucket_name_gold}/api/one/asset_allocation/asset_allocation_historico.parquet"
    path_resultados = f"s3://{bucket_name_silver}/ml/clusterizacao_clientes/asset_allocation_processada"
    path_contas = f"s3://{bucket_name_silver}/btg/contas/"

    logging.basicConfig(level=logging.INFO)

    date_interval_end = date_interval_end.split("T")[0]
    data_base = datetime.strptime(date_interval_end, "%Y%m%d").date()
    data_base = data_base - relativedelta(day=1)
    data_base = data_base - relativedelta(days=1)
    data_inicio = data_base - relativedelta(days=30)

    start_time = time.time()

    logging.info(f"[1/5]Definindo data base: {data_base}.")

    df_c = DeltaTable(path_contas).to_pandas(
        partitions=[
            ("date_escrita", ">=", data_inicio.strftime("%Y-%m-%d")),
            ("date_escrita", "<=", data_base.strftime("%Y-%m-%d")),
        ]
    )

    logging.info(f"[2/5]Extração de dados de contas realizada.")

    # Tentar usar lib deltalake

    df_aa = pd.read_parquet(path_dados_aa)

    check_datas(
        data_inicio=data_inicio,
        data_base=data_base,
        data_min=df_aa.Data.min(),
        data_max=df_aa.Data.max(),
    )

    logging.info(f"[3/5]DataFrames carregados.")

    df_c["carteira"] = df_c["carteira"].astype(str)

    contas_pf = list(df_c[df_c["tipo_cliente"] == "PF"]["carteira"])

    df = df_aa[df_aa["Conta"].isin(contas_pf)]

    df_pl = (
        df[
            (df.Data >= pd.to_datetime(data_inicio))
            & (df.Data <= pd.to_datetime(data_base))
        ]
        .groupby("Conta", as_index=False)["PL"]
        .mean()
    )

    # Ajuste 1 - Removendo PL < 50.000 reais #
    # Filtrando contas com PL MAIOR OU IGUAL QUE 50.000 nos últimos 30 dias #
    df_pl = df_pl.loc[df_pl["PL"] >= 50000]

    # Raise se não tiver linhas

    # Pegar uma lista com essas contas do df_pl #
    lista_contas_filtrado = list(df_pl["Conta"])

    df = df[
        ~(df.Categoria == "SALDO") & (df.Conta.isin(lista_contas_filtrado))
    ].reset_index(drop=True)

    df_pivot = (
        pd.pivot_table(
            data=df, index=["Conta", "Data"], columns="Categoria", values="% Alocado"
        )
        .fillna(0)
        .reset_index()
    )

    d = {"Data": ["nunique", "max", "min"]}
    for col in df_pivot.columns:
        if col not in ["Conta", "Data"]:
            d[col] = [np.mean]

    df_group = (
        df_pivot[
            (df_pivot["Data"] >= pd.to_datetime(data_inicio))
            & (df_pivot["Data"] <= pd.to_datetime(data_base))
        ]
        .groupby("Conta", as_index=False)
        .agg(d)
    )

    # Renomeando as colunas
    new_cols = []
    for col in df_group.columns:
        col1, col2 = col
        if col2 == "":
            new_cols.append(col1)
        else:
            new_cols.append(f"{col1}_{col2}")

    df_group.columns = new_cols

    df_group = df_group[
        (df_group.Data_max == df_group.Data_max.max())
        & (df_group.Data_nunique == df_group.Data_nunique.max())
    ].reset_index(drop=True)

    logging.info(
        f"[4/5]Manipulações realizadas. {df_group['Conta'].nunique()} Contas únicas restantes."
    )

    ano = data_base.year
    mes = data_base.month
    dia = data_base.day

    df_group.to_parquet(f"{path_resultados}/{ano}/{mes}/{ano}_{mes}_{dia}.parquet")

    end_time = time.time()

    execution_time = end_time - start_time

    logging.info(
        f"[5/5]Resultados salvos. Tempo de execução: {execution_time:.2f} segundos"
    )


# FUNÇÕES DE PREDIÇÃO #######################


def categorizar(x):
    """Essa função é aplicadas nas linhas dos dados de posição para criar a
    coluna categoria a partir de 'market', 'classe' e 'tipo'.
    """
    if x["tipo"] != "N/A":
        return x["market"] + " - " + x["classe"] + " - " + x["tipo"]
    elif x["classe"] != "N/A":
        return x["market"] + " - " + x["classe"]
    else:
        return x["market"]


def predict_movimentacoes(date_interval_end: str = None):
    """Esta função aplica o modelo de segmentação de movimentações
    nos dados da data passada nos argumentos, posteriormente salva
    os resultados em formato parquet no S3.

    Args:
        date_interval_end (str): data base usada para o treinamento,
            vai determinar o path de onde serão puxados os dados.
    """

    bucket_name_gold = get_bucket_name("lake-gold")
    bucket_name_silver = get_bucket_name("lake-silver")
    bucket = get_bucket_name("modelos-ml")

    # Paths a serem utilizados
    path_dados_mov = (
        f"s3://{bucket_name_silver}/ml/clusterizacao_clientes/movimentacao_processada"
    )
    path_modelo_mov = "clusterizacao_clientes/Movimentacoes/Current/model.pkl"
    path_resultados_mov = f"s3://{bucket_name_gold}/ml/clusterizacao_clientes/movimentacao/predict_mov.parquet"

    # Nomes dos clusters
    cluster_nomes = {
        "Movimentações": {
            0: "Cliente Previdência",
            1: "Passivo RV",
            2: "Equilibrado",
            3: "Ativo RV",
            4: "Passivo RF",
            5: "Ativo Fundos",
            "Dormente": "Dormente",
        }
    }

    # Determinação da data a ser usada
    date_interval_end = date_interval_end.split("T")[0]
    data_base = datetime.strptime(date_interval_end, "%Y%m%d").date()
    data_base = data_base - relativedelta(day=1)
    data = data_base - relativedelta(days=1)

    # Lê os dados salvos da data assinalada.
    # É necessário realizar o processo de processamento na data requerida para que esse arquivo exista.
    df = pd.read_parquet(
        f"{path_dados_mov}/{data.year}/{data.month}/{data.year}_{data.month}_{data.day}.parquet"
    )

    # Lê o modelo vigente (salvo na pasta Current)
    with BytesIO() as f:
        boto3.client("s3").download_fileobj(
            Bucket=bucket, Key=path_modelo_mov, Fileobj=f
        )
        f.seek(0)
        modelo_mov = joblib.load(f)

    # Manipulações
    df["soma_ativa"] = df["soma_ativa"] / df["pl"]
    df["soma_passiva"] = df["soma_passiva"] / df["pl"]
    df = df.reset_index(drop=True)

    # Deixa os dados no formato necessário para aplicação do modelo
    df_modelm = df.copy()
    df_modelm.index = df["account"]
    df_modelm = df_modelm.drop(columns="account")
    for col in df_modelm.columns:
        if "frequencia" in col:
            if "geral" not in col:
                df_modelm[col] = df_modelm[col] / df_modelm["frequencia_geral"]

    # Remove colunas que não serão usadas
    df_modelm = df_modelm.drop(columns=["idade_conta", "pl", "date"])

    # Remove contas que tiveram menos de 5 movimentações no período
    df_modelm2 = df_modelm[df_modelm.frequencia_geral >= 5]

    # Realiza as predições do modelo
    df_resultsm = pd.DataFrame(
        modelo_mov.predict(df_modelm2), index=df_modelm2.index, columns=["Cluster"]
    )

    # Junta as predições com as contas previamente eliminadas
    df_modelm = df_modelm.merge(
        df_resultsm["Cluster"], how="left", left_index=True, right_index=True
    )

    # Dá nome aos clusters
    df_modelm["Cluster"] = df_modelm["Cluster"].fillna("Dormente")
    df_modelm["Cluster"] = df_modelm["Cluster"].apply(
        lambda x: cluster_nomes["Movimentações"][x]
    )

    # Cria coluna de datas
    df_modelm["Data"] = data
    df_modelm["Data"] = df_modelm["Data"].astype(str)

    # Lê dados de resultados, anexa os resultados da função, remove duplicatas e salva
    df_final = pd.read_parquet(path_resultados_mov)
    df_final = pd.concat([df_final, df_modelm])
    df_reset = df_final.reset_index()
    df_unique = df_reset[~df_reset.duplicated(subset=["account", "Data"], keep="last")]
    df_unique.set_index("account", inplace=True)
    df_unique.to_parquet(path_resultados_mov)


def predict_nnm(date_interval_end: str = None):
    """Esta função aplica o modelo de segmentação de Net New Money
    nos dados da data passada nos argumentos, posteriormente salva
    os resultados em formato parquet no S3.

    Args:
        date_interval_end (str): data base usada para o treinamento,
            vai determinar o path de onde serão puxados os dados.
    """

    bucket_name_gold = get_bucket_name("lake-gold")
    bucket_name_silver = get_bucket_name("lake-silver")

    # Paths a serem utilizados
    bucket = get_bucket_name("modelos-ml")
    path_dados_nnm = (
        f"s3://{bucket_name_silver}/ml/clusterizacao_clientes/nnm_processada"
    )
    path_modelo_nnm = "clusterizacao_clientes/NNM/Current/model.pkl"
    path_resultados_nnm = (
        f"s3://{bucket_name_gold}/ml/clusterizacao_clientes/nnm/predict_nnm.parquet"
    )

    # Nomes dos clusters
    cluster_nomes = {
        "NNM": {
            0: "Alta Frequência Tendência Positiva",
            1: "Baixa Frequência Tendência Positiva",
            2: "Baixa Frequência Tendência Negativa",
            3: "Altos Aportes",
            4: "Alta Frequência Tendência Negativa",
            5: "Altas Retiradas",
            "Dormente": "Dormente",
        }
    }

    # Determinação da data a ser usada
    date_interval_end = date_interval_end.split("T")[0]
    data_base = datetime.strptime(date_interval_end, "%Y%m%d").date()
    data_base = data_base - relativedelta(day=1)
    data = data_base - relativedelta(days=1)

    # Lê os dados salvos da data assinalada.
    # É necessário realizar o processo de processamento na data requerida para que esse arquivo exista.
    df = pd.read_parquet(
        f"{path_dados_nnm}/{data.year}/{data.month}/{data.year}_{data.month}_{data.day}.parquet"
    )

    # Lê o modelo vigente (salvo na pasta Current)
    with BytesIO() as f:
        boto3.client("s3").download_fileobj(
            Bucket=bucket, Key=path_modelo_nnm, Fileobj=f
        )
        f.seek(0)
        modelo_nnm = joblib.load(f)

    # Manipulações
    df["soma_valor_nnm"] = df["soma_valor_nnm"] / df["pl"]
    df = df.reset_index(drop=True)

    # Deixa os dados no formato necessário para aplicação do modelo
    df_modeln = df.copy()
    df_modeln.index = df["account"]
    df_modeln = df_modeln.drop(columns="account")
    for col in df_modeln.columns:
        if "frequencia" in col:
            if "nnm" not in col:
                df_modeln[col] = df_modeln[col] / df_modeln["frequencia_nnm"]

    # Remove colunas que não serão utilizadas
    df_modeln = df_modeln.drop(
        columns=["idade_conta", "pl", "date", "mediana_valor_nnm"]
    )

    # Remove contas com menos 3 movimentações no período
    df_modeln2 = df_modeln[df_modeln.frequencia_nnm >= 3]

    # Realiza as predições do modelo
    df_resultsn = pd.DataFrame(
        modelo_nnm.predict(df_modeln2), index=df_modeln2.index, columns=["Cluster"]
    )

    # Junta as predições com as contas previamente eliminadas
    df_modeln = df_modeln.merge(
        df_resultsn["Cluster"], how="left", left_index=True, right_index=True
    )

    # Dá nome aos clusters
    df_modeln["Cluster"] = df_modeln["Cluster"].fillna("Dormente")
    df_modeln["Cluster"] = df_modeln["Cluster"].apply(lambda x: cluster_nomes["NNM"][x])

    # Cria coluna de datas
    df_modeln["Data"] = data
    df_modeln["Data"] = df_modeln["Data"].astype(str)

    # Lê dados de resultados, anexa os resultados da função, remove duplicatas e salva
    df_final = pd.read_parquet(path_resultados_nnm)
    df_final = pd.concat([df_final, df_modeln])
    df_reset = df_final.reset_index()
    df_unique = df_reset[~df_reset.duplicated(subset=["account", "Data"], keep="last")]
    df_unique.set_index("account", inplace=True)
    df_unique.to_parquet(path_resultados_nnm)


def predict_aa(date_interval_end: str = None):
    """Esta função aplica o modelo de segmentação de Asset Allocation
    nos dados da data passada nos argumentos, posteriormente salva
    os resultados em formato parquet no S3.

    Args:
        date_interval_end (str): data base usada para o treinamento,
            vai determinar o path de onde serão puxados os dados.
    """

    bucket_name_gold = get_bucket_name("lake-gold")
    bucket_name_silver = get_bucket_name("lake-silver")

    # Paths a serem utilizados
    bucket = get_bucket_name("modelos-ml")
    path_dados_aa = f"s3://{bucket_name_silver}/ml/clusterizacao_clientes/asset_allocation_processada"
    path_dados_posicao = (
        f"s3://{bucket_name_silver}/ml/clusterizacao_clientes/posicao_processada"
    )
    path_modelo_aa = "clusterizacao_clientes/AA/Current/model.pkl"
    path_resultados_aa = f"s3://{bucket_name_gold}/ml/clusterizacao_clientes/asset_allocation/predict_aa.parquet"
    path_modelo_pos = "clusterizacao_clientes/Posicao/Current/model.pkl"

    # Nomes dos clusters
    sub_modelos = {
        "Investidor de Bolsa": {
            "Nome": "RV",
            "Num_clusters": 3,
            "Cols": ["RENDA VARIÁVEL", "RV BR"],
            "Cluster": {
                0: "Superconcentrado - Ações BR",
                1: "Diversificado - RV",
                2: "Concentrado - Ações BR",
                "Não se aplica": "Não se aplica",
            },
        },
        "Investidor de Juros": {
            "Nome": "Juros",
            "Num_clusters": 4,
            "Cols": ["PÓS", "PRÉ", "CAIXA"],
            "Cluster": {
                0: "Diversificado - Pós",
                1: "Equilibrado - Pós",
                2: "Concentrado - Fundos Pós",
                3: "Concentrado - Pós Bancário",
                "Não se aplica": "Não se aplica",
            },
        },
        "Investidor de Inflação": {
            "Nome": "Inflacao",
            "Num_clusters": 3,
            "Cols": ["IPCA"],
            "Cluster": {
                0: "Diversificado - IPCA",
                1: "Concentrado - IPCA Corporativo",
                2: "Superconcentrado - IPCA Corporativo",
                "Não se aplica": "Não se aplica",
            },
        },
        "Investidor de Alta Liquidez": {
            "Nome": "Caixa",
            "Num_clusters": 3,
            "Cols": ["CAIXA"],
            "Cluster": {
                0: "Concentrado - Fundos Caixa",
                1: "Concentrado - Ativos Bancários",
                2: "Superconcentrado - Fundos Caixa",
                "Não se aplica": "Não se aplica",
            },
        },
    }

    cluster_nomes = {
        "AA": {
            0: "Investidor de Bolsa",
            1: "Investidor de Juros",
            2: "Investidor Balanceado",
            3: "Investidor Multimercados",
            4: "Investidor de Alta Liquidez",
            5: "Investidor de Inflação",
        }
    }

    # Determinação da data a ser usada
    date_interval_end = date_interval_end.split("T")[0]
    data_base = datetime.strptime(date_interval_end, "%Y%m%d").date()
    data_base = data_base - relativedelta(day=1)
    data = data_base - relativedelta(days=1)

    # Lê os dados salvos da data assinalada.
    # É necessário realizar o processo de processamento na data requerida para que esse arquivo exista.
    df = pd.read_parquet(
        f"{path_dados_aa}/{data.year}/{data.month}/{data.year}_{data.month}_{data.day}.parquet"
    )

    # Retirando colunas que não serão utilizadas
    df_model = df[
        [
            col
            for col in df.columns
            if "Data" not in col and "Conta" not in col and "mean" in col
        ]
    ]
    for col in ["AÇÕES_mean", "MULTIMERCADOS_mean", "RENDA FIXA_mean", "FIDC_mean"]:
        try:
            df_model = df_model.drop(columns=col)
        except KeyError:
            pass

    # Coloca os dados no formato necessário
    df_model.index = df["Conta"]
    df_model.head()
    df_model = df_model / df_model.sum(axis=1).values.reshape(-1, 1)

    # Lê modelo salvo
    with BytesIO() as f:
        boto3.client("s3").download_fileobj(
            Bucket=bucket, Key=path_modelo_aa, Fileobj=f
        )
        f.seek(0)
        model = joblib.load(f)

    # Realiza as predições do modelo
    df_results = pd.DataFrame(
        model.predict(df_model), index=df_model.index, columns=["Cluster"]
    )
    df_results = df_model.merge(
        df_results["Cluster"], how="left", left_index=True, right_index=True
    )

    # Nomea os clusters
    df_results["Cluster"] = df_results["Cluster"].apply(
        lambda x: cluster_nomes["AA"][x]
    )

    # Lê dados de posição para a data
    df_posicao = pd.read_parquet(
        f"{path_dados_posicao}/{data.year}/{data.month}/{data.year}_{data.month}_{data.day}.parquet"
    )

    # Cria coluna categoria para dados de posição
    df_posicao["categoria"] = df_posicao.apply(lambda x: categorizar(x), axis=1)

    # Manipulações e remoção de coluna
    pivoted = pd.pivot_table(
        df_posicao,
        index="account",
        columns="categoria",
        values="valor_posicao",
        aggfunc="median",
    ).fillna(0)
    pivoted = pivoted.drop(columns="VALOR EM TRÂNSITO")
    df_model2 = pivoted.copy()
    df_model2 = df_model2 / df_model2.sum(axis=1).values.reshape(-1, 1)

    print("Predição do Modelo de AA Feita")
    print(path_modelo_pos)

    # Lê dicionários com modelos de posição
    with BytesIO() as f:
        boto3.client("s3").download_fileobj(
            Bucket=bucket, Key=path_modelo_pos, Fileobj=f
        )
        f.seek(0)
        modelos = joblib.load(f)

    # Loop que aplica cada sub-modelo de posição nos dados filtrados
    for key, value in sub_modelos.items():
        cluster = key

        contas_i = df_results[df_results.Cluster == cluster].index

        df_model_i = df_model2[df_model2.index.isin(contas_i)]

        cols = [
            col
            for col in df_model_i.columns
            if any(substring in col for substring in value["Cols"])
        ]
        df_model_i = df_model_i[cols]

        results_i = pd.DataFrame(
            modelos[key].predict(df_model_i),
            index=df_model_i.index,
            columns=["Cluster"],
        )

        cluster_nomes2 = value["Cluster"]

        results_i["Cluster"] = results_i["Cluster"].apply(lambda x: cluster_nomes2[x])

        df_results = df_results.merge(
            results_i["Cluster"],
            how="left",
            left_index=True,
            right_index=True,
            suffixes=("", f"{value['Nome']}"),
        )

        print(f"Predição do Modelo de {key} Feita")

    # Cria coluna de data
    df_results["Data"] = data
    df_results["Data"] = df_results["Data"].astype(str)

    # Lê dados de resultados, anexa os resultados da função, remove duplicatas e salva
    df_final = pd.read_parquet(path_resultados_aa)
    df_final = pd.concat([df_final, df_results])
    df_reset = df_final.reset_index()
    df_unique = df_reset[~df_reset.duplicated(subset=["Conta", "Data"], keep="last")]
    df_unique.set_index("Conta", inplace=True)
    df_unique.to_parquet(path_resultados_aa)
