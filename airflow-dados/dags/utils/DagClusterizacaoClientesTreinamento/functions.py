import pandas as pd
import numpy as np
import boto3
from datetime import date, datetime
from sklearn.pipeline import Pipeline
from dateutil.relativedelta import relativedelta
from sklearn.cluster import KMeans
import pickle
from utils.bucket_names import get_bucket_name
from sklearn.decomposition import PCA
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import MinMaxScaler, PowerTransformer, StandardScaler

bucket_name_gold = get_bucket_name("lake-gold")
bucket_name_silver = get_bucket_name("lake-silver")
bucket = get_bucket_name("modelos-ml")

# Paths num dicionário

path_dados_mov = (
    f"s3://{bucket_name_silver}/ml/clusterizacao_clientes/movimentacao_processada"
)
path_dados_nnm = f"s3://{bucket_name_silver}/ml/clusterizacao_clientes/nnm_processada"
path_dados_aa = (
    f"s3://{bucket_name_silver}/ml/clusterizacao_clientes/asset_allocation_processada"
)
path_dados_posicao = (
    f"s3://{bucket_name_silver}/ml/clusterizacao_clientes/posicao_processada"
)

path_historico_mov = "clusterizacao_clientes/Movimentacoes/Historico"
path_current_mov = "clusterizacao_clientes/Movimentacoes/Current/"
path_historico_nnm = "clusterizacao_clientes/NNM/Historico"
path_current_nnm = "clusterizacao_clientes/NNM/Current"
path_historico_aa = "clusterizacao_clientes/AA/Historico/Modelo"
path_current_aa = "clusterizacao_clientes/AA/Current/Modelo/"

# Novos paths adicionados
path_historico_pos = "clusterizacao_clientes/Posicao/Historico"
path_current_pos = "clusterizacao_clientes/Posicao/Current/model.pkl"

path_resultados_aa = f"s3://{bucket_name_gold}/ml/clusterizacao_clientes/asset_allocation/predict_aa.parquet"

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


def remove_outliers_func(df: pd.DataFrame, cols: list = None):
    """Esta função remove outliers das colunas assinaladas com base no método
    do IQR.

    Args:
        df (pd.DataFrame): dataframe a ser utilizado.
        cols (list): lista de colunas a serem utilizadas.
    """
    dfi = df.copy()
    if cols is None:
        cols = df.columns
    for col in cols:
        q1 = dfi[col].quantile(0.01)
        q3 = dfi[col].quantile(0.99)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        upper_array = np.where(dfi[col] >= upper)[0]
        lower_array = np.where(dfi[col] <= lower)[0]
        dfi = dfi.drop(index=upper_array)
        dfi = dfi.drop(index=lower_array).reset_index(drop=True)
    return dfi


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


def train_movimentacoes(date_interval_end: str = None, save_as_current: bool = False):
    """Esta função treina o modelo de segmentação de movimentações
    com base na data passada nos argumentos, posteriormente salva
    o pipeline em formato pickle no S3.

    Args:
        date_interval_end (str): data base usada para o treinamento,
            vai determinar o path de onde serão puxados os dados.
        save_as_current (bool): determina se o modelo treinado substituirá
            o modelo na pasta "Current" ou não.
    """

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

    # Manipulações
    df["soma_ativa"] = df["soma_ativa"] / df["pl"]
    df["soma_passiva"] = df["soma_passiva"] / df["pl"]
    df = df.reset_index(drop=True)

    # Remoção de outliers
    df2 = remove_outliers_func(
        df,
        cols=[
            "frequencia_geral",
            "media_ativa",
            "media_passiva",
            "soma_ativa",
            "soma_passiva",
        ],
    )

    # Coloca os dados no formato necessário para treinamento do modelo
    df_modelm = df2.copy()
    df_modelm.index = df2["account"]
    df_modelm = df_modelm.drop(columns="account")
    for col in df_modelm.columns:
        if "frequencia" in col:
            if "geral" not in col:
                df_modelm[col] = df_modelm[col] / df_modelm["frequencia_geral"]

    # Remove algumas colunas que não serão utilizadas
    df_modelm = df_modelm.drop(columns=["idade_conta", "pl", "date"])

    # Remove os clientes que tiveram menos de 5 movimentações no período
    df_modelm2 = df_modelm[df_modelm.frequencia_geral >= 5]

    # Divide colunas de frequência dos demais
    frequencia_columns = [col for col in df_modelm.columns if "frequencia" in col]
    other_columns = [col for col in df_modelm.columns if col not in frequencia_columns]

    # Pipeline de pré-processamento e modelo
    pipeline = Pipeline(
        [
            (
                "preprocessor",
                ColumnTransformer(
                    transformers=[
                        (
                            "yeo_johnson_transform",
                            PowerTransformer(method="yeo-johnson", standardize=True),
                            frequencia_columns,
                        )
                    ]
                ),
            ),
            ("kmeans", KMeans(n_clusters=6, random_state=42)),
        ]
    )

    # Fit do pipeline
    pipeline.fit(df_modelm2)

    # Salva o pipeline na pasta histórica
    pipeline_bytes = pickle.dumps(pipeline)

    ano = data.year
    mes = data.month
    dia = data.day

    s3_key = f"{path_historico_mov}/{ano}/{mes}/model.pkl"

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=s3_key, Body=pipeline_bytes)

    # Se True substitui o modelo na pasta Current pelo modelo treinado nessa função.
    if save_as_current == True:
        s3.put_object(Bucket=bucket, Key=path_current_mov, Body=pipeline_bytes)


def train_nnm(date_interval_end: str = None, save_as_current: bool = False):
    """Esta função treina o modelo de segmentação de Net New Money
    com base na data passada nos argumentos, posteriormente salva
    o pipeline em formato pickle no S3.

    Args:
        date_interval_end (str): data base usada para o treinamento,
            vai determinar o path de onde serão puxados os dados.
        save_as_current (bool): determina se o modelo treinado substituirá
            o modelo na pasta "Current" ou não.
    """

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

    # Manipulações
    df["soma_valor_nnm"] = df["soma_valor_nnm"] / df["pl"]
    df = df.reset_index(drop=True)

    # Remoção de outliers
    df2 = remove_outliers_func(
        df, cols=["frequencia_nnm", "soma_valor_nnm", "media_valor_nnm"]
    )

    # Coloca os dados no formato necessário para treinamento do modelo
    df_modeln = df2.copy()
    df_modeln.index = df2["account"]
    df_modeln = df_modeln.drop(columns="account")
    for col in df_modeln.columns:
        if "frequencia" in col:
            if "nnm" not in col:
                df_modeln[col] = df_modeln[col] / df_modeln["frequencia_nnm"]

    # Remove algumas colunas que não serão utilizadas
    df_modeln = df_modeln.drop(
        columns=["idade_conta", "pl", "date", "mediana_valor_nnm"]
    )

    # Remove os clientes que tiveram menos de 3 movimentações no período
    df_modeln2 = df_modeln[df_modeln.frequencia_nnm >= 3]

    # Divide colunas de frequência dos demais
    frequencia_columns = [col for col in df_modeln.columns if "frequencia" in col]

    # Pipeline de pré-processamento e modelo
    pipeline = Pipeline(
        [
            (
                "preprocessor",
                ColumnTransformer(
                    transformers=[
                        (
                            "yeo_johnson_transform",
                            PowerTransformer(method="yeo-johnson", standardize=True),
                            df_modeln2.columns,
                        )
                    ]
                ),
            ),
            ("kmeans", KMeans(n_clusters=6, random_state=42)),
        ]
    )

    # Fit do pipeline
    pipeline.fit(df_modeln2)

    # Salva o pipeline na pasta histórica
    pipeline_bytes = pickle.dumps(pipeline)

    ano = data.year
    mes = data.month
    dia = data.day

    s3_key = f"{path_historico_nnm}/{ano}/{mes}/model.pkl"

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=s3_key, Body=pipeline_bytes)

    # Se True substitui o modelo na pasta Current pelo modelo treinado nessa função.
    if save_as_current == True:
        s3.put_object(Bucket=bucket, Key=path_current_nnm, Body=pipeline_bytes)


def train_aa(date_interval_end: str = None, save_as_current: bool = False):
    """Esta função treina o modelo de segmentação de Asset Allocation
    com base na data passada nos argumentos, posteriormente salva
    o pipeline em formato pickle no S3.

    Args:
        date_interval_end (str): data base usada para o treinamento,
            vai determinar o path de onde serão puxados os dados.
        save_as_current (bool): determina se o modelo treinado substituirá
            o modelo na pasta "Current" ou não.
    """

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

    # Remove algumas colunas que não serão utilizadas.
    df_model = df[
        [
            col
            for col in df.columns
            if "Data" not in col and "Conta" not in col and "mean" in col
        ]
    ]
    for col in ["AÇÕES_mean", "MULTIMERCADOS_mean", "RENDA FIXA_mean"]:
        try:
            df_model = df_model.drop(columns=col)
        except KeyError:
            pass

    # Coloca os dados no formato necessário para modelagem
    df_model.index = df["Conta"]
    df_model.head()
    df_model = df_model / df_model.sum(axis=1).values.reshape(-1, 1)

    # PCA
    pca = PCA(n_components=0.95)

    # Modelo
    kmeans = KMeans(n_clusters=6, random_state=42)

    # Une PCA e Modelo num Pipeline
    pipeline = Pipeline([("pca", pca), ("kmeans", kmeans)])

    # Fit do Pipeline
    pipeline.fit(df_model)

    # Salva o pipeline na pasta histórica
    pipeline_bytes = pickle.dumps(pipeline)

    ano = data.year
    mes = data.month
    dia = data.day

    s3_key = f"{path_historico_aa}/{ano}/{mes}/model.pkl"

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=s3_key, Body=pipeline_bytes)

    # Se True substitui o modelo na pasta Current pelo modelo treinado nessa função.
    if save_as_current == True:
        s3.put_object(Bucket=bucket, Key=path_current_aa, Body=pipeline_bytes)


def train_posicao(date_interval_end: str = None, save_as_current=False):
    """Esta função treina os submodelos de segmentação de posição
    com base na data passada nos argumentos, posteriormente salva
    o pipeline em formato pickle no S3.

    Args:
        date_interval_end (str): data base usada para o treinamento,
            vai determinar o path de onde serão puxados os dados.
        save_as_current (bool): determina se o modelo treinado substituirá
            o modelo na pasta "Current" ou não.
    """

    # Determinação da data a ser usada
    date_interval_end = date_interval_end.split("T")[0]
    data_base = datetime.strptime(date_interval_end, "%Y%m%d").date()
    data_base = data_base - relativedelta(day=1)
    data = data_base - relativedelta(days=1)

    # Lê os dados salvos da data assinalada.
    # É necessário realizar o processo de processamento na data requerida para que esse arquivo exista.
    df_posicao = pd.read_parquet(
        f"{path_dados_posicao}/{data.year}/{data.month}/{data.year}_{data.month}_{data.day}.parquet"
    )

    # Cria a coluna categoria
    df_posicao["categoria"] = df_posicao.apply(lambda x: categorizar(x), axis=1)

    # Manipulações
    pivoted = pd.pivot_table(
        df_posicao,
        index="account",
        columns="categoria",
        values="valor_posicao",
        aggfunc="median",
    ).fillna(0)

    # Remove coluna que não será utilizada
    pivoted = pivoted.drop(columns="VALOR EM TRÂNSITO")

    # Coloca os dados em formato que será treinado no modelo
    df_model2 = pivoted.copy()
    df_model2 = df_model2 / df_model2.sum(axis=1).values.reshape(-1, 1)

    # Puxar dados de resultados AA
    df_aa = pd.read_parquet(path_resultados_aa)
    df_aa = df_aa[df_aa["Data"] == data.strftime("%Y-%m-%d")]

    if len(df_aa) == 0:
        raise DatesError(
            f"Data base do modelo {data.strftime('%Y-%m-%d')} não encontrada nos resultados do modelo de AA"
        )

    # Loop que filtra os dados de resultados AA por Cluster, filtra colunas e treina cada modelo
    # Salva os modelos no dicionário abaixo
    modelos = {}

    for key, value in sub_modelos.items():
        cluster = key

        contas_i = df_aa[df_aa.Cluster == cluster].index

        df_model_i = df_model2[df_model2.index.isin(contas_i)]

        cols = [
            col
            for col in df_model_i.columns
            if any(substring in col for substring in value["Cols"])
        ]

        df_model_i = df_model_i[cols]

        # Pré-processamento
        preprocessor = Pipeline([("scaler", StandardScaler())])

        # Modelo
        kmeans = KMeans(n_clusters=value["Num_clusters"], random_state=42)

        # Une Modelo e Pré-processamento em Pipeline único
        pipeline = Pipeline([("preprocessor", preprocessor), ("kmeans", kmeans)])

        # Fit do pipeline
        pipeline.fit(df_model_i)

        modelos[key] = pipeline

    # Salva os pipelines na pasta histórica
    pipeline_bytes = pickle.dumps(modelos)

    ano = data.year
    mes = data.month
    dia = data.day

    s3_key = f"{path_historico_pos}/{ano}/{mes}/model.pkl"

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=s3_key, Body=pipeline_bytes)

    # Se True substitui os pipelines na pasta Current pelo modelo treinado nessa função.
    if save_as_current == True:
        s3.put_object(Bucket=bucket, Key=path_current_pos, Body=pipeline_bytes)
