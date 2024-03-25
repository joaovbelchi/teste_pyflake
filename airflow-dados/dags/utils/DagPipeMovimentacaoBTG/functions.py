import pandas as pd
from utils.bucket_names import get_bucket_name
from deltalake import DeltaTable
from dateutil.parser import isoparse


def atualiza_movimentacao_silver(data_interval_end: str):
    # Buckets S3
    bucket_lake_silver = get_bucket_name("lake-silver")
    bucket_lake_bronze = get_bucket_name("lake-bronze")

    # Mapeamento de colunas
    rename_dict = {
        "nr_conta": "account",
        "mercado": "market",
        "sub_mercado": "sub_market",
        "grupo_contabil": "product",
        "historico_movimentacao": "movimentation",
        "tipo_lancamento": "launch",
        "emissor": "issuer",
        "ativo": "stock",
        "tipo_opcao": "option_type",
        "indexador": "indexer",
        "vl_taxa": "coupon",
        "vl_taxa_compra": "bought_coupon",
        "dt_movimentacao": "purchase_date",
        "dt_exercicio": "exercise_date",
        "dt_vencimento": "maturity_date",
        "quantidade": "amount",
        "vl_bruto": "gross_value",
        "vl_ir": "ir",
        "vl_iof": "iof",
        "vl_liquido": "net_value",
        "dt_interface": "quote_date",
        "dt_liquidacao": "liquidation_date",
    }
    colunas_bronze = list(rename_dict.keys()) + [
        "timestamp_dagrun",
        "timestamp_escrita_bronze",
    ]
    colunas_silver = list(rename_dict.values()) + ["name", "date"]

    # Leitura do movimentação histórico
    df_mov_historico_raw = pd.read_parquet(
        f"s3://{bucket_lake_silver}/api/btg/s3/movimentacao_historico/movimentacao_historico.parquet",
        columns=colunas_silver,
    )

    # Leitura do movimentação bronze do dia
    parsed_datetime = isoparse(data_interval_end)
    ano_particao = str(parsed_datetime.year)
    mes_particao = str(parsed_datetime.month)
    dt_mov_bronze = DeltaTable(
        f"s3://{bucket_lake_bronze}/btg/webhooks/downloads/movimentacao"
    )
    movimentacao_bronze = dt_mov_bronze.to_pandas(
        partitions=[
            ("ano_particao", "=", ano_particao),
            ("mes_particao", "=", mes_particao),
        ],
        columns=colunas_bronze,
    )
    movimentacao_bronze = movimentacao_bronze[
        (movimentacao_bronze["timestamp_dagrun"] == data_interval_end)
    ]
    movimentacao_bronze = movimentacao_bronze[
        (
            movimentacao_bronze["timestamp_escrita_bronze"]
            == movimentacao_bronze["timestamp_escrita_bronze"].max()
        )
    ]

    # Abre base de contas gerado pela Dag Run desejada
    dt = DeltaTable(f"s3://{bucket_lake_silver}/btg/contas")
    df_contas = dt.to_pandas(
        partitions=[
            ("date_escrita", "=", parsed_datetime.strftime("%Y-%m-%d")),
        ]
    )

    # Define a data limite de atualização
    date_limit = movimentacao_bronze["dt_interface"].min()
    date_max = movimentacao_bronze["dt_interface"].max()

    # Corrige datas de movimentação menores do que a data interface mínima
    movimentacao_bronze.loc[
        movimentacao_bronze["dt_movimentacao"] < date_limit, ["dt_movimentacao"]
    ] = movimentacao_bronze["dt_interface"]

    # Define tipos das contas para string e remove zeros
    df_mov_historico_raw["account"] = df_mov_historico_raw["account"].astype("string")
    movimentacao_bronze["nr_conta"] = (
        movimentacao_bronze["nr_conta"].astype("int64").astype("string")
    )
    df_contas["carteira"] = df_contas["carteira"].astype("string")

    # Define tipos das datas para data
    df_mov_historico_raw["date"] = pd.to_datetime(df_mov_historico_raw["date"])
    movimentacao_bronze["dt_movimentacao"] = pd.to_datetime(
        movimentacao_bronze["dt_movimentacao"]
    )

    # Coleta de contas que estão na base da one
    contas_hoje = df_contas["carteira"].tolist()

    # Mantém as contas que já saíram ou movimentações anteriores à data limite de atualização
    df_mov_historico = df_mov_historico_raw[
        (~df_mov_historico_raw["account"].isin(contas_hoje))
        | (df_mov_historico_raw["purchase_date"] < date_limit)
    ]
    # Mantém apenas contas que estão na base hoje
    df_mov_novos = movimentacao_bronze[
        (movimentacao_bronze["nr_conta"].isin(contas_hoje))
    ]
    # Altera os nomes das colunas
    df_mov_novos = df_mov_novos.rename(
        rename_dict,
        axis=1,
    )

    # Formata coluna 'quote_date'
    df_mov_novos["quote_date"] = df_mov_novos["quote_date"].dt.strftime("%Y-%m-%d")

    # Cria coluna de nome
    df_mov_novos["name"] = None
    
    #Cria um df novo renomeando as colunasd de df_contas para poder dar merge com df_mov_novos
    contas_account = df_contas.rename(columns={'carteira': 'account', 'nome_completo': 'name'})
    #Realiza o merge on left(df_mov_novos)
    df_mov_novos = df_mov_novos.merge(contas_account[['account', 'name']], on='account', how='left')
    
    #dropa a coluna de nomes pré-existente, mantendo apenas a do contas_account
    df_mov_novos = df_mov_novos.drop(columns=['name_x'])
    df_mov_novos = df_mov_novos.rename(columns={'name_y': 'name'})
    #Atualiza o campo name, agora apenas com o primeiro nome, em vez de completo
    df_mov_novos['name'] = df_mov_novos['name'].str.split(' ').str[0]

    # Cria coluna de data
    df_mov_novos["date"] = date_max.strftime("%Y-%m-%d")

    # Seleciona as colunas necessárias para o movimentação
    df_mov_final = df_mov_novos[colunas_silver]
   
    # Une o movimentação histórico com o novos
    df_mov_final = pd.concat([df_mov_final, df_mov_historico]).reset_index(drop=True)

    # Ajusta dtypes
    string_cols = [
        "account",
        "market",
        "sub_market",
        "product",
        "movimentation",
        "launch",
        "issuer",
        "stock",
        "option_type",
        "indexer",
        "quote_date",
        "name",
    ]
    datetime_cols = ["purchase_date", "exercise_date", "maturity_date", "liquidation_date", "date"]
    float_cols = [
        "coupon",
        "bought_coupon",
        "amount",
        "gross_value",
        "ir",
        "iof",
        "net_value",
    ]

    for col in string_cols:
        df_mov_final[col] = df_mov_final[col].astype("string")

    for col in datetime_cols:
        df_mov_final[col] = pd.to_datetime(df_mov_final[col])

    for col in float_cols:
        df_mov_final[col] = df_mov_final[col].astype("float64")

    # Escreve resultado
    df_mov_final = df_mov_final.sort_values("date")
    df_mov_final.to_parquet(
        f"s3://{bucket_lake_silver}/api/btg/s3/movimentacao_historico/movimentacao_historico.parquet"
    )
