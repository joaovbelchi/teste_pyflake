import pandas as pd
import json
from datetime import datetime

from utils.OneGraph.drive import OneGraphDrive
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name
from utils.mergeasof import (
    fill_retroativo,
    remover_antes_entrada,
    mergeasof_socios,
)

from utils.DagReceitaEstimada.funcoes_auxiliares import (
    movimentacao_rv,
    remover_trades_internos_duplicados,
    extrair_data_maxima_da_receita,
)


def receita_estimada(drive_id):
    # Credenciais da Graph API da Microsoft
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    # Definir os buckets
    bucket_lake_silver = get_bucket_name("lake-silver")
    bucket_lake_gold = get_bucket_name("lake-gold")

    # Definir variáveis de data
    now = datetime.now()

    date = now.date().strftime("%Y-%m-%d")
    date_aux = now.date().strftime("%Y%m%d")

    # Pegar o dia 1° do mês atual no ano passado
    date_12m = datetime(now.year - 1, now.month, 1)
    date_12m = datetime.strftime(date_12m, format="%Y-%m-%d")

    # Ler o Consolidado-Historico (Receita Realizada)
    path_receita = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "csv_pbi",
        "Consolidado-Historico.csv",
    ]

    kwargs = {"sep": ";", "decimal": ","}

    # Ler o conteúdo do arquivo e transformar em dataframe
    df_receita_raw = drive.download_file(path=path_receita, to_pandas=True, **kwargs)

    # Filtrar últimos 12 meses
    df_receita_realizada = df_receita_raw[(df_receita_raw["Data"] >= date_12m)]

    # Definir como Receita Realizada, pois já chegou o relatório com essa receita
    df_receita_realizada.loc[
        (df_receita_realizada["Receita Bruta"].notnull()),
        "Tipo Receita",
    ] = "Receita Realizada"

    # Renomear as colunas
    cols_names = {
        "Produto_1": "Produto",
        "Produto_2": "Ativo",
        "Receita Bruta": "Receita",
        "Data": "Data",
    }

    df_receita_realizada = df_receita_realizada.rename(cols_names, axis=1)

    # Reordenar as colunas
    df_receita_realizada = df_receita_realizada[
        [
            "Conta",
            "Tipo",
            "Categoria",
            "Produto",
            "Ativo",
            "Receita",
            "Data",
            "Tipo Receita",
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "Departamento",
        ]
    ]

    # Ler os arquivos com a receita estimada mais recenta
    bucket_lake_silver = get_bucket_name("lake-silver")

    path = f"s3://{bucket_lake_silver}/one/receita/estimativa"

    arquivos = [
        "renda_fixa",
        "renda_variavel",
        "fundos",
        "previdencia",
        "oferta_publica",
        "cambio",
        "estruturadas",
        "trades_internos_rf",
        "trades_internos_rv",
    ]

    df_receita = pd.DataFrame()

    for arquivo in arquivos:
        df = pd.read_parquet(
            f"{path}/{arquivo}/date={date}/{arquivo}_{date_aux}.parquet"
        )

        df_receita = pd.concat([df_receita, df])

    # Renomear colunas
    df_receita.rename({"Receita Estimada": "Receita"}, axis=1, inplace=True)

    # Padronizar as colunas
    df_receita["Conta"] = df_receita["Conta"].astype(str)

    df_receita.rename({"Data Movimentação": "Data"}, axis=1, inplace=True)
    df_receita["Data"] = pd.to_datetime(df_receita["Data"])

    df_receita = df_receita[df_receita["Data"].notnull()]
    df_receita = df_receita.sort_values("Data").reset_index()

    # Remover trades internos duplicados,
    # visto que podem aparecer tanto na planilha de trades internos,
    # quanto na base de movimentação
    df_receita = remover_trades_internos_duplicados(df_receita)

    # Ler base de movimentações de Renda Variavel
    # para preencher algumas informações na receita realizada
    max_date_rv = extrair_data_maxima_da_receita(
        tipo="VALORES MOBILIARIOS", categoria="RENDA VARIAVEL", produto="CORRETAGEM"
    )

    df_movimentacao_rv = movimentacao_rv(drive, max_date_rv)

    # Renomear as colunas
    df_movimentacao_rv.rename(
        {
            "purchase_date": "Data",
            "account": "Conta",
            "stock": "Ativo",
            "launch": "Movimentação",
            "amount": "Quantidade",
            "gross_value": "Valor Bruto",
        },
        axis=1,
        inplace=True,
    )

    # Padronizar colunas
    df_receita["Data"] = df_receita["Data"].astype(str)
    df_movimentacao_rv["Data"] = df_movimentacao_rv["Data"].astype(str)

    # Merge da base de receita com a base de movimentação
    df_receita = df_receita.merge(
        df_movimentacao_rv,
        on=["Conta", "Ativo", "Data"],
        how="left",
        suffixes=("_receita", "_mov"),
    )

    # Completar colunas em comum (fillna)
    columns = ["Movimentação", "Quantidade", "Valor Bruto"]
    for col in columns:
        df_receita[col] = df_receita[f"{col}_receita"].fillna(df_receita[f"{col}_mov"])

    print(df_receita[df_receita["Conta"] == "OU163656"])

    # Merge asof
    # Ler e tratar as bases necessárias
    df_contrata = pd.read_parquet(
        f"s3://{bucket_lake_gold}/api/one/socios/socios.parquet"
    )
    df_contrata["Data início"] = pd.to_datetime(df_contrata["Data início"])
    df_contrata["Data Saída"] = pd.to_datetime(df_contrata["Data Saída"])
    df_contrata_red = df_contrata[["Nome_bd", "Data início", "Data Saída"]]

    df_advisors = pd.read_parquet(
        f"s3a://{bucket_lake_silver}/api/btg/crm/ContasHistorico/ContasHistorico.parquet"
    )

    # Reordenar colunas
    df_advisors = df_advisors[
        [
            "Conta",
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "Grupo familiar",
            "Nome",
            "Id GF",
            "Origem do cliente",
            "Data",
            "Id Conta",
            "Contato/Empresa",
            "Origem 1",
            "Origem 2",
            "Origem 3",
            "Origem 4",
        ]
    ]

    # Padronizar colunas
    # Receita
    df_receita["Conta"] = df_receita["Conta"].astype(str)
    df_receita["Data"] = pd.to_datetime(df_receita["Data"])
    df_receita = df_receita.sort_values("Data")

    # Advisors
    df_advisors["Conta"] = df_advisors["Conta"].astype(str)
    df_advisors["Data"] = pd.to_datetime(df_advisors["Data"])
    df_advisors = df_advisors.sort_values("Data")

    # Executar o merge asof
    df_receita_cp = df_receita.copy()
    df_receita = pd.merge_asof(
        df_receita, df_advisors, on="Data", by="Conta", direction="backward"
    )
    temp = pd.merge_asof(
        df_receita_cp, df_advisors, on="Data", by="Conta", direction="forward"
    )
    df_receita = fill_retroativo(df_receita, temp)
    df_receita = df_receita.sort_values("Data").reset_index(drop=True)
    df_receita = remover_antes_entrada(df_receita, df_contrata_red)

    print(df_receita[df_receita["Conta"] == "OU163656"])

    # Merge Asof para utilizar a coluna "Departamento"
    df_receita = mergeasof_socios(df_receita, "Comercial", "Data").drop(
        ["Área", "Unidade de Negócios", "Team Leader", "Business Leader"], axis=1
    )

    # Unir receita realizada e estimada
    df_receita = pd.concat([df_receita_realizada, df_receita], ignore_index=True)

    print(df_receita[df_receita["Conta"] == "OU163656"])

    # Separa o primeiro nome do cliente
    df_receita["Nome"] = df_receita["Nome"].astype(str).str.split().str[0]

    # Remover receitas do Corporate
    df_receita = df_receita[df_receita["Departamento"] != "CORPORATE"]

    df_receita["Conta"] = df_receita["Conta"].astype("string")
    df_receita["Data"] = pd.to_datetime(df_receita["Data"])

    # Reordenar as colunas
    df_receita = df_receita[
        [
            "Conta",
            "Nome",
            "Grupo familiar",
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "Tipo",
            "Categoria",
            "Produto",
            "Emissor",
            "Ativo",
            "CNPJ",
            "Movimentação",
            "Quantidade",
            "Valor Bruto",
            "Valor Reservado",
            "Valor Obtido",
            "Data",
            "Data Vencimento",
            "Receita",
            "Tipo Receita",
            "Receita Reservada",
            "Receita Obtida",
            "Estrutura",
            "Tipo Ordem",
            "Bookbuilding",
            "Trade Interno",
        ]
    ]

    df_receita.to_parquet(
        f"s3://{bucket_lake_silver}/one/receita/estimativa/receita_estimada/date={date}/receita_estimada_{date_aux}.parquet"
    )
