import pandas as pd
import io
import json
from datetime import datetime

from utils.OneGraph.drive import OneGraphDrive
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name

from utils.DagReceitaEstimada.funcoes_auxiliares import (
    posicao,
    get_most_recent_file,
    extrair_data_maxima_da_receita,
)


def previdencia(drive_id):
    IMPOSTO = 0.9335

    # Credenciais da Graph API da Microsoft
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    # Identificar a data do último relatório de receita contendo previdência
    max_date = extrair_data_maxima_da_receita(
        tipo="PREVIDENCIA PRIVADA", produto="ADMINISTRAÇAO"
    )

    # Ler a base PREV na pasta Dados_mercado
    # Definir o caminho do arquivo
    path_prev = ["One Analytics", "Banco de dados", "Dados_mercado", "PREV.xlsx"]

    # Ler o conteúdo do arquivo
    df_prev_onedrive = drive.download_file(path=path_prev, to_pandas=True)

    # Identificar e ler a base mais recente com as informações dos fundos
    bucket_lake_bronze = get_bucket_name("lake-bronze")

    kwargs = {
        "Bucket": bucket_lake_bronze,
        "Prefix": "api/btg/s3/tb_fundo_informacao/date_escrita",
        "StartAfter": "api/btg/s3/tb_fundo_informacao/date_escrita=2023-10-01",
    }

    df_prev_s3 = get_most_recent_file(**kwargs)
    df_prev_s3.rename({"NM_FUNDO": "Produto", "NR_CNPJ": "CNPJ"}, axis=1, inplace=True)

    # Unir as bases com o CNPJ dos fundos
    df_prev = pd.concat([df_prev_onedrive, df_prev_s3])
    df_prev = df_prev.drop_duplicates("Produto")

    # Ler a base dados_previdencia para usar o ROA dos fundos
    # Definir o caminho do arquivo
    path_dados_prev = [
        "One Analytics",
        "Análises",
        "Receita",
        "Infos",
        "dados_previdencia.xlsx",
    ]

    dados_prev = drive.download_file(path=path_dados_prev).content

    # Ler o conteúdo do arquivo
    # Previdências BTG
    # Coloquei número no usecols, pois a planilha possui nome de coluna repetido
    df_dados_prev_btg = pd.read_excel(
        io.BytesIO(dados_prev), sheet_name="Prev BTG", skiprows=3, usecols=[1, 2, 12]
    )

    # Previdências ICATU
    # Coloquei número no usecols, pois a planilha possui nome de coluna repetido
    df_dados_prev_icatu = pd.read_excel(
        io.BytesIO(dados_prev), sheet_name="Icatu", skiprows=1, usecols=[1, 2, 6]
    )

    # Ler a base Outros Fundos para identificar ROA dos fundos que não estão na planilha dados_previdencia
    # Definir o caminho do arquivo
    path_outros_fundos = [
        "One Analytics",
        "Renda Fixa",
        "13 - Planilhas de Atualização",
        "Outros Fundos.xlsx",
    ]

    kwargs = {"sheet_name": "Prev"}

    # Ler o conteúdo do arquivo
    df_outros_fundos = drive.download_file(
        path=path_outros_fundos, to_pandas=True, **kwargs
    )

    # Padronizar o CNPJ
    df_outros_fundos["CNPJ"] = (
        df_outros_fundos["CNPJ"].astype(str).str.split(".").str[0].str.zfill(14)
    )

    # Unir previdência ICATU, previdência BTG e outros fundos
    df_dados_prev = pd.concat(
        [df_dados_prev_btg, df_dados_prev_icatu, df_outros_fundos]
    )

    df_dados_prev["receita_estimada"] = (
        df_dados_prev["ROA.2"]
        .fillna(df_dados_prev["ROA"])
        .fillna(df_dados_prev["Margem Adm"])
    )

    # Padronizar o CNPJ
    df_dados_prev["CNPJ"] = (
        df_dados_prev["CNPJ"].astype(str).str.split(".").str[0].str.zfill(14)
    )

    # Remover duplicadas, priorizando o input da planilha
    df_dados_prev = df_dados_prev.drop_duplicates("CNPJ", keep="last")

    # Separar base de receita a partir da data da última receita de previdência
    df_posicao = posicao(max_date)

    df_posicao["data_interface"] = pd.to_datetime(df_posicao["data_interface"])

    # Filtrar o dataframe de posição
    df_posicao_prev = df_posicao[
        (df_posicao["market"] == "PREVIDÊNCIA")
        & (df_posicao["data_interface"].dt.date > max_date.date())
    ]

    # Merge para identificar o CNPJ do fundo
    df_posicao_prev = df_posicao_prev.merge(
        df_prev, left_on="product", right_on="Produto", how="left", indicator=True
    )

    # Padronizar o CNPJ
    df_posicao_prev["CNPJ"] = (
        df_posicao_prev["CNPJ"].astype(str).str.split(".").str[0].str.zfill(14)
    )

    # Merge para identificar o ROA dos fundos a partir do CNPJ
    df_prev = df_posicao_prev.merge(df_dados_prev, on="CNPJ", how="left")

    # Margem de Adm na planilha dados_previdencia é anual, mas aqui precisamos utilizar a diária
    df_prev["repasse"] = ((1 + df_prev["receita_estimada"]) ** (1 / 252)) - 1
    df_prev["repasse"] = df_prev["repasse"] * IMPOSTO

    # Calcular a receita estimada
    df_prev["Receita"] = df_prev["gross_value"] * df_prev["repasse"]

    # Remover algumas colunas
    df_prev = df_prev.drop(["ROA", "Categoria", "Produto"], axis=1)

    # Preencher as colunas
    df_prev.loc[df_prev["Receita"].notnull(), "Tipo Receita"] = "Receita Estimada"
    df_prev["Tipo"] = "PREVIDENCIA PRIVADA"
    df_prev["Produto"] = "ADMINISTRAÇAO"

    # Renomear as colunas
    cols_names = {
        "account": "Conta",
        "market": "Categoria",
        "product": "Ativo",
        "amount": "Quantidade",
        "gross_value": "Valor Bruto",
        "data_interface": "Data Movimentação",
        "receita_estimada": "ROA",
        "repasse": "Repasse",
        "Receita": "Receita Estimada",
    }

    df_prev.rename(cols_names, axis=1, inplace=True)

    # Reordenar as colunas
    df_prev = df_prev[
        [
            "Conta",
            "Tipo",
            "Categoria",
            "Produto",
            "Ativo",
            "CNPJ",
            "Data Movimentação",
            "Quantidade",
            "Valor Bruto",
            "ROA",
            "Repasse",
            "Receita Estimada",
            "Tipo Receita",
        ]
    ]

    # Definir variáveis de data
    now = datetime.now()
    date = now.date().strftime("%Y-%m-%d")
    date_aux = now.date().strftime("%Y%m%d")

    bucket_lake_silver = get_bucket_name("lake-silver")

    df_prev.to_parquet(
        f"s3://{bucket_lake_silver}/one/receita/estimativa/previdencia/date={date}/previdencia_{date_aux}.parquet"
    )
