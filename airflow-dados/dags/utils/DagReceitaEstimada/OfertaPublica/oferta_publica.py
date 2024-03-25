import pandas as pd
import json
from datetime import datetime
import unicodedata

from utils.OneGraph.drive import OneGraphDrive
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name

from utils.DagReceitaEstimada.funcoes_auxiliares import (
    col_to_datetime,
    extrair_data_maxima_da_receita,
    write_delta_table,
)
from utils.DagReceitaEstimada.OfertaPublica.funcoes_auxiliares import (
    tratamento_ipo,
    ofertas_ausentes,
)


def oferta_publica(drive_id):
    # Credenciais da Graph API da Microsoft
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    today = datetime.now().date()
    last_month = today - pd.DateOffset(months=1)
    yesterday = pd.to_datetime(today) - pd.Timedelta(days=1)

    # Extrair a data máxima de valores mobiliários, já que as ofertas estão nesse relatório
    # e não tem um padrão claro de quais emissões chegam na prévia
    max_date = extrair_data_maxima_da_receita(tipo="VALORES MOBILIARIOS")

    # Se a data máxima for antes do dia 25, isso indica que os valores são da prévia
    # Sendo assim, a data máxima será o último dia do mês anterior
    if max_date <= datetime(last_month.year, last_month.month, 25):
        max_date = datetime(last_month.year, last_month.month, 1) - pd.DateOffset(
            days=1
        )

    # Ler a base de ipo do Histórico para identificar ofertas
    # Definir o caminho do arquivo
    path_ipo = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "ipo.csv",
    ]

    kwargs = {"sep": ";", "decimal": ","}

    # Ler o conteúdo do arquivo e transformar em dataframe
    df_ipo_raw = drive.download_file(path=path_ipo, to_pandas=True, **kwargs)

    df_ipo_raw = tratamento_ipo(df_ipo_raw)

    # Ler a base ROA - IPO da pasta de Renda Fixa
    # Definir o caminho do arquivo
    path_roa_ipo = [
        "One Analytics",
        "Renda Fixa",
        "13 - Planilhas de Atualização",
        "ROA - IPO.xlsx",
    ]

    # Ler o conteúdo do arquivo
    df_roa_ipo = drive.download_file(path=path_roa_ipo, to_pandas=True)

    # Tratar colunas de data
    datetime_columns = [
        "Data Inicial de Reserva",
        "Data Final de Reserva",
        "Data Receita",
    ]

    df_roa_ipo = col_to_datetime(df_roa_ipo, datetime_columns)

    # Remover registros duplicados mantendo o mais recente
    df_roa_ipo = df_roa_ipo.drop_duplicates(
        ["Ativo", "Data Inicial de Reserva", "Data Final de Reserva"], keep="last"
    )

    # Identificar ROA das ofertas
    df_ipo = df_ipo_raw.merge(
        df_roa_ipo,
        left_on=["stock", "reservation_initial_date", "reservation_final_date"],
        right_on=["Ativo", "Data Inicial de Reserva", "Data Final de Reserva"],
        how="left",
        indicator=True,
    )

    # Definir as variáveis para envio do e-mail de ativos ausentes
    kwargs = {
        "drive_id": drive_id,
        "item_id": "d64bcd7f-c47a-4343-b0b4-73f4a8e927a5",  # tech@investimentos.one
        "destinatarios": [
            "back.office@investimentos.one",
            "nathalia.montandon@investimentos.one",
            "samuel.machado@investimentos.one",
        ],
        "titulo": "[Receita Estimada] Ofertas Ausentes",
        "previa": "Preencher ofertas públicas",
    }

    # Se for o caso, enviar e-mail informando sobre ofertas que ainda não estão na planilha ROA - IPO
    ofertas_ausentes(df_ipo, max_date, **kwargs)

    df_ipo.drop("_merge", axis=1, inplace=True)

    # Definir os repasses, caso não tenha sido informado na planilha
    df_ipo.loc[(df_ipo["Tipo"] == "AAI RV") & (df_ipo["Repasse"].isna()), "Repasse"] = (
        0.85
    )
    df_ipo.loc[(df_ipo["Tipo"] == "AAI RF") & (df_ipo["Repasse"].isna()), "Repasse"] = (
        0.75
    )

    # Calcular a receita
    df_ipo["ROA Bruto"] = df_ipo["ROA Bruto"].astype(float)
    df_ipo["Repasse"] = df_ipo["Repasse"].astype(float)

    # Considerar impostos e repasse
    df_ipo["ROA Líquido"] = df_ipo["ROA Bruto"] * df_ipo["Repasse"] * 0.9035

    # Antes do bookbuilding
    df_ipo["Receita Reservada"] = df_ipo["booking_value"] * df_ipo["ROA Líquido"]

    # Após o bookbuilding
    df_ipo["Receita Obtida"] = df_ipo["value_obtained"] * df_ipo["ROA Líquido"]

    # Caso já tem ocorrido o bookbuilding, preencher a receita estimada com 0 antes do fillna
    # para que não apareça na estimaiva a receita de quem não obteve nada.
    # O bookbulding deve ter ocorrido em D-2 ou antes, visto que geralmente o rateio só aparece
    # na base após 1 ou 2 dias.
    df_ipo.loc[
        (df_ipo["book_building_date"] < yesterday) & (df_ipo["Receita Obtida"].isna()),
        "Receita Obtida",
    ] = 0

    df_ipo.loc[
        (df_ipo["book_building_date"] < yesterday) & (df_ipo["value_obtained"].isna()),
        "value_obtained",
    ] = 0

    # Criar coluna de receita priorizando a receita obtida sobre a reservada
    df_ipo["Receita"] = df_ipo["Receita Obtida"].fillna(df_ipo["Receita Reservada"])

    # Criar coluna de valor bruto priorizando o obtido sobre o reservado
    df_ipo["gross_value"] = df_ipo["value_obtained"].fillna(df_ipo["booking_value"])

    # Definir a data para considerar a receita
    # Utilizar a data de liquidação, pois é o dia que contabilizará a receita
    df_ipo["Data Movimentação"] = df_ipo["Data Receita"].fillna(
        df_ipo["liquidation_date"]
    )
    df_ipo["Data Movimentação"] = pd.to_datetime(df_ipo["Data Movimentação"])

    # Padronizar as colunas de acordo com o tipo da receita (RF ou RV)
    df_ipo.loc[df_ipo["Tipo"] == "AAI RV", "market"] = "RENDA VARIAVEL"
    df_ipo.loc[df_ipo["Tipo"] == "AAI RF", "market"] = "RENDA FIXA"

    # Preencher as colunas
    df_ipo.loc[df_ipo["Receita"].notnull(), "Tipo Receita"] = "Receita Estimada"
    df_ipo["launch"] = "COMPRA"
    df_ipo["Tipo"] = "VALORES MOBILIARIOS"
    df_ipo["product"] = "OFERTA PRIMARIA"

    df_ipo = df_ipo.drop(["Produto", "Ativo"], axis=1)

    # Renomear as colunas
    cols_names = {
        "account": "Conta",
        "market": "Categoria",
        "product": "Produto",
        "launch": "Movimentação",
        "stock": "Ativo",
        "amount": "Quantidade",
        "gross_value": "Valor Bruto",
        "booking_value": "Valor Reservado",
        "value_obtained": "Valor Obtido",
        "ROA Líquido": "ROA",
        "repasse": "Repasse",
        "Receita": "Receita Estimada",
    }

    df_ipo.rename(cols_names, axis=1, inplace=True)

    # Reordenar as colunas
    df_ipo = df_ipo[
        [
            "Conta",
            "Tipo",
            "Categoria",
            "Produto",
            "Movimentação",
            "Ativo",
            "Data Movimentação",
            "Quantidade",
            "Valor Bruto",
            "Valor Reservado",
            "Valor Obtido",
            "ROA",
            "Repasse",
            "Receita Estimada",
            "Receita Reservada",
            "Receita Obtida",
            "Tipo Receita",
            "book_building_date",
        ]
    ]

    # Definir variáveis de data
    now = datetime.now()
    date = now.date().strftime("%Y-%m-%d")
    date_aux = now.date().strftime("%Y%m%d")

    bucket_lake_silver = get_bucket_name("lake-silver")
    bucket_lake_gold = get_bucket_name("lake-gold")

    df_ipo_all = df_ipo.drop("book_building_date", axis=1)

    # Salvar na silver
    df_ipo_all.to_parquet(
        f"s3://{bucket_lake_silver}/one/receita/estimativa/oferta_publica_all/date={date}/oferta_publica_all_{date_aux}.parquet"
    )

    # Padronizar o nome das colunas
    # Ou seja, todas as letras minúsculas, sem caracteres especiais e sem espaço
    cols_normalized = [
        unicodedata.normalize("NFKD", col.replace(" ", "_").lower())
        .encode("ASCII", "ignore")
        .decode("utf-8")
        for col in df_ipo_all.columns
    ]

    df_ipo_all.columns = cols_normalized

    # Salvar na gold
    write_delta_table(
        df_ipo_all,
        partition_col="data_movimentacao",
        dt_path=f"s3://{bucket_lake_gold}/one/receita/estimativa/oferta_publica_all",
        mode="overwrite",
        partition_by=["ano", "mes"],
    )

    # Filtrar reservas que já estão confirmadas e ocorreram após o último relatório final ou prévia,
    # ou seja, que a receita ainda não chegou
    df_ipo_result = df_ipo[(df_ipo["Data Movimentação"] > max_date)]

    # Criar coluna para informar se já foi o Bookbuilding
    df_ipo_result["Bookbuilding"] = ""

    # Identificar reservas que ainda NÃO passaram pelo rateio (bookbuilding)
    df_ipo_result.loc[
        (df_ipo_result["book_building_date"] >= yesterday),
        "Bookbuilding",
    ] = "Não"

    df_ipo_result.to_parquet(
        f"s3://{bucket_lake_silver}/one/receita/estimativa/oferta_publica/date={date}/oferta_publica_{date_aux}.parquet"
    )
