import pandas as pd
import io
import json
import pandas_market_calendars as mcal
from datetime import datetime
from dateutil.relativedelta import relativedelta

from utils.OneGraph.drive import OneGraphDrive
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name

from utils.DagReceitaEstimada.funcoes_auxiliares import extrair_data_maxima_da_receita


def trades_internos_rv(drive_id):
    IMPOSTO = 0.9335

    # Credenciais da Graph API da Microsoft
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    max_date = extrair_data_maxima_da_receita(
        tipo="VALORES MOBILIARIOS", categoria="RENDA VARIAVEL", produto="CORRETAGEM"
    )

    # Ler a base de ipo do Histórico para identificar ofertas
    # Definir o caminho do arquivo
    path_rv = ["One Analytics", "Planilha Mesa RV", "Fundos Cetipados.xlsm"]

    # Ler o conteúdo do arquivo e transformar em dataframe
    rv = drive.download_file(path=path_rv).content

    cols_names = {
        "CONTA": "Conta",
        "ATIVO": "Ativo",
        "QNTD.": "Quantidade",
        "DIREÇÃO": "Movimentação",
        "VOLUME BRUTO": "Valor Bruto",
        "RECEITA BRUTA": "Receita Estimada",
        "ORDEM EXECUTADA": "Ordem Executada",
        "MÊS EXEC.": "Data Movimentação",
        "DATA EXEC.": "Data Movimentação",
    }

    # Identificar as colunas
    # Remover Data Movimentação uma vez, pois está "duplicado" no dicionário,
    # visto que possui duas correspondências
    cols = list(cols_names.values())
    cols.remove("Data Movimentação")

    df_rv = pd.DataFrame()
    for sheet_name in ["Ordens de Venda", "Ordens de Compra"]:
        # Ler a aba em questão
        df = pd.read_excel(io.BytesIO(rv), sheet_name=sheet_name, skiprows=1)

        # Escrever todas as colunas em letra maiúscula
        # para evitar que o código dê erro por conta disso
        upper_cols = []
        for col in df.columns.to_list():
            upper_cols += [col.upper()]

        df.columns = upper_cols

        # Renomear as colunas
        df = df.rename(cols_names, axis=1)

        print("cols", cols)
        print("columns", df.columns)

        # Selecionar apenas as colunas que serão mantidas
        df = df[cols]

        # Concatenar os dataframes
        df_rv = pd.concat([df_rv, df])

    # Utilizar calendário de dias úteis da B3
    bmf = mcal.get_calendar("BMF")

    # CBM para extrair o último dia do mês
    month = pd.bdate_range(
        start=max_date,
        end=max_date + pd.DateOffset(months=1),
        freq="CBM",
        holidays=bmf.holidays().holidays[2000:],
    )

    month = month.to_frame()
    month = month[0].map(lambda x: x.strftime("%Y-%m-%d")).tolist()

    # Os trades internos de cetipados aparecem apenas no relatório final,
    # por isso considero apenas o último dia útil do mês da data máxima de receita
    last_day_max_date_month = month[0]

    # Variáveis de data
    # Agora
    now = datetime.now()

    # Mês e ano atual
    year = now.year
    month = now.month
    current_month = pd.to_datetime(f"{year}-{month}-01").to_period("M")

    # Mês passado
    # Considero dia 1° porque na planilha todas as movimentações estão registrada como dia 1°
    last_month = pd.to_datetime(
        (now - relativedelta(months=1)).replace(day=1)
    ).to_period("M")

    # Identificar o mês da movimentação
    df_rv["Mes"] = pd.to_datetime(df_rv["Data Movimentação"]).dt.to_period("M")

    # Caso já tenha fechado a receita realizada do mês passado (1ª situação)
    # ou já tenha chego algum relatório desse mês (2ª situação)
    # deve-se considerar apenas os trades internos do mês atual
    if (
        max_date.strftime("%Y-%m-%d") >= last_day_max_date_month
        or max_date.to_period("M") == current_month
    ):
        filtro_data = df_rv["Mes"] == current_month

    # Caso contrário, ou seja, ainda não fechou a receita do mês passado,
    # deve-se considerar ambos os meses
    else:
        filtro_data = (df_rv["Mes"] == last_month) | (df_rv["Mes"] == current_month)

    # Filtrar o dataframe de acordo com os critérios definidos anteriormente
    df_rv = df_rv.loc[(df_rv["Ordem Executada"] == "Sim") & (filtro_data)]

    # Padronizar coluna de movimentação
    df_rv["Movimentação"] = df_rv["Movimentação"].replace({"C": "COMPRA", "V": "VENDA"})

    # Calcular receita estimada
    df_rv["Receita Estimada"] = df_rv["Receita Estimada"] * 0.5 * IMPOSTO

    # Preencher colunas
    df_rv["Tipo Receita"] = "Receita Estimada"
    df_rv["Tipo"] = "VALORES MOBILIARIOS"
    df_rv["Categoria"] = "RENDA VARIAVEL"
    df_rv["Produto"] = "CORRETAGEM BOLSA"
    df_rv["Trade Interno"] = True

    # Reordenar colunas
    df_rv = df_rv[
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
            "Receita Estimada",
            "Tipo Receita",
            "Trade Interno",
        ]
    ]

    # Definir variáveis de data
    now = datetime.now()
    date = now.date().strftime("%Y-%m-%d")
    date_aux = now.date().strftime("%Y%m%d")

    bucket_lake_silver = get_bucket_name("lake-silver")

    df_rv.to_parquet(
        f"s3://{bucket_lake_silver}/one/receita/estimativa/trades_internos_rv/date={date}/trades_internos_rv_{date_aux}.parquet"
    )
