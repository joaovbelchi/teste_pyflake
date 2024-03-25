import pandas as pd
import numpy as np
import json
from datetime import datetime
import pandas_market_calendars as mcal
from dateutil.relativedelta import relativedelta

from utils.OneGraph.drive import OneGraphDrive
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name

from utils.DagReceitaEstimada.funcoes_auxiliares import extrair_data_maxima_da_receita


def cambio(drive_id):
    # Credenciais da Graph API da Microsoft
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    # Ler a planilha de controle de receita de câmbio
    # Definir o caminho do arquivo
    path_cambio = [
        "One Analytics",
        "Cambio",
        "Resultados_Cambio.xlsx",
    ]

    kwargs = {"sheet_name": "OPERACOES"}

    # Ler o arquivo
    df_cambio = drive.download_file(path=path_cambio, to_pandas=True, **kwargs)

    # Manter apenas colunas relevantes
    df_cambio = df_cambio.loc[
        :,
        [
            "CPF_CNPJ",
            "CLIENTE",
            "DATA_LIQUIDACAO",
            "INSTITUICAO",
            "OPERACAO",
            "Comissão",
        ],
    ]

    # Filtro para desconsiderar operações que ainda não foram fechadas
    df_cambio = df_cambio.loc[
        ((df_cambio["DATA_LIQUIDACAO"].notnull()) & (df_cambio["Comissão"].notnull()))
    ]

    # De-Para dos intermediadores
    intermedadores = {
        "Ourinvest S/A (712)": "OURIBANK",
        "Ourinvest S/A ( 712)": "OURIBANK",
        "Ouribank S/A (712)": "OURIBANK",
        "Ouribank  S/A (712)": "OURIBANK",
        "Ouribank  S.A. (712)": "OURIBANK",
        "Ouribank S.A. (712)": "OURIBANK",
        "Travelex S.A. (095)": "TRAVELEX",
        "BTG Pactual S.A. (208)": "BTG PACTUAL",
        "BANCO BS2 S.A. (218)": "BS2",
        "Confidence CC S.A. (060)": np.nan,
        "C6 S.A. (336)": np.nan,
        "XP": np.nan,
        "Banco Semear S.A (743)": np.nan,
        "HUMBERG AGRIBRASIL COMERCIO E EXPORTAÇAO DE GRÃOS  SA": np.nan,
    }

    # Padronizar o nome dos intermediadores
    df_cambio["Intermediador"] = df_cambio["INSTITUICAO"].replace(intermedadores)

    # Extrair a data máxima entre todas as categorias de câmbio para preencher valores vazios
    max_date = extrair_data_maxima_da_receita(tipo="CAMBIO")

    # Extrair a data máxima de câmbio por categoria
    df_max_date = extrair_data_maxima_da_receita(tipo="CAMBIO", return_date=False)
    df_max_date = df_max_date.sort_values("Data").drop_duplicates(
        "Intermediador", keep="last"
    )

    # Identificar a data aproximada da operação ,
    # visto que a receita pode ser contabilizda nesta data e não na data de liquidação
    # Normalmente a operção ocorre dois dias úteis antes da data de liquidação
    # Definir a data inicial a ser utilizada para contabilizar os dias úteis
    start_date = df_max_date["Data"].min()

    # Identificar os dia úteis no período: Menor data máxima até a próxima semana (D+7)
    # Estou considerando + ou - 7 dias para garantir que todas as operações recentes serão consideradas
    bmf = mcal.get_calendar("BMF")
    bmf = bmf.schedule(
        start_date=start_date - relativedelta(days=7),
        end_date=datetime.now() + relativedelta(days=7),
    )

    bmf["Data"] = pd.to_datetime(bmf["market_open"].dt.strftime("%Y-%m-%d"))

    # Calcular a data em questão menos dois dias úteis (data aproximada da operção)
    bmf["D-2 Liquidação"] = bmf.groupby(
        pd.Grouper(key="Data", freq="M"), as_index=False
    )["Data"].shift(2)
    bmf["D-2 Liquidação"] = bmf["D-2 Liquidação"].fillna(bmf["Data"])

    bmf = bmf[["Data", "D-2 Liquidação"]]

    # Identificar a data máxima por categoria
    df_cambio = df_cambio.merge(df_max_date, on="Intermediador", how="left")
    df_cambio["Data"] = df_cambio["Data"].fillna(max_date)

    df_cambio = df_cambio.rename({"Data": "max_date"}, axis=1)

    df_cambio = df_cambio.merge(
        bmf, left_on="DATA_LIQUIDACAO", right_on="Data", how="left"
    )

    # Garantir que a data da operação é maior do que a data máxima de receita
    df_cambio = df_cambio.loc[(df_cambio["D-2 Liquidação"] > df_cambio["max_date"])]

    # Ler a base ClientesAssessores para identificar a conta e os responsáveis do cliente em questão
    bucket_lake_silver = get_bucket_name("lake-silver")

    df_clientes_assessores = pd.read_parquet(
        f"s3://{bucket_lake_silver}/api/hubspot/clientesassessores/"
    )

    # Manter apenas os registros mais recentes
    df_clientes_assessores = df_clientes_assessores[
        df_clientes_assessores["REGISTRO"] == df_clientes_assessores["REGISTRO"].max()
    ]

    # Padronizar o nome do cliente
    df_cambio["Nome_upper"] = df_cambio["CLIENTE"].astype(str).str.upper()
    df_cambio["Nome_upper"] = df_cambio["Nome_upper"].apply(
        lambda x: (
            " ".join(x.split(" ")[:-1])
            if x.split(" ")[-1] in ["S.A", "S.A.", "SA", "S A", "LTDA", "LTDA."]
            else x
        )
    )

    df_clientes_assessores["Nome_upper"] = (
        df_clientes_assessores["Nome"].astype(str).str.upper()
    )
    df_clientes_assessores["Nome_upper"] = df_clientes_assessores["Nome_upper"].apply(
        lambda x: (
            " ".join(x.split(" ")[:-1])
            if x.split(" ")[-1] in ["S.A", "S.A.", "SA", "S A", "LTDA", "LTDA."]
            else x
        )
    )

    # Unir a base de operações com ClientesAssessores
    df_cambio = df_cambio.merge(df_clientes_assessores, on="Nome_upper", how="left")

    # Remover possíveis linhas duplicadas após o merge
    df_cambio = df_cambio.drop_duplicates(
        [
            "CPF_CNPJ",
            "CLIENTE",
            "DATA_LIQUIDACAO",
            "INSTITUICAO",
            "OPERACAO",
            "Comissão",
            "Intermediador",
            "Tipo",
            "Categoria",
            "Produto_1",
            "Data",
        ]
    )

    df_cambio["Data Movimentação"] = pd.to_datetime(
        df_cambio["DATA_LIQUIDACAO"], errors="coerce"
    )

    # Padronizar as colunas
    df_cambio["Conta"] = df_cambio["Conta"].astype(str).str.split(".").str[0]
    df_cambio["Tipo"] = "CAMBIO"
    df_cambio["Tipo Receita"] = "Receita Estimada"

    df_cambio = df_cambio.drop("Categoria", axis=1)

    # Renomear as colunas
    cols_names = {
        "Intermediador": "Categoria",
        "Comissão": "Receita Estimada",
    }

    df_cambio.rename(cols_names, axis=1, inplace=True)

    # Organizar as colunas
    df_cambio = df_cambio[
        [
            "Conta",
            "Tipo",
            "Categoria",
            "Data Movimentação",
            "Receita Estimada",
            "Tipo Receita",
        ]
    ]

    # Definir variáveis de data
    now = datetime.now()
    date = now.date().strftime("%Y-%m-%d")
    date_aux = now.date().strftime("%Y%m%d")

    df_cambio.to_parquet(
        f"s3://{bucket_lake_silver}/one/receita/estimativa/cambio/date={date}/cambio_{date_aux}.parquet"
    )
