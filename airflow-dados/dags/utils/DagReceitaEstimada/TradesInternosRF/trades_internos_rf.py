import pandas as pd
import io
import json
from datetime import datetime

from utils.OneGraph.drive import OneGraphDrive
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name

from utils.DagReceitaEstimada.funcoes_auxiliares import extrair_data_maxima_da_receita


def trades_internos_rf(drive_id):
    # Credenciais da Graph API da Microsoft
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    max_date = extrair_data_maxima_da_receita(
        tipo="VALORES MOBILIARIOS", categoria="RENDA FIXA", produto="SECUNDARIO"
    )

    # Ler a base de ipo do Histórico para identificar ofertas
    # Definir o caminho do arquivo
    path_rf = [
        "One Analytics",
        "Renda Fixa",
        "09 - Intermediação",
        "Run Interno",
        "RUN CRÉDITO PRIVADO.xlsx",
    ]

    # Ler o conteúdo do arquivo e transformar em dataframe
    # A planilha possui duas abas, por isso não transformo direto em dataframe
    rf = drive.download_file(path=path_rf).content

    df_rf = pd.DataFrame()
    for sheet_name in ["HISTORICO CP 2024", "HISTORICO BANCÁRIOS 2024"]:
        # Transformar em dataframe
        df = pd.read_excel(io.BytesIO(rf), sheet_name=sheet_name)

        # Se for crédito corporativo, a coluna empresa equivale ao produto
        if sheet_name == "HISTORICO BANCÁRIOS 2024":
            df.rename({"EMPRESA": "Produto"}, axis=1, inplace=True)

        # Se for crédito corporativo, é preciso identificar o produto
        elif sheet_name == "HISTORICO CP 2024":
            df["Produto"] = "DEB"

            df.loc[df["TICKER"].astype(str).str.startswith("CRA"), "Produto"] = "CRA"
            df.loc[df["TICKER"].astype(str).str.startswith("CRI"), "Produto"] = "CRI"

        # Concatenar as bases
        df_rf = pd.concat([df, df_rf])

    # Filtrar apenas o que foi liquidado após a data da última receita de renda fixa
    df_rf = df_rf[(df_rf["STATUS"] == "OK") & (df_rf["DATA"] > max_date)]

    df_rf = df_rf.reset_index(drop=True)

    # Cada linha possui duas contas (comprador e vendedor),
    # por isso é necessário transformar cada linha em duas
    # Definir as variáveis fixas
    id_vars = [
        "DATA",
        "EMPRESA",
        "TICKER",
        "Produto",
        "QTD",
        "RECEITA POR COMERCIAL",
        "ROA LÍQUIDO OPERAÇÃO",
    ]

    # Fazer o melt na coluna da conta
    df_rf_contas = df_rf.melt(
        id_vars=id_vars,
        value_vars=[
            "CONTA VENDEDOR",
            "CONTA COMPRADOR",
        ],
        ignore_index=False,
        var_name="Direção Conta",
        value_name="Conta",
    )

    df_rf_contas = df_rf_contas.reset_index(drop=False)

    # Fazer o melt na coluna da volume (valor bruto)
    df_rf_volume = df_rf.melt(
        id_vars=id_vars,
        value_vars=[
            "VOLUME VENDEDOR",
            "VOLUME COMPRADOR",
        ],
        ignore_index=False,
        var_name="Direção Volume",
        value_name="Valor Bruto",
    )

    df_rf_volume = df_rf_volume.reset_index(drop=False)

    # Concatenar as bases a partir do index
    # Não fiz um merge, pois não há dados únicos em comum
    df_rf = pd.concat(
        [
            df_rf_contas.sort_values("index"),
            df_rf_volume[["Direção Volume", "Valor Bruto", "index"]].sort_values(
                "index"
            ),
        ],
        axis=1,
    )

    # Tratar as colunas
    df_rf["Conta"] = df_rf["Conta"].astype(str).str.split(".").str[0]
    df_rf["Movimentação"] = df_rf["Direção Conta"].replace(
        {"CONTA VENDEDOR": "VENDA", "CONTA COMPRADOR": "COMPRA"}
    )

    # Preencher as colunas
    df_rf["Tipo Receita"] = "Receita Estimada"
    df_rf["Tipo"] = "VALORES MOBILIARIOS"
    df_rf["Categoria"] = "RENDA FIXA"
    df_rf["Trade Interno"] = True

    # Renomear as colunas
    cols_names = {
        "DATA": "Data Movimentação",
        "EMPRESA": "Emissor",
        "TICKER": "Ativo",
        "QTD": "Quantidade",
        "RECEITA POR COMERCIAL": "Receita Estimada",
        "ROA LÍQUIDO OPERAÇÃO": "Repasse",
    }

    df_rf.rename(cols_names, axis=1, inplace=True)

    # Reordenar as colunas
    df_rf = df_rf[
        [
            "Conta",
            "Tipo",
            "Categoria",
            "Produto",
            "Movimentação",
            "Emissor",
            "Ativo",
            "Data Movimentação",
            "Quantidade",
            "Valor Bruto",
            "Repasse",
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

    df_rf.to_parquet(
        f"s3://{bucket_lake_silver}/one/receita/estimativa/trades_internos_rf/date={date}/trades_internos_rf_{date_aux}.parquet"
    )
