import pandas as pd
import json
from datetime import datetime

from utils.OneGraph.drive import OneGraphDrive
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name

from utils.DagReceitaEstimada.funcoes_auxiliares import extrair_data_maxima_da_receita


def estruturadas(drive_id):
    # Credenciais da Graph API da Microsoft
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    # Identificar a data do último relatório de receita contendo estruturadas
    max_date = extrair_data_maxima_da_receita(
        tipo="VALORES MOBILIARIOS",
        categoria="RENDA VARIAVEL",
        produto="OPÇAO ESTRUTURADA",
    )

    IMPOSTO = 0.9035

    # Ler a base de ipo do Histórico para identificar ofertas
    # Definir o caminho do arquivo
    path_estruturadas = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "dados_estruturadas.csv",
    ]

    kwargs = {"sep": ";", "decimal": ","}

    # Ler o conteúdo do arquivo e transformar em dataframe
    df_estruturadas = drive.download_file(
        path=path_estruturadas, to_pandas=True, **kwargs
    )

    # Transformar o tipo das colunas
    df_estruturadas["Conta"] = df_estruturadas["Conta"].astype(str)
    df_estruturadas["Data de Início"] = pd.to_datetime(
        df_estruturadas["Data de Início"]
    )

    # Manter apenas os registros após o último relatório de receita
    df_estruturadas = df_estruturadas[(df_estruturadas["Data de Início"] > max_date)]

    # Remover opções duplicadas
    df_estruturadas = df_estruturadas.drop_duplicates(
        [
            "Conta",
            "Nome do Produto",
            "Ativo",
            "Data de Início",
            "Data de Vencimento",
            "Tipo Operação",
            "Direção",
            "Valor da Reseva (R$)",
        ]
    )

    # Agrupar o dataframe, pois cada opção vem em uma linha, apesar de pertencerem a mesma estrutura
    df_estruturadas = df_estruturadas.groupby(
        ["Conta", "Nome do Produto", "Ativo", "Data de Início", "Data de Vencimento"],
        as_index=False,
        dropna=False,
    ).agg({"Quantidade": "max", "Fee (R$)": "sum"})

    # Calcular a receita
    # Receita gerada considerando repasse para a One e impostos
    df_estruturadas["Receita Estimada"] = df_estruturadas["Fee (R$)"] * 0.5 * IMPOSTO

    # Mudar a receita negativas para positivo
    df_estruturadas.loc[df_estruturadas["Receita Estimada"] < 0, "Receita Estimada"] = (
        df_estruturadas["Receita Estimada"] * -1
    )

    # Padronizar as colunas
    df_estruturadas["Tipo Receita"] = "Receita Estimada"
    df_estruturadas["Produto"] = "OPÇAO ESTRUTURADA"
    df_estruturadas["Categoria"] = "RENDA VARIAVEL"
    df_estruturadas["Tipo"] = "VALORES MOBILIARIOS"

    # Organizar as colunas
    df_estruturadas = df_estruturadas.drop(["Fee (R$)"], axis=1)

    # Renomear as colunas
    df_estruturadas.rename(
        {
            "Data de Início": "Data Movimentação",
            "Nome do Produto": "Estrutura",
        },
        axis=1,
        inplace=True,
    )

    # Definir variáveis de data
    now = datetime.now()
    date = now.date().strftime("%Y-%m-%d")
    date_aux = now.date().strftime("%Y%m%d")

    bucket_lake_silver = get_bucket_name("lake-silver")

    df_estruturadas.to_parquet(
        f"s3://{bucket_lake_silver}/one/receita/estimativa/estruturadas/date={date}/estruturadas_{date_aux}.parquet"
    )
