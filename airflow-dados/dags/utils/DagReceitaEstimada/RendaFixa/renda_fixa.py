import pandas as pd
import json
from datetime import datetime

from utils.OneGraph.drive import OneGraphDrive
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name

from utils.DagReceitaEstimada.funcoes_auxiliares import (
    extrair_data_maxima_da_receita,
    tratamento_movimentacao,
    posicao,
)

from utils.DagReceitaEstimada.RendaFixa.funcoes_auxiliares import (
    SCHEMA_CREDITO_BANCARIO,
    SCHEMA_CREDITO_CORPORATIVO,
    SCHEMA_TITULO_PUBLICO,
    leitura_run,
    titulos_publicos,
    credito_corporativo,
    credito_bancario,
    coe,
    cdb_liquidez_diaria,
    compromissadas,
)


def renda_fixa(drive_id):
    # Credenciais da Graph API da Microsoft
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    # Identificar a data máxima de Renda Fixa
    max_date = extrair_data_maxima_da_receita(
        tipo="VALORES MOBILIARIOS", categoria="RENDA FIXA", produto="SECUNDARIO"
    )

    # Ler a base de movimentação do Histórico
    # Definir o caminho do arquivo
    path_movimentacao = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "movimentacao.csv",
    ]

    # Ler o conteúdo do arquivo
    kwargs_read_csv = {"sep": ";", "decimal": ","}
    df_movimentacao_raw = drive.download_file(
        path=path_movimentacao, to_pandas=True, **kwargs_read_csv
    )

    # Padronizar base de movimentação
    df_movimentacao_raw = tratamento_movimentacao(
        df_movimentacao_raw,
        duplicated_cols=[
            "account",
            "stock",
            "indexer",
            "maturity_date",
            "purchase_date",
        ],
    )

    # A Receita de compromissadas é diária, por isso é necessário calcular a receita separadamente
    # Movimentações DESCONSIDERANDO compromissadas
    df_movimentacao = df_movimentacao_raw[
        ~df_movimentacao_raw["launch"].astype(str).str.contains("REVENDA")
    ]

    # Calcular a receita de títulos públicos
    # Ler o RUN do s3
    df_run_titulo_publico = leitura_run(
        path="one/run/credito_bancario/titulo_publico", schema=SCHEMA_TITULO_PUBLICO
    )

    # Aplicar função para calcular a receita
    df_titulos_publicos = titulos_publicos(
        df_movimentacao, df_run_titulo_publico, max_date
    )

    # Calcular a receita de Crédito Bancário
    # Ler o RUN do s3
    df_run_credito_bancario = leitura_run(
        path="one/run/credito_bancario/credito_bancario", schema=SCHEMA_CREDITO_BANCARIO
    )

    # Ler a planilha "ativos_bancarios" para padronizar os emissores
    # Definir o caminho do arquivo
    path_emissores = [
        "One Analytics",
        "Banco de dados",
        "Novos",
        "ativos_bancario.xlsx",
    ]

    # Ler o conteúdo do arquivo
    df_emissores = drive.download_file(path=path_emissores, to_pandas=True)

    # Definir as variáveis para envio do e-mail de ativos ausentes
    kwargs = {
        "drive_id": drive_id,
        "item_id": "d64bcd7f-c47a-4343-b0b4-73f4a8e927a5",  # tech@investimentos.one
        "destinatarios": [
            "back.office@investimentos.one",
            "nathalia.montandon@investimentos.one",
            "samuel.machado@investimentos.one",
        ],
        "titulo": "[Receita Estimada] Emissores Ausentes - Crédito Bancário",
        "previa": "Preencher emissores de crédito bancário",
    }

    # Aplicar função para calcular a receita e enviar e-mail, se for o caso
    df_credito_bancario = credito_bancario(
        df_movimentacao_raw=df_movimentacao,
        df_receita=df_run_credito_bancario,
        df_emissores=df_emissores,
        max_date=max_date,
        **kwargs,
    )

    # Calcular a receita de Crédito Corporativo
    # Ler o RUN do s3
    df_run_credito_corporativo = pd.DataFrame()
    for i in ["cricra", "debentures_isentas", "debentures_nao_isentas"]:
        df = leitura_run(
            path=f"one/run/credito_corporativo/{i}", schema=SCHEMA_CREDITO_CORPORATIVO
        )

        df_run_credito_corporativo = pd.concat([df_run_credito_corporativo, df])

    # Ler a base de ipo do Histórico para identificar ofertas
    # Definir o caminho do arquivo
    path_ipo = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "ipo.csv",
    ]

    # Ler o conteúdo do arquivo
    df_ipo_raw = drive.download_file(path=path_ipo, to_pandas=True, **kwargs_read_csv)

    # Aplicar função para calcular a receita
    df_credito_corporativo = credito_corporativo(
        df_movimentacao, df_ipo_raw, df_run_credito_corporativo, max_date
    )

    # Ler a base de posição e manter apenas os registros após a data máxima de receita de Renda Fixa
    df_posicao = posicao(max_date)
    df_posicao["data_interface"] = pd.to_datetime(df_posicao["data_interface"])
    df_posicao = df_posicao[(df_posicao["data_interface"] > max_date)]

    # CDB Liquidez Diária
    df_cdb_liquidez_diaria = cdb_liquidez_diaria(df_posicao)

    # Compromissadas
    df_compromissadas = compromissadas(df_posicao)

    # Calcular a receita de COE
    df_coe = coe(df_movimentacao, max_date)

    # Concatenar os dataframes
    dfs = [
        df_titulos_publicos,
        df_credito_bancario,
        df_credito_corporativo,
        df_cdb_liquidez_diaria,
        df_compromissadas,
        df_coe,
    ]
    df_result = pd.concat(dfs)

    # Renomear as colunas
    cols_names = {
        "account": "Conta",
        "product": "Produto",
        "launch": "Movimentação",
        "issuer": "Emissor",
        "stock": "Ativo",
        "purchase_date": "Data Movimentação",
        "maturity_date": "Data Vencimento",
        "amount": "Quantidade",
        "gross_value": "Valor Bruto",
        "receita_estimada": "ROA",
        "repasse": "Repasse",
        "Receita": "Receita Estimada",
    }

    df_result.rename(cols_names, axis=1, inplace=True)

    # Reordenar as colunas
    df_result = df_result[
        [
            "Conta",
            "Tipo",
            "Categoria",
            "Produto",
            "Movimentação",
            "Emissor",
            "Ativo",
            "Data Movimentação",
            "Data Vencimento",
            "Quantidade",
            "Valor Bruto",
            "ROA",
            "Repasse",
            "Receita Estimada",
            "Tipo Receita",
        ]
    ]

    df_result["Data Vencimento"] = pd.to_datetime(df_result["Data Vencimento"])

    # Definir variáveis de data
    now = datetime.now()
    date = now.date().strftime("%Y-%m-%d")
    date_aux = now.date().strftime("%Y%m%d")

    bucket_lake_silver = get_bucket_name("lake-silver")

    df_result.to_parquet(
        f"s3://{bucket_lake_silver}/one/receita/estimativa/renda_fixa/date={date}/renda_fixa_{date_aux}.parquet"
    )
