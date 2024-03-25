import pandas as pd
import json
import io
from datetime import datetime

from utils.OneGraph.drive import OneGraphDrive
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name

from utils.DagReceitaEstimada.funcoes_auxiliares import (
    extrair_data_maxima_da_receita,
    movimentacao_rv,
)

from utils.DagReceitaEstimada.RendaVariavel.funcoes_auxiliares import (
    top_picks,
    carteiras_recomendadas,
    estruturadas,
    derivativos,
    ordens_vac,
    tratamento_corretagem,
    tratamento_ordens,
    receita_estimada_hb,
    receita_estimada_rv,
    remover_emissoes,
)


def renda_variavel(drive_id):
    # Credenciais da Graph API da Microsoft
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    # Identificar a data do último relatório de receita contendo Renda Variável
    max_date = extrair_data_maxima_da_receita(
        tipo="VALORES MOBILIARIOS", categoria="RENDA VARIAVEL", produto="CORRETAGEM"
    )

    # Ler a base de movimentação do Histórico
    # Definir o caminho do arquivo
    path_ordens = [
        "One Analytics",
        "Banco de dados",
        "Novos",
        "ordens.zip",
    ]

    # Ler o conteúdo do arquivo
    ordens = drive.download_file(path=path_ordens).content
    df_ordens_raw = pd.read_csv(
        io.BytesIO(ordens), compression="zip", sep=",", encoding="ISO-8859-1"
    )

    # Aplicar tratamentos na base de ordens
    df_ordens_raw = tratamento_ordens(df_ordens_raw)

    df_ordens_raw = df_ordens_raw[df_ordens_raw["purchase_date"] > max_date]

    # Identificar o número de ordens no dia para distribuir a corretagem proporcionalmente
    group_columns = ["account", "stock", "launch", "purchase_date"]
    agg = {"Login": "count", "amount": "sum"}
    df_ordens_grouped_day = df_ordens_raw.groupby(group_columns, as_index=False).agg(
        agg
    )

    # Renomear as colunas
    df_ordens_grouped_day.rename(
        {"Login": "N° Ordens Operação", "amount": "amount operação"},
        axis=1,
        inplace=True,
    )

    # Identificar o número de ordens no mês para calcular a corretagem
    group_columns = ["account", "Mês"]
    agg = {"stock": "count", "amount": "sum"}
    df_ordens_grouped_month = df_ordens_raw.groupby(group_columns, as_index=False).agg(
        agg
    )

    # Renomear as colunas
    df_ordens_grouped_month.rename(
        {"stock": "N° Ordens", "amount": "amount total"}, inplace=True, axis=1
    )

    # Remover duplicadas
    df_ordens = df_ordens_raw.drop_duplicates(
        ["purchase_date", "launch", "account", "stock", "amount"]
    )

    # Identificar o número de ordens no mês em questão
    df_ordens_result = df_ordens.merge(
        df_ordens_grouped_month, on=["account", "Mês"], how="left"
    )

    # Calcular a receita estimada de ordens HB
    df_ordens_result = receita_estimada_hb(df_ordens_result, df_ordens_grouped_day)

    # Base de movimentação de renda variavel
    df_movimentacao = movimentacao_rv(drive, max_date)

    # Ler a base de corretagem do Novos
    # Definir o caminho do arquivo
    path_corretagem = [
        "One Analytics",
        "Banco de dados",
        "Novos",
        "Planilhas fora dos Novos",
        "corretagem.xlsx",
    ]

    # Ler o conteúdo do arquivo
    df_corretagem = drive.download_file(path=path_corretagem, to_pandas=True)

    # Aplicar tratamentos na base de corretagem
    df_corretagem = tratamento_corretagem(df_corretagem)
    df_corretagem = df_corretagem[
        ["account", "receita_estimada", "receita_estimada_hb"]
    ]

    # Identificar a corretagem de cada conta
    df_movimentacao = df_movimentacao.merge(df_corretagem, on="account", how="left")

    # Carteiras Recomendadas
    df_movimentacao = carteiras_recomendadas(drive, df_movimentacao)

    # Top Picks
    df_movimentacao = top_picks(drive, df_movimentacao)

    # Estruturadas
    df_movimentacao = estruturadas(drive, df_movimentacao)

    # Agrupar o dataframe de ordens,
    # pois frequentemente a operação é dividida em várias ordens
    df_ordens_grouped = df_ordens_result.groupby(
        [
            "account",
            "stock",
            "launch",
            "Login",
            "purchase_date",
            "Receita",
            "Tipo Ordem",
        ],
        as_index=False,
        dropna=False,
    ).agg(
        {
            "Receita": "sum",
            "amount": "sum",
            "amount operação": "sum",
        }
    )

    # Unir ordens e movimentação para calcular a receita estimada
    df_movimentacao = df_movimentacao.merge(
        df_ordens_grouped,
        on=["account", "stock", "launch", "purchase_date"],
        how="left",
        suffixes=("", "_ordem"),
    )

    df_movimentacao["Tipo Ordem"] = df_movimentacao["Tipo Ordem"].fillna(
        df_movimentacao["Tipo Ordem_ordem"]
    )

    # Derivativos
    df_movimentacao = derivativos(df_ordens_result, df_movimentacao)

    # Ordens VAC e VAD
    df_movimentacao = ordens_vac(df_movimentacao, df_ordens_result)

    # Calcular a receita estimada de ordens mesa
    df_movimentacao = receita_estimada_rv(df_movimentacao)

    # Ler a base de ipo do Histórico para identificar ofertas
    # Definir o caminho do arquivo
    path_ipo = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "ipo.csv",
    ]

    kwargs = {"sep": ";", "decimal": ","}

    # Ler o conteúdo do arquivo
    df_ipo_raw = drive.download_file(path=path_ipo, to_pandas=True, **kwargs)

    df_movimentacao = remover_emissoes(df_ipo_raw, df_movimentacao)

    df_movimentacao["amount"] = df_movimentacao["amount"].fillna(
        df_movimentacao["amount_ordem"]
    )

    # Preencher as colunas
    df_movimentacao.loc[
        df_movimentacao["Receita"].notnull(), "Tipo Receita"
    ] = "Receita Estimada"
    df_movimentacao["product"] = "CORRETAGEM BOLSA"
    df_movimentacao["Tipo"] = "VALORES MOBILIARIOS"
    df_movimentacao["Categoria"] = "RENDA VARIAVEL"

    df_movimentacao = df_movimentacao.drop(["Conta", "Ativo", "Quantidade"], axis=1)

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

    df_movimentacao.rename(cols_names, axis=1, inplace=True)

    # Reordenar as colunas
    df_result = df_movimentacao[
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
            "Tipo Ordem",
        ]
    ]

    # Definir variáveis de data
    now = datetime.now()
    date = now.date().strftime("%Y-%m-%d")
    date_aux = now.date().strftime("%Y%m%d")

    bucket_lake_silver = get_bucket_name("lake-silver")

    df_result.to_parquet(
        f"s3://{bucket_lake_silver}/one/receita/estimativa/renda_variavel/date={date}/renda_variavel_{date_aux}.parquet"
    )
