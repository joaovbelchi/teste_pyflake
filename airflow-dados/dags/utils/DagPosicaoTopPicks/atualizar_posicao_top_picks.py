# Bibliotecas
import json
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal
from datetime import datetime
import io
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name
from utils.OneGraph.drive import OneGraphDrive


def atualizar_posicao_top_picks(drive_id):
    bucket_lake_gold = get_bucket_name("lake-gold")

    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    # Ler a base de movimentação do Histórico
    # Definir o caminho do arquivo
    path_movimentacao = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "movimentacao.csv",
    ]

    # Definir parâmetros para leitura do arquivo
    kwargs = {"sep": ";", "decimal": ","}

    # Ler o conteúdo do arquivo, já convertendo em dataframe
    df_movimentacao = drive.download_file(
        path=path_movimentacao, to_pandas=True, **kwargs
    )

    df_movimentacao["account"] = df_movimentacao["account"].astype(str)

    # Ler a planilha Ativos Top Picks
    # Definir o caminho do arquivo
    path_ativos_top_picks = [
        "One Analytics",
        "Planilha Mesa RV",
        "Arquivos Boletador Top Picks",
        "Ativos Top Picks.xlsx",
    ]
    # Ler o conteúdo do arquivo e converter em dataframe
    df_top_picks = drive.download_file(path=path_ativos_top_picks, to_pandas=True)

    df_top_picks_entradas = df_top_picks[
        (df_top_picks["Alocação Anterior"] == 0)
        | (df_top_picks["Alocação Anterior"].isna())
    ]

    df_top_picks_saidas = df_top_picks[
        (df_top_picks["Alocação Atual"] == 0) | (df_top_picks["Alocação Atual"].isna())
    ]

    # Ler a Base de Sócios
    df_contrata = pd.read_parquet(
        f"s3://{bucket_lake_gold}/api/one/socios/socios.parquet"
    )

    # Aplicar tratamentos nas colunas de data
    df_contrata["Data início"] = pd.to_datetime(df_contrata["Data início"])
    df_contrata["Data Saída"] = pd.to_datetime(df_contrata["Data Saída"])

    # Lista de traders para filtrar as bases
    operacionais_rv = df_contrata[
        (df_contrata["Área"].astype(str).str.contains("RV"))
        & (df_contrata["Data Saída"].isna())
        & (df_contrata["sub_area"] != "INTELIG. ESTRATÉGICA")
    ]["Nome_bd"].unique()

    folders_rv = [f"Operação - {trader}" for trader in operacionais_rv]

    df_inicio = pd.DataFrame()
    for folder in folders_rv:
        # Ler o arquivo Clientes Top Picks na pasta do trader
        path = [
            "One Analytics",
            "Inteligência Operacional",
            "4 - Base - Receita",
            folder,
            "Boletador",
            "Top Picks",
            "Clientes Top Picks.xlsx",
        ]
        try:
            # Definir parâmetros para leitura do arquivo
            kwargs = {"sheet_name": "Clientes"}

            # Ler o conteúdo do arquivo e converter em dataframe
            df_clientes_top_picks = drive.download_file(
                path=path, to_pandas=True, **kwargs
            )

            df_clientes_top_picks = df_clientes_top_picks.dropna(axis=1, how="all")
            df_clientes_top_picks = df_clientes_top_picks.dropna(axis=0, how="all")

            # Concatenar a base do trader com as demais
            df_inicio = pd.concat([df_inicio, df_clientes_top_picks])

            print(folder, ": Concluido!")

        except ValueError:
            print(folder, ": Arquivo não encontrado")

        except Exception as e:
            print(e)

    # Definir as colunas que devem ser mantidas no dataframe (colunas que não são ativos)
    colunas_fixas = [
        "Conta",
        "Caixa",
        "Operacional RV",
        "Nome do Cliente",
    ]

    # Identificar se existem outros ativos que estão saindo da carteira da estratégia
    for column in df_inicio.columns:
        if (
            column not in df_top_picks["Ativo"].to_list()
            and column not in colunas_fixas
        ):
            df = pd.DataFrame({"Ativo": [column]})
            df_top_picks_saidas = pd.concat([df_top_picks_saidas, df])

    # Os ativos que estão entrando na carteira da estratégia na semana atual
    # devem começar zerados, preencho a quantidade deles como nula
    for ativo in df_top_picks_entradas["Ativo"].to_list():
        df_inicio[ativo] = np.nan

    # Definir quais são as colunas que correspondem aos ativos
    value_cols = df_inicio.columns.to_list()
    value_cols.remove("Conta")
    value_cols.remove("Operacional RV")

    # Algumas vezes essa coluna aparece, por isso é necessário remove-la
    if "Coluna1" in value_cols:
        value_cols.remove("Coluna1")

    # Remover os ativos que estão saindo da estratégia
    for ativo in df_top_picks_saidas["Ativo"].to_list():
        if ativo in value_cols:
            value_cols.remove(ativo)

    # Inverter o dataframe para fazer o merge
    df_inicio_melted = df_inicio.melt(
        id_vars=["Conta", "Caixa", "Nome do Cliente"], value_vars=value_cols
    )

    # Aplicar tratamentos nas colunas
    df_inicio_melted["Conta"] = (
        df_inicio_melted["Conta"].astype(str).str.split("\.").str[0]
    )
    df_inicio_melted["value"] = df_inicio_melted["value"].fillna(0)

    # Identificar o primeiro dia útil da semana
    bmf = mcal.get_calendar("BMF")
    bmf = bmf.schedule(
        start_date=datetime.now() - pd.Timedelta(days=7),
        end_date=f"{datetime.now().year}-12-31",
    )

    bmf = bmf[["market_open"]].reset_index(drop=True)
    bmf = bmf.rename(columns={"market_open": "Data"})

    # Selecionar o primeiro dia útil de cada semana
    bmf["week_number"] = bmf["Data"].dt.isocalendar().week
    bmf["Data"] = pd.to_datetime(bmf["Data"]).dt.date
    bmf = bmf.drop_duplicates("week_number", keep="first")

    # Identificar e filtrar a semana atual
    week_of_year = pd.Period(datetime.now(), freq="D").weekofyear
    bmf = bmf[bmf["week_number"] == week_of_year]

    # Extrair o primeiro dia útil da semana atual
    bmf["Data"] = pd.to_datetime(bmf["Data"])
    bmf["Data"] = bmf["Data"].map(lambda x: x.strftime("%Y-%m-%d"))
    start_of_week = bmf.iloc[0][0]

    df_inicio_melted["Conta"] = df_inicio_melted["Conta"].astype(str)
    df_movimentacao["account"] = df_movimentacao["account"].astype(str)

    # Identificar as operações que ocorreram na semana atual
    df_movimentacao_last_week = df_movimentacao[
        (df_movimentacao["purchase_date"] >= start_of_week)
        & (df_movimentacao["market"] == "RENDA VARIÁVEL")
        & (df_movimentacao["sub_market"] == "ACOES")
        & (df_movimentacao["launch"].isin(["VENDA", "COMPRA"]))
        & (df_movimentacao["account"].isin(df_inicio_melted["Conta"]))
    ]

    # Unir posição em top picks com as movimentações da semana
    df_result = df_movimentacao_last_week.merge(
        df_inicio_melted,
        left_on=["account", "stock"],
        right_on=["Conta", "variable"],
        how="right",
    )

    df_result.loc[df_result["launch"] == "VENDA", "amount"] = df_result["amount"] * -1
    df_result["amount"] = df_result["amount"].fillna(0)

    # Calcular a quantidade que o cliente possui atualmente no ativo
    df_result["Quantidade"] = df_result["amount"].astype(float) + df_result[
        "value"
    ].astype(float)

    df_result = df_result[
        ["Nome do Cliente", "Conta", "Caixa", "variable", "Quantidade"]
    ]
    df_result.rename({"variable": "Ativo"}, axis=1, inplace=True)

    df_result["Nome do Cliente"] = df_result["Nome do Cliente"].fillna("")
    df_result["Caixa"] = df_result["Caixa"].fillna(0)

    # Voltar o dataframe para o formato utilizado
    df_result = pd.pivot_table(
        df_result,
        values="Quantidade",
        columns="Ativo",
        index=["Nome do Cliente", "Conta", "Caixa"],
    )
    df_result = df_result.reset_index(drop=False)

    # Identificar o Trader atual da conta
    df_clientes_assessores = pd.read_parquet(
        "s3://prd-dados-lake-gold-oneinvest/api/hubspot/ClientesAssessores_novos/ClientesAssessores_novos.parquet"
    )
    df_clientes_assessores["Conta"] = df_clientes_assessores["Conta"].astype(str)

    df_result = df_result.merge(
        df_clientes_assessores[["Conta", "Operacional RV"]], on="Conta", how="left"
    )

    for folder in folders_rv:
        # Definir o trader
        trader = folder.split("Operação - ")[-1]

        # Filtrar a base do trader
        df = df_result[df_result["Operacional RV"] == trader]

        with io.BytesIO() as buffer:
            df.to_excel(buffer, index=False)
            bytes_file = buffer.getvalue()

        # Definir o caminho em que o arquivo será salvo
        path = [
            "One Analytics",
            "Inteligência Operacional",
            "4 - Base - Receita",
            folder,
            "Outputs",
            "Posicao_Clientes_Top_Picks.xlsx",
        ]

        # Fazer upload do arquivo
        try:
            response = drive.upload_file(path=path, bytes=bytes_file)
        except Exception as e:
            print(e)

        print(trader, response)
