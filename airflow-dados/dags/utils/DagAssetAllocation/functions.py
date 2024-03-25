import pandas as pd
import numpy as np
import requests
import io
import math
from datetime import datetime, timedelta
import json
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name
from utils.upload_onedrive import upload_onedrive
from utils.requests_function import http_session
from utils.mergeasof import remover_antes_entrada, fill_retroativo, mergeasof_socios
from utils.migracoes_de_base import padroniza_posicao
from utils.OneGraph.drive import OneGraphDrive
from deltalake import DeltaTable
from dateutil.relativedelta import relativedelta


def movimentacao(drive_id):
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    bucket_lake_gold = get_bucket_name("lake-gold")
    bucket_lake_bronze = get_bucket_name("lake-bronze")

    change_categoria = {
        "Ações": "RV BR",
        "FII": "RV BR",
        "Multimercados": "MM BR",
        "OF": "RV BR",
        "Renda Fixa": "Pós",
        "Fundos de Participações": "MM BR",
        "Direitos Creditórios": "MM BR",
        # 'Outros':'MM BR', # Comentei, pois não faz sentido generalizar a categoria
        "Pós-fixado": "Pós",
        "Inflação": "IPCA",
        "Pré-fixado": "Pré",
        "SELIC": "Pós",
        "DI": "Pós",
        "Saldo": "Pós",
        "PTXV": "MM IE",
        "Investimento Imobiliário": "RV BR",
        "Não Classificada": "Outros",
        "IPCA_INDEX": "IPCA",
        "PTAX": "MM IE",
        "SELIC_ACUM": "Pós",
        "IGP-M": "IPCA",
        "IGPM_INDEX": "IPCA",
        "CDI": "Pós",
        "CDIE": "Pós",
        "PRE": "Pré",
        "IGPM": "IPCA",
        "IGP": "IPCA",
        "IGP-DI": "IPCA",
        "OVER": "Pós-fixado",
        "REF": "Pós-fixado",
        "Valor em Trânsito": "Saldo",
        "CAIXA": "Saldo",
        "CRIPTO": "Alternativo",
        "CRIPTOATIVOS": "Alternativo",
        "CRYPTO": "Alternativo",
        "CRY": "Alternativo",
        "EXCLUSIVO": "Alternativo",
        "OURO": "Alternativo",
        "FIP": "RV BR",
        "DERIVATIVOS": "RV BR",
        # 'Outros': 'Alternativo',
        "002059719": "IPCA",
        "005814926": "Pós",
    }

    # get microsoft graph credentials
    secret_dict = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    microsoft_client_id = secret_dict["client_id"]
    microsoft_client_secret = secret_dict["client_secret"]

    url = "https://login.microsoftonline.com/356b3283-f55e-4a9f-a199-9acdf3c79fb7/oauth2/v2.0/token"
    payload = f"client_id={microsoft_client_id}&client_secret={microsoft_client_secret}&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default&grant_type=client_credentials"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Cookie": "fpc=AocyqXXzjRBLm2yxwKDTgJgoPQEuAQAAAL8_XdkOAAAA; stsservicecookie=estsfd; x-ms-gateway-slice=estsfd",
    }

    # Criação do Token da Microsoft
    response = requests.request("POST", url, headers=headers, data=payload)
    token = response.json()["access_token"]

    # Histórico
    folder_id = "01NZ7UAFRKH7U6OCZ7SNE35YC34Z7SD3YZ"
    url = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drives/{drive_id}/items/{folder_id}/children"

    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_hist = requests.request("GET", url, headers=headers, data=payload)
    files_hist = pd.DataFrame(files_hist.json()["value"])

    # Separar base para identificar CNPJ dos fundos
    # Banco de dados/Dados_mercado
    folder_id = "01NZ7UAFUPRFW63NGPNBA3ERDHDBWDVXJG"
    url = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drives/{drive_id}/items/{folder_id}/children"

    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_dados_mercado = requests.request("GET", url, headers=headers, data=payload)
    files_dados_mercado = pd.DataFrame(files_dados_mercado.json()["value"])

    # Banco de Dados/Outputs
    folder_id = "01NZ7UAFWNOTMQBX2DEZFJIFXDFHCKCN6B"
    url = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drives/{drive_id}/items/{folder_id}/children"

    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_outputs = requests.request("GET", url, headers=headers, data=payload)
    files_outputs = pd.DataFrame(files_outputs.json()["value"])

    # Análises/Receita/Infos
    folder_id = "01NZ7UAFQUWPLVHHA2QNA2KLTLIKPQMTE4"
    url = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drives/{drive_id}/items/{folder_id}/children"

    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_info_receita = requests.request("GET", url, headers=headers, data=payload)
    files_info_receita = pd.DataFrame(files_info_receita.json()["value"])

    # csv_pbi
    folder_hist = "01NZ7UAFX33RUBA3NRYBE3XZYVEJ3VX76P"
    url = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drives/{drive_id}/items/{folder_hist}/children"

    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_csv_pbi = requests.request("GET", url, headers=headers, data=payload)
    files_csv_pbi = pd.DataFrame(files_csv_pbi.json()["value"])

    # Base de Sócios
    df_contrata = pd.read_parquet(
        f"s3://{bucket_lake_gold}/api/one/socios/socios.parquet"
    )
    df_contrata["Data início"] = pd.to_datetime(df_contrata["Data início"])
    df_contrata["Data Saída"] = pd.to_datetime(df_contrata["Data Saída"])
    df_contrata_red = df_contrata[["Nome_bd", "Data início", "Data Saída"]]

    # Planilha de Fundos para identificar respectivos CNPJs e Categorias
    base_id = files_dados_mercado[files_dados_mercado["name"] == "Fundos.xlsx"][
        "id"
    ].iloc[0]
    url_base = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drive/items/{base_id}/content"
    payload_base = {}
    headers_base = {"Authorization": f"Bearer {token}"}
    fundos = requests.request("GET", url_base, headers=headers_base, data=payload_base)

    df_fundos_raw = pd.read_excel(io.BytesIO(fundos.content))

    df_fundos = df_fundos_raw[["Produto", "CNPJ", "Planilha_Asset"]]
    df_fundos.columns = ["Ativo", "CNPJ", "Categoria"]

    # Informações de Fundos para identificar o CNPJ
    today = datetime.today()
    dt = DeltaTable(
        f"s3://{bucket_lake_bronze}/btg/webhooks/downloads/fundos_informacao/"
    )
    df_fundos_info_raw = dt.to_pandas(
        partitions=[
            ("ano_particao", "=", str(today.year)),
            ("mes_particao", "=", str(today.month)),
        ]
    )
    df_fundos_info_raw = df_fundos_info_raw[
        df_fundos_info_raw["timestamp_dagrun"]
        == df_fundos_info_raw["timestamp_dagrun"].max()
    ]
    df_fundos_info_raw = df_fundos_info_raw[
        df_fundos_info_raw["timestamp_escrita_bronze"]
        == df_fundos_info_raw["timestamp_escrita_bronze"].max()
    ]

    # Fund Guide para identifica a Liquidez dos fundos
    base_id = files_info_receita[files_info_receita["name"] == "Fund Guide.xlsm"][
        "id"
    ].iloc[0]
    url_base = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drive/items/{base_id}/content"
    payload_base = {}
    headers_base = {"Authorization": f"Bearer {token}"}
    fund_guide = requests.request(
        "GET", url_base, headers=headers_base, data=payload_base
    )

    df_fund_guide_raw = pd.read_excel(
        io.BytesIO(fund_guide.content),
        sheet_name="Fundos Completo (B2C+Assessor)",
        skiprows=2,
    )

    df_fund_guide = df_fund_guide_raw[
        [
            "Fundo",
            "CNPJ",
            "Liquidez (Cotização + Liquidação dos Recursos Resgatados) D+",
        ]
    ]

    df_fund_guide.columns = ["Ativo", "CNPJ", "Resgate (dias)"]

    df_fund_guide["CNPJ"] = df_fund_guide["CNPJ"].replace(
        {"\.": "", "\/": "", "\-": ""}, regex=True
    )
    df_fund_guide["Resgate (dias)"] = (
        df_fund_guide["Resgate (dias)"].astype(str).str.split("+").str[-1]
    )
    df_fund_guide.loc[df_fund_guide["Resgate (dias)"] == "nan", "Resgate (dias)"] = 30

    print(df_fund_guide["Resgate (dias)"].value_counts())

    df_fund_guide["Resgate (dias)"] = df_fund_guide["Resgate (dias)"].astype(int)

    # Ler a base de cotações para identificar a categoria dos ativos
    base_id = files_outputs[
        files_outputs["name"] == "Cotacoes_fechamento_anterior.xlsx"
    ]["id"].iloc[0]
    url_base = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drive/items/{base_id}/content"
    payload_base = {}
    headers_base = {"Authorization": f"Bearer {token}"}
    cotacoes = requests.request(
        "GET", url_base, headers=headers_base, data=payload_base
    )

    df_cotacoes_raw = pd.read_excel(io.BytesIO(cotacoes.content))

    df_cotacoes = df_cotacoes_raw[["Papel"]]
    df_cotacoes.columns = ["Ativo"]

    df_cotacoes["Categoria"] = "RV BR"
    df_cotacoes["Number"] = df_cotacoes["Ativo"].str.extract("(\d+)")
    df_cotacoes.loc[
        df_cotacoes["Number"].isin(["31", "32", "33", "34", "35", "38", "39"]),
        "Categoria",
    ] = "RV IE"

    # Base de contas para identificar algumas informações dos clientes
    df_contas_raw = pd.read_parquet(f"s3://{bucket_lake_gold}/btg/contas/")
    df_contas_raw["carteira"] = df_contas_raw["carteira"].astype(str)

    df_contas = df_contas_raw[["carteira", "suitability", "pl_declarado"]]

    df_contas.columns = ["Conta", "Perfil", "PL Declarado"]

    # Ler o arquivo ContasHistorico
    id_pl = files_csv_pbi[files_csv_pbi["name"] == "ContasHistorico.csv"]["id"].iloc[0]
    url_pl = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drive/items/{id_pl}/content"
    payload_pl = {}
    headers_pl = {"Authorization": f"Bearer {token}"}
    contas_historico = requests.request(
        "GET", url_pl, headers=headers_pl, data=payload_pl
    )

    df_advisors = pd.read_csv(
        io.BytesIO(contas_historico.content), sep=";", decimal=","
    )
    df_advisors["Conta"] = df_advisors["Conta"].astype(str)
    df_advisors["Data"] = pd.to_datetime(df_advisors["Data"])

    # Asset Allocation Histórico
    df_posicao_liquidez = pd.read_parquet(
        f"s3a://{bucket_lake_gold}/api/one/asset_allocation/asset_allocation_historico.parquet"
    )

    now = datetime.now()
    date_filter = datetime(now.year - 1, 1, 1)

    df_posicao_liquidez = df_posicao_liquidez[
        (df_posicao_liquidez["Data"] >= date_filter)
    ]

    # Ler o arquivo operações_boletador
    file_id = files_hist[files_hist["name"] == "movimentacao.csv"]["id"].iloc[0]
    url = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drive/items/{file_id}/content"
    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    mov_hist = requests.request("GET", url, headers=headers, data=payload)

    df_mov_raw = pd.read_csv(io.BytesIO(mov_hist.content), sep=";", decimal=",")

    df_mov_raw["account"] = df_mov_raw["account"].astype(str)
    df_mov_raw["purchase_date"] = pd.to_datetime(df_mov_raw["purchase_date"])

    df_mov = df_mov_raw[
        (
            df_mov_raw["launch"].isin(
                [
                    "COMPRA",
                    "COMPRA DEFINITIVA",
                    "COMPRA DE COE" "VENDA",
                    "VENDA DEFINITIVA",
                    "AQ",
                    "RS",
                    "RESGATE",
                    "RESGATE TOTAL",
                ]
            )
        )
        & (df_mov_raw["purchase_date"] >= date_filter)
    ]

    df_mov = df_mov[
        [
            "account",
            "market",
            "sub_market",
            "product",
            "launch",
            "issuer",
            "stock",
            "indexer",
            "bought_coupon",
            "purchase_date",
            "maturity_date",
            "amount",
            "gross_value",
        ]
    ]

    df_mov.columns = [
        "Conta",
        "Mercado",
        "Sub_Mercado",
        "Produto",
        "Movimentação",
        "Emissor",
        "Ativo",
        "Indexador",
        "Taxa de Compra",
        "Data",
        "Data de Vencimento",
        "Quantidade",
        "Valor Bruto",
    ]

    df_mov = df_mov.merge(df_fundos, on="Ativo", how="left")

    df_mov = df_mov.merge(
        df_fundos_info_raw[["nm_fundo", "nr_cnpj", "ds_tipo_categoria_fundo"]],
        left_on="Ativo",
        right_on="nm_fundo",
        how="left",
    )

    df_mov["CNPJ"] = df_mov["CNPJ"].fillna(df_mov["nr_cnpj"]).astype(str).str.zfill(14)

    df_mov = df_mov.merge(df_fund_guide, on="CNPJ", how="left")

    df_mov["Indexador"] = df_mov["Indexador"].replace(change_categoria)
    df_mov["Categoria"] = df_mov["Categoria"].fillna(df_mov["Indexador"])
    df_mov["Ativo"] = df_mov["Ativo_x"].fillna(df_mov["Ativo_y"])

    df_mov = df_mov.merge(df_cotacoes, on="Ativo", how="left")

    df_mov["Categoria"] = (
        df_mov["Categoria_x"]
        .fillna(df_mov["Categoria_y"])
        .fillna(df_mov["ds_tipo_categoria_fundo"])
    )

    df_mov.loc[
        (df_mov["Mercado"].isin(["RENDA VARIÁVEL", "DERIVATIVOS"]))
        & (df_mov["Categoria"].isna()),
        "Categoria",
    ] = "RV BR"

    df_mov.loc[
        (df_mov["Mercado"].isin(["CRIPTOATIVOS", "CRYPTO", "CRY"]))
        & (df_mov["Categoria"].isna()),
        "Categoria",
    ] = "Alternativo"

    df_mov["Categoria"] = df_mov["Categoria"].replace(change_categoria)
    df_mov["Categoria"] = df_mov["Categoria"].fillna("Outros")
    df_mov["Categoria"] = df_mov["Categoria"].astype(str).str.upper()

    # Ler o arquivo com os valores sugeridos de alocação por categoria
    path = [
        "One Analytics",
        "Renda Fixa",
        "13 - Planilhas de Atualização",
        "Asset_Allocation.xlsx",
    ]

    df_asset_allocation_raw = drive.download_file(path=path, to_pandas=True)

    df_asset_allocation_raw["Perfil"] = (
        df_asset_allocation_raw["Perfil"].astype(str).str.upper()
    )
    df_asset_allocation_raw["Categoria"] = (
        df_asset_allocation_raw["Categoria"].astype(str).str.upper()
    )

    df_mov["Data"] = pd.to_datetime(df_mov["Data"])
    df_posicao_liquidez["Data"] = pd.to_datetime(df_posicao_liquidez["Data"])

    df_result_all = df_mov.merge(
        df_posicao_liquidez, on=["Conta", "Categoria", "Data"], how="left"
    )

    df_result_all = df_result_all.drop("PL", axis=1)

    # Adicionar o PL, tem que ser separado por conta de categorias que aparecem pela primeira vez
    df_result_all = df_result_all.merge(
        df_posicao_liquidez[["Conta", "Data", "PL"]], on=["Conta", "Data"], how="left"
    )

    df_result_all = df_result_all.merge(df_contas, on="Conta", how="left")

    # df_result_all = df_result.copy()
    df_result_all["Conta"] = df_result_all["Conta"].astype(str)

    df_result_all["Data"] = pd.to_datetime(df_result_all["Data"])
    df_result_all = df_result_all.sort_values("Data").reset_index()

    df_result_all_cp = df_result_all.copy()
    df_result_all = pd.merge_asof(
        df_result_all, df_advisors, on="Data", by="Conta", direction="backward"
    )
    temp = pd.merge_asof(
        df_result_all_cp, df_advisors, on="Data", by="Conta", direction="forward"
    )
    df_result_all = fill_retroativo(df_result_all, temp)
    df_result_all = df_result_all.sort_values("Data").reset_index(drop=True)
    df_result_all = remover_antes_entrada(df_result_all, df_contrata_red)

    df_allocation_now = df_posicao_liquidez[
        df_posicao_liquidez["Data"] == df_posicao_liquidez["Data"].max()
    ]

    df_result = df_result_all[
        [
            "Conta",
            "Nome",
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "Perfil",
            "Categoria",
            "Movimentação",
            "Produto",
            "Emissor",
            "Ativo",
            "Valor Bruto_x",
            "Data",
            "Data de Vencimento",
            "% Alocado",
            "PL",
        ]
    ]

    df_result = df_result.merge(
        df_allocation_now[["Conta", "Categoria", "% Alocado"]],
        on=["Conta", "Categoria"],
        how="left",
    )

    df_result = df_result.merge(
        df_asset_allocation_raw, on=["Perfil", "Categoria"], how="left"
    )

    # Calular % Desenquadrado no dia da aplicação
    df_result.loc[
        df_result["% Alocado_x"] < df_result["Mínimo"],
        "% Desenquadramento (Data Movimentação)",
    ] = (
        df_result["Mínimo"] - df_result["% Alocado_x"]
    )

    df_result.loc[
        df_result["% Alocado_x"] > df_result["Máximo"],
        "% Desenquadramento (Data Movimentação)",
    ] = (
        df_result["Máximo"] - df_result["% Alocado_x"]
    )

    df_result["Valor Desenquadrado (Data Movimentação)"] = (
        df_result["% Desenquadramento (Data Movimentação)"] * df_result["PL"]
    )

    # Calcular % Desenquadrado hoje
    df_result.loc[
        df_result["% Alocado_y"] < df_result["Mínimo"],
        "% Desenquadramento (Data Atual)",
    ] = (
        df_result["Mínimo"] - df_result["% Alocado_y"]
    )

    df_result.loc[
        df_result["% Alocado_y"] > df_result["Máximo"],
        "% Desenquadramento (Data Atual)",
    ] = (
        df_result["Máximo"] - df_result["% Alocado_y"]
    )

    df_result["Valor Desenquadrado (Data Atual)"] = (
        df_result["% Desenquadramento (Data Atual)"] * df_result["PL"]
    )

    # Padronizar as colunas
    df_result["Nome"] = df_result["Nome"].astype(str).str.split().str[0]

    df_result.drop(["Máximo", "Mínimo"], axis=1, inplace=True)

    df_result.rename(
        {
            "Categoria": "Classificação",
            "Valor Bruto_x": "Valor Bruto",
            "% Alocado_x": "% Alocado (Data Movimentação)",
            "% Alocado_y": "% Alocado (Data Atual)",
            "Data": "Data Movimentação",
        },
        axis=1,
        inplace=True,
    )

    df_result["Movimentação"] = df_result["Movimentação"].replace(
        {
            "RS": "RESGATE",
            "RESGATE TOTAL": "RESGATE",
            "AQ": "AQUISIÇÃO",
            "COMPRA DEFINITIVA": "COMPRA",
            "VENDA DEFINITIVA": "VENDA",
        },
        regex=True,
    )

    df_result = df_result[
        [
            "Conta",
            "Nome",
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "Perfil",
            "PL",
            "Classificação",
            "Movimentação",
            "Produto",
            "Emissor",
            "Ativo",
            "Valor Bruto",
            "Data Movimentação",
            "Data de Vencimento",
            "% Alocado (Data Movimentação)",
            "% Desenquadramento (Data Movimentação)",
            "Valor Desenquadrado (Data Movimentação)",
            "% Alocado (Data Atual)",
            "% Desenquadramento (Data Atual)",
            "Valor Desenquadrado (Data Atual)",
        ]
    ]

    df_result = df_result.drop_duplicates(["Conta", "Ativo", "Data Movimentação"])

    # Get microsoft graph credentials para api de UPLOAD
    secret_dict = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    microsoft_client_id = secret_dict["client_id"]
    microsoft_client_secret = secret_dict["client_secret"]

    tenant_id = "356b3283-f55e-4a9f-a199-9acdf3c79fb7"

    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = f"client_id={microsoft_client_id}&client_secret={microsoft_client_secret}&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default&grant_type=client_credentials"

    # Criação do Token de upload da Microsoft
    session = http_session()
    response = session.request("POST", url, data=payload)
    token_upload = response.json()["access_token"]

    # Upload em csv da Receita Estimada na pasta Banco de dados/gestao_performance
    itemId = (
        "01NZ7UAFXDPYVIN7T5KNCLKJKUSMGK3CK4"  # pasta Banco de dados/gestao_performance
    )
    file_name = "asset_allocation_movimentacao.csv"
    response_csv = upload_onedrive(df_result, file_name, token_upload, itemId)
    print(response_csv)

    df_result.to_parquet(
        f"s3a://{bucket_lake_gold}/api/one/asset_allocation/asset_allocation_movimentacao.parquet",
        index=False,
    )

    print(len(df_result))


def incremental(drive_id):
    bucket_lake_gold = get_bucket_name("lake-gold")
    bucket_lake_bronze = get_bucket_name("lake-bronze")

    change_categoria = {
        # 'Ações': 'RV BR',
        "FII": "RV BR",
        # 'Multimercados': 'MM BR',
        "OF": "RV BR",
        # 'Renda Fixa': 'Pós',
        "Fundos de Participações": "RV BR",
        # 'Direitos Creditórios': 'MM BR',
        # 'Outros':'MM BR', # Comentei, pois não faz sentido generalizar a categoria
        "Pós-fixado": "Pós",
        "Inflação": "IPCA",
        "Pré-fixado": "Pré",
        "SELIC": "Pós",
        "DI": "Pós",
        # 'Saldo': 'Pós',
        "PTXV": "Alternativo",
        "Investimento Imobiliário": "RV BR",
        "Não Classificada": "Outros",
        "IPCA_INDEX": "IPCA",
        "PTAX": "Alternativo",
        "SELIC_ACUM": "Pós",
        "IGP-M": "IPCA",
        "IGPM_INDEX": "IPCA",
        "CDI": "Pós",
        "CDIE": "Pós",
        "PRE": "Pré",
        "IGPM": "IPCA",
        "IGP": "IPCA",
        "IGP-DI": "IPCA",
        "OVER": "Pós-fixado",
        "REF": "Pós-fixado",
        "Valor em Trânsito": "Saldo",
        # 'CAIXA': 'Saldo',
        "CRIPTO": "Alternativo",
        "CRIPTOATIVOS": "Alternativo",
        "CRYPTO": "Alternativo",
        "CRY": "Alternativo",
        "EXCLUSIVO": "Alternativo",
        "OURO": "Alternativo",
        "FIP": "RV BR",
        "DERIVATIVOS": "RV BR",
        # 'Outros': 'Alternativo',
        "002059719": "IPCA",
        "005814926": "Pós",
        "CAMBIAL": "Alternativo",
    }

    # get microsoft graph credentials
    secret_dict = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    microsoft_client_id = secret_dict["client_id"]
    microsoft_client_secret = secret_dict["client_secret"]

    url = "https://login.microsoftonline.com/356b3283-f55e-4a9f-a199-9acdf3c79fb7/oauth2/v2.0/token"
    payload = f"client_id={microsoft_client_id}&client_secret={microsoft_client_secret}&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default&grant_type=client_credentials"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Cookie": "fpc=AocyqXXzjRBLm2yxwKDTgJgoPQEuAQAAAL8_XdkOAAAA; stsservicecookie=estsfd; x-ms-gateway-slice=estsfd",
    }

    # Criação do Token da Microsoft
    response = requests.request("POST", url, headers=headers, data=payload)
    token = response.json()["access_token"]

    # Pastas
    # csv_pbi
    folder_hist = "01NZ7UAFX33RUBA3NRYBE3XZYVEJ3VX76P"
    url = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drives/{drive_id}/items/{folder_hist}/children"

    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_csv_pbi = requests.request("GET", url, headers=headers, data=payload)
    files_csv_pbi = pd.DataFrame(files_csv_pbi.json()["value"])

    # Histórico
    folder_id = "01NZ7UAFRKH7U6OCZ7SNE35YC34Z7SD3YZ"
    url = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drives/{drive_id}/items/{folder_id}/children"

    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_hist = requests.request("GET", url, headers=headers, data=payload)
    files_hist = pd.DataFrame(files_hist.json()["value"])

    # Banco de Dados/Outputs
    folder_id = "01NZ7UAFWNOTMQBX2DEZFJIFXDFHCKCN6B"
    url = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drives/{drive_id}/items/{folder_id}/children"

    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_outputs = requests.request("GET", url, headers=headers, data=payload)
    files_outputs = pd.DataFrame(files_outputs.json()["value"])

    # Separar base para identificar CNPJ dos fundos
    # Banco de dados/Dados_mercado
    folder_id = "01NZ7UAFUPRFW63NGPNBA3ERDHDBWDVXJG"
    url = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drives/{drive_id}/items/{folder_id}/children"

    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_dados_mercado = requests.request("GET", url, headers=headers, data=payload)
    files_dados_mercado = pd.DataFrame(files_dados_mercado.json()["value"])

    # Novos/Planilhas fora dos novos
    folder_id = "01NZ7UAFV7X3IP7SI3ANDY2BZDU4DJONW5"
    url = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drives/{drive_id}/items/{folder_id}/children"

    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_novos_outros = requests.request("GET", url, headers=headers, data=payload)
    files_novos_outros = pd.DataFrame(files_novos_outros.json()["value"])

    # Análises/Receita/Infos
    folder_id = "01NZ7UAFQUWPLVHHA2QNA2KLTLIKPQMTE4"
    url = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drives/{drive_id}/items/{folder_id}/children"

    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_info_receita = requests.request("GET", url, headers=headers, data=payload)
    files_info_receita = pd.DataFrame(files_info_receita.json()["value"])

    # Base de Sócios
    df_contrata = pd.read_parquet(
        f"s3://{bucket_lake_gold}/api/one/socios/socios.parquet"
    )
    df_contrata["Data início"] = pd.to_datetime(df_contrata["Data início"])
    df_contrata["Data Saída"] = pd.to_datetime(df_contrata["Data Saída"])
    df_contrata_red = df_contrata[["Nome_bd", "Data início", "Data Saída"]]

    # Base de contas para identificar algumas informações dos clientes
    df_contas_raw = pd.read_parquet(f"s3://{bucket_lake_gold}/btg/contas/")
    df_contas_raw["carteira"] = df_contas_raw["carteira"].astype(str)

    df_contas = df_contas_raw[["carteira", "suitability", "pl_declarado"]]

    df_contas.columns = ["Conta", "Perfil", "PL Declarado"]

    # Ler o arquivo ContasHistorico
    id_pl = files_csv_pbi[files_csv_pbi["name"] == "ContasHistorico.csv"]["id"].iloc[0]
    url_pl = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drive/items/{id_pl}/content"
    payload_pl = {}
    headers_pl = {"Authorization": f"Bearer {token}"}
    contas_historico = requests.request(
        "GET", url_pl, headers=headers_pl, data=payload_pl
    )

    df_advisors = pd.read_csv(
        io.BytesIO(contas_historico.content), sep=";", decimal=","
    )
    df_advisors["Conta"] = df_advisors["Conta"].astype(str)
    df_advisors["Data"] = pd.to_datetime(df_advisors["Data"])

    # Ler a base de cotações para identificar a categoria dos ativos
    base_id = files_outputs[
        files_outputs["name"] == "Cotacoes_fechamento_anterior.xlsx"
    ]["id"].iloc[0]
    url_base = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drive/items/{base_id}/content"
    payload_base = {}
    headers_base = {"Authorization": f"Bearer {token}"}
    cotacoes = requests.request(
        "GET", url_base, headers=headers_base, data=payload_base
    )

    df_cotacoes = pd.read_excel(io.BytesIO(cotacoes.content))

    df_cotacoes = df_cotacoes[["Papel"]]
    df_cotacoes.columns = ["Ativo"]

    df_cotacoes["Categoria"] = "RV BR"
    df_cotacoes["Number"] = df_cotacoes["Ativo"].str.extract("(\d+)")
    df_cotacoes.loc[
        df_cotacoes["Number"].isin(["31", "32", "33", "34", "35", "38", "39"]),
        "Categoria",
    ] = "RV IE"

    # Base histórica de Asset Allocation
    df_asset_allocation = pd.read_parquet(
        f"s3a://{bucket_lake_gold}/api/one/asset_allocation/asset_allocation_historico.parquet"
    )
    max_date = pd.to_datetime(df_asset_allocation["Data"]).max().date()
    print(max_date)

    # Base de posição para identificar as alocações dia a dia
    today = datetime.today()
    dt_posicao = DeltaTable(f"s3://{bucket_lake_gold}/btg/posicao/")
    partitions = [
        [("ano_particao", "=", str(dt.year)), ("mes_particao", "=", str(dt.month))]
        for dt in [today, today - relativedelta(months=1)]
    ]
    df_posicao_raw = dt_posicao.to_pandas(filters=partitions)

    df_posicao_raw = padroniza_posicao(df_posicao_raw)

    df_posicao_raw = df_posicao_raw.loc[df_posicao_raw["date"] > max_date].reset_index(
        drop=True
    )

    df_posicao_raw["account"] = df_posicao_raw["account"].astype(str)
    df_posicao_raw["market"] = df_posicao_raw["market"].astype(str).str.upper()

    df_posicao_raw["date"] = pd.to_datetime(df_posicao_raw["date"])
    df_posicao_raw["maturity_date"] = pd.to_datetime(df_posicao_raw["maturity_date"])

    # Posição de Renda Variável
    df_posicao_rv = df_posicao_raw[(df_posicao_raw["market"] == "RENDA VARIÁVEL")]

    df_posicao_rv = df_posicao_rv[["account", "stock", "gross_value", "date"]]

    df_posicao_rv.columns = ["Conta", "Ativo", "Valor Bruto", "Data"]

    df_posicao_rv = df_posicao_rv.merge(df_cotacoes, on="Ativo", how="left")

    df_posicao_rv["Categoria"] = df_posicao_rv["Categoria"].fillna("RV BR")
    df_posicao_rv["Tipo"] = "Renda Variável"
    df_posicao_rv["Resgate (dias)"] = 2

    # Posição de Derivativso
    df_posicao_derivativo = df_posicao_raw[(df_posicao_raw["market"] == "DERIVATIVOS")]

    df_posicao_derivativo = df_posicao_derivativo[
        ["account", "product", "gross_value", "maturity_date", "date"]
    ]

    df_posicao_derivativo.columns = [
        "Conta",
        "Ativo",
        "Valor Bruto",
        "Data de Vencimento",
        "Data",
    ]

    df_posicao_derivativo["Tipo"] = "Renda Variável"
    df_posicao_derivativo["Categoria"] = "Derivativos"
    # df_posicao_derivativo['Resgate (dias)'] = df_posicao_derivativo['Data de Vencimento'].apply(lambda x: datetime.date(x) - datetime.now().date() if pd.notnull(x) else x)
    df_posicao_derivativo["Resgate (dias)"] = (
        df_posicao_derivativo["Data de Vencimento"] - df_posicao_derivativo["Data"]
    ).dt.days
    df_posicao_derivativo["Resgate (dias)"] = df_posicao_derivativo[
        "Resgate (dias)"
    ].fillna(10)
    df_posicao_derivativo["Resgate (dias)"] = df_posicao_derivativo[
        "Resgate (dias)"
    ].astype(int)

    # Planilha de Fundos para identificar respectivos CNPJs e Categorias
    base_id = files_dados_mercado[files_dados_mercado["name"] == "Fundos.xlsx"][
        "id"
    ].iloc[0]
    url_base = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drive/items/{base_id}/content"
    payload_base = {}
    headers_base = {"Authorization": f"Bearer {token}"}
    fundos = requests.request("GET", url_base, headers=headers_base, data=payload_base)

    df_fundos = pd.read_excel(io.BytesIO(fundos.content))

    df_fundos = df_fundos[["Produto", "CNPJ", "Planilha_Asset"]]
    df_fundos.columns = ["Ativo", "CNPJ", "Categoria"]

    # Fund Guide para identifica a Liquidez dos fundos
    base_id = files_info_receita[files_info_receita["name"] == "Fund Guide.xlsm"][
        "id"
    ].iloc[0]
    url_base = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drive/items/{base_id}/content"
    payload_base = {}
    headers_base = {"Authorization": f"Bearer {token}"}
    fund_guide = requests.request(
        "GET", url_base, headers=headers_base, data=payload_base
    )

    df_fund_guide = pd.read_excel(
        io.BytesIO(fund_guide.content),
        sheet_name="Fundos Completo (B2C+Assessor)",
        skiprows=2,
    )

    df_fund_guide = df_fund_guide[
        [
            "Fundo",
            "CNPJ",
            "Liquidez (Cotização + Liquidação dos Recursos Resgatados) D+",
        ]
    ]

    df_fund_guide.columns = ["Ativo", "CNPJ", "Resgate (dias)"]

    df_fund_guide["CNPJ"] = df_fund_guide["CNPJ"].replace(
        {"\.": "", "\/": "", "\-": ""}, regex=True
    )
    df_fund_guide["Resgate (dias)"] = (
        df_fund_guide["Resgate (dias)"].astype(str).str.split("+").str[-1]
    )
    df_fund_guide.loc[df_fund_guide["Resgate (dias)"] == "nan", "Resgate (dias)"] = 30
    df_fund_guide["Resgate (dias)"] = df_fund_guide["Resgate (dias)"].astype(int)

    # Posição de Fundos
    df_posicao_fundos = df_posicao_raw[(df_posicao_raw["market"] == "FUNDOS")]

    df_posicao_fundos = df_posicao_fundos[
        ["account", "product", "cnpj", "gross_value", "date"]
    ]

    df_posicao_fundos.columns = ["Conta", "Ativo", "CNPJ", "Valor Bruto", "Data"]

    df_posicao_fundos["CNPJ"] = df_posicao_fundos["CNPJ"].fillna(0)
    df_posicao_fundos["CNPJ"] = (
        df_posicao_fundos["CNPJ"]
        .astype(float)
        .astype(str)
        .str.split("\.")
        .str[0]
        .str.zfill(14)
    )

    df_fundos["CNPJ"] = df_fundos["CNPJ"].astype(str).str.zfill(14)
    df_posicao_fundos = df_posicao_fundos.merge(df_fundos, on="CNPJ", how="left")

    df_posicao_fundos = df_posicao_fundos.merge(df_fund_guide, on="CNPJ", how="left")

    df_posicao_fundos["Tipo"] = "Fundos"
    df_posicao_fundos["Categoria"] = df_posicao_fundos["Categoria"].fillna("Outros")

    df_posicao_fundos["Resgate (dias)"] = df_posicao_fundos["Resgate (dias)"].fillna(30)
    df_posicao_fundos["Resgate (dias)"] = df_posicao_fundos["Resgate (dias)"].astype(
        int
    )

    df_posicao_fundos = df_posicao_fundos[
        [
            "Conta",
            "Ativo_x",
            "CNPJ",
            "Valor Bruto",
            "Data",
            "Categoria",
            "Resgate (dias)",
            "Tipo",
        ]
    ]

    df_posicao_fundos.rename({"Ativo_x": "Ativo"}, axis=1, inplace=True)

    # Posição de Renda Fixa
    df_posicao_rf = df_posicao_raw[(df_posicao_raw["market"] == "RENDA FIXA")]

    df_posicao_rf["Resgate (dias)"] = (
        df_posicao_rf["maturity_date"] - df_posicao_rf["date"]
    ).dt.days
    df_posicao_rf["Resgate (dias)"] = df_posicao_rf["Resgate (dias)"].astype(int)

    df_posicao_rf.loc[
        (df_posicao_rf["product"] == "CDB")
        & (df_posicao_rf["amount"] > df_posicao_rf["gross_value"])
        & (df_posicao_rf["issuer"].str.upper().str.contains("BTG PACTUAL")),
        "Resgate (Dias)",
    ] = 0

    df_posicao_rf["Tipo"] = "Renda Fixa"
    df_posicao_rf["Categoria"] = df_posicao_rf["indexer"]
    df_posicao_rf.loc[
        df_posicao_rf["indexer"].astype(str).str.contains("%"), "Categoria"
    ] = (df_posicao_rf["indexer"].str.split("% ").str[-1])

    df_posicao_rf.loc[
        (df_posicao_rf["product"] == "CDB")
        & (df_posicao_rf["amount"] > df_posicao_rf["gross_value"])
        & (df_posicao_rf["issuer"].str.upper().str.contains("BTG PACTUAL")),
        "Categoria",
    ] = "Caixa"

    df_posicao_rf["Categoria"] = df_posicao_rf["Categoria"].replace(change_categoria)

    df_posicao_rf["Categoria"] = df_posicao_rf["Categoria"].fillna("Outros")

    df_posicao_rf = df_posicao_rf[
        ["account", "product", "Resgate (dias)", "gross_value", "date", "Categoria"]
    ]

    df_posicao_rf.columns = [
        "Conta",
        "Ativo",
        "Resgate (dias)",
        "Valor Bruto",
        "Data",
        "Categoria",
    ]

    # Informações de Fundos para identificar o CNPJ
    today = datetime.today()
    dt = DeltaTable(
        f"s3://{bucket_lake_bronze}/btg/webhooks/downloads/fundos_informacao/"
    )
    df_fundos_info = dt.to_pandas(
        partitions=[
            ("ano_particao", "=", str(today.year)),
            ("mes_particao", "=", str(today.month)),
        ]
    )
    df_fundos_info = df_fundos_info[
        df_fundos_info["timestamp_dagrun"] == df_fundos_info["timestamp_dagrun"].max()
    ]
    df_fundos_info = df_fundos_info[
        df_fundos_info["timestamp_escrita_bronze"]
        == df_fundos_info["timestamp_escrita_bronze"].max()
    ]

    df_fundos_info_prev = df_fundos_info[
        (
            df_fundos_info["nm_anbid_tipo"]
            .astype(str)
            .str.upper()
            .str.contains("PREVIDÊNCIA")
        )
    ]

    df_fundos_info_prev = df_fundos_info_prev[
        ["nm_fundo", "nr_cnpj", "ds_tipo_categoria_fundo"]
    ]

    df_fundos_info_prev.columns = ["Ativo", "CNPJ", "Categoria"]

    df_fundos_info_prev = df_fundos_info_prev.drop_duplicates("CNPJ")

    # Prev Guide para identificar liquidez dos fundos
    base_id = files_info_receita[files_info_receita["name"] == "Prev_Guide.xlsx"][
        "id"
    ].iloc[0]
    url_base = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drive/items/{base_id}/content"
    payload_base = {}
    headers_base = {"Authorization": f"Bearer {token}"}
    prev_guide = requests.request(
        "GET", url_base, headers=headers_base, data=payload_base
    )

    df_prev_guide = pd.read_excel(
        io.BytesIO(prev_guide.content),
        sheet_name="Fundos PREV Plataforma BTG",
        skiprows=2,
    )

    df_prev_guide = df_prev_guide[
        ["Fundo", "CNPJ", "Disponibilização dos Recursos Resgatados"]
    ]

    df_prev_guide.columns = ["Ativo", "CNPJ", "Resgate (dias)"]

    df_prev_guide["CNPJ"] = df_prev_guide["CNPJ"].replace(
        {"\.": "", "\/": "", "\-": ""}, regex=True
    )
    df_prev_guide["Resgate (dias)"] = (
        df_prev_guide["Resgate (dias)"]
        .astype(str)
        .str.split("+")
        .str[-1]
        .str.split(" du")
        .str[0]
    )
    df_prev_guide.loc[df_prev_guide["Resgate (dias)"] == "nan", "Resgate (dias)"] = 30
    df_prev_guide["Resgate (dias)"] = df_prev_guide["Resgate (dias)"].astype(int)

    # Base Prev para identificar categoria dos fundos
    base_id = files_dados_mercado[files_dados_mercado["name"] == "PREV.xlsx"][
        "id"
    ].iloc[0]
    url_base = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drive/items/{base_id}/content"
    payload_base = {}
    headers_base = {"Authorization": f"Bearer {token}"}
    prev = requests.request("GET", url_base, headers=headers_base, data=payload_base)

    df_prev = pd.read_excel(io.BytesIO(prev.content))

    df_prev = df_prev[["Produto", "CNPJ", "Classificação"]]
    df_prev.columns = ["Ativo", "CNPJ", "Categoria"]

    # Poisção de Previdência
    df_posicao_prev = df_posicao_raw[(df_posicao_raw["market"] == "PREVIDÊNCIA")]

    df_posicao_prev = df_posicao_prev[
        ["account", "cnpj", "product", "gross_value", "date"]
    ]

    df_posicao_prev.columns = ["Conta", "CNPJ", "Ativo", "Valor Bruto", "Data"]

    df_posicao_prev = df_posicao_prev.merge(df_fundos_info_prev, on="Ativo", how="left")

    df_posicao_prev["CNPJ"] = df_posicao_prev["CNPJ_x"].fillna(
        df_posicao_prev["CNPJ_y"]
    )

    df_posicao_prev["CNPJ"] = df_posicao_prev["CNPJ"].astype(str)
    df_prev["CNPJ"] = df_prev["CNPJ"].astype(str).str.zfill(14)

    df_posicao_prev["CNPJ"] = (
        df_posicao_prev["CNPJ"]
        .astype(float)
        .astype(str)
        .str.split("\.")
        .str[0]
        .str.zfill(14)
    )

    df_prev["CNPJ"] = (
        df_prev["CNPJ"].astype(float).astype(str).str.split("\.").str[0].str.zfill(14)
    )

    df_posicao_prev = df_posicao_prev.merge(df_prev, on="CNPJ", how="left")

    df_posicao_prev["Ativo"] = df_posicao_prev["Ativo_x"].fillna(
        df_posicao_prev["Ativo_y"]
    )
    df_posicao_prev["Categoria"] = df_posicao_prev["Categoria_y"].fillna(
        df_posicao_prev["Categoria_x"]
    )
    df_posicao_prev = df_posicao_prev.drop(
        ["Categoria_x", "Categoria_y", "Ativo_x", "Ativo_y", "CNPJ_x", "CNPJ_y"], axis=1
    )

    df_posicao_prev_na = df_posicao_prev[(df_posicao_prev["Categoria"].isna())]

    df_posicao_prev_na = df_posicao_prev_na.merge(df_prev, on="Ativo", how="left")

    df_posicao_prev_na["Categoria"] = df_posicao_prev_na["Categoria_y"].fillna(
        df_posicao_prev_na["Categoria_x"]
    )
    df_posicao_prev_na = df_posicao_prev_na.drop(
        ["Categoria_x", "Categoria_y", "CNPJ_x", "CNPJ_y"], axis=1
    )

    df_posicao_prev_notnull = df_posicao_prev[df_posicao_prev["Categoria"].notnull()]

    df_posicao_prev = pd.concat([df_posicao_prev_notnull, df_posicao_prev_na])

    df_posicao_prev = df_posicao_prev.merge(df_prev_guide, on="CNPJ", how="left")

    df_posicao_prev["Tipo"] = "Previdência"
    df_posicao_prev["Categoria"] = df_posicao_prev["Categoria"].fillna("Outros")
    df_posicao_prev["Resgate (dias)"] = df_posicao_prev["Resgate (dias)"].fillna(30)
    df_posicao_prev["Resgate (dias)"] = df_posicao_prev["Resgate (dias)"].astype(int)

    df_posicao_prev = df_posicao_prev[
        [
            "Conta",
            "Tipo",
            "Categoria",
            "Ativo_x",
            "CNPJ",
            "Valor Bruto",
            "Resgate (dias)",
            "Data",
        ]
    ]

    df_posicao_prev.rename({"Ativo_x": "Ativo"}, axis=1, inplace=True)

    # Posição de Conta Corrente
    df_posicao_cc = df_posicao_raw[(df_posicao_raw["market"] == "CONTA CORRENTE")]

    df_posicao_cc = df_posicao_cc[["account", "gross_value", "date"]]

    df_posicao_cc.columns = ["Conta", "Valor Bruto", "Data"]

    df_posicao_cc["Tipo"] = "Saldo"
    df_posicao_cc["Ativo"] = "Saldo"
    df_posicao_cc["Categoria"] = "Saldo"
    df_posicao_cc["Resgate (dias)"] = 0

    # # Posição de Valor em Trânsito
    # df_posicao_vt = df_posicao_raw[
    #     (df_posicao_raw['market'] == 'VALOR EM TRÂNSITO')
    # ]

    # del df_posicao_raw

    # df_posicao_vt = df_posicao_vt[[
    #     'account', 'gross_value', 'maturity_date', 'date'
    # ]]

    # df_posicao_vt.columns = ['Conta', 'Valor Bruto', 'Data de Vencimento', 'Data']

    # df_posicao_vt['Data de Vencimento'] = pd.to_datetime(df_posicao_vt['Data de Vencimento'])
    # df_posicao_vt['Data'] = pd.to_datetime(df_posicao_vt['Data'])

    # df_posicao_vt['Tipo'] = 'Valor em Trânsito'
    # df_posicao_vt['Ativo'] = 'Valor em Trânsito'
    # df_posicao_vt['Categoria'] = 'Valor em Trânsito'

    # df_posicao_vt['Resgate (dias)'] = (df_posicao_vt['Data de Vencimento'] - df_posicao_vt['Data']).dt.days
    # df_posicao_vt['Resgate (dias)'] = df_posicao_vt['Resgate (dias)'].fillna(10)
    # df_posicao_vt['Resgate (dias)'] = df_posicao_vt['Resgate (dias)'].astype(int)

    # Resultado
    df_posicao_all = pd.concat(
        [
            df_posicao_rv,
            df_posicao_derivativo,
            df_posicao_rf,
            df_posicao_fundos,
            df_posicao_prev,
            df_posicao_cc,
            # df_posicao_vt
        ]
    )

    del (df_posicao_rv,)
    del (df_posicao_derivativo,)
    del (df_posicao_rf,)
    del (df_posicao_fundos,)
    del (df_posicao_prev,)
    del (df_posicao_cc,)
    # del df_posicao_vt

    df_posicao_all["Categoria"] = df_posicao_all["Categoria"].replace(change_categoria)
    df_posicao_all["Categoria"] = df_posicao_all["Categoria"].str.upper()

    df_posicao_all["Conta"] = df_posicao_all["Conta"].astype(str)
    df_posicao_pl = df_posicao_all.groupby(
        ["Conta", "Data"], as_index=False, dropna=False
    )["Valor Bruto"].sum()

    df_posicao_pl.rename({"Valor Bruto": "PL"}, axis=1, inplace=True)

    df_posicao_grouped = df_posicao_all.groupby(
        ["Conta", "Data", "Categoria"], as_index=False, dropna=False
    )["Valor Bruto"].sum()

    df_posicao_grouped = df_posicao_grouped.merge(
        df_posicao_pl, on=["Conta", "Data"], how="left"
    )

    df_posicao_grouped["% Alocado"] = (
        df_posicao_grouped["Valor Bruto"] / df_posicao_grouped["PL"]
    )

    df_liquidez = (
        df_posicao_all[
            (df_posicao_all["Tipo"].astype(str).str.upper() != "PREVIDÊNCIA")
            & (~df_posicao_all["Categoria"].isin(["RV BR", "RV IE"]))
            & (df_posicao_all["Resgate (dias)"] < 2)
        ]
        .groupby(["Conta", "Data"], as_index=False, dropna=False)["Valor Bruto"]
        .sum()
    )

    df_liquidez.columns = ["Conta", "Data", "Caixa"]

    df_posicao_liquidez = df_posicao_grouped.merge(
        df_liquidez, on=["Conta", "Data"], how="left"
    )

    df_result = pd.concat([df_asset_allocation, df_posicao_liquidez])

    df_result.to_parquet(
        f"s3a://{bucket_lake_gold}/api/one/asset_allocation/asset_allocation_historico.parquet",
        index=False,
    )

    # Salvar no OneDrive (após o merge_asof)
    # Credenciais da Graph API da Microsoft
    df_asset_allocation = df_result[df_result["Data"] == df_result["Data"].max()]

    df_contas = df_contas_raw[["carteira", "tipo_cliente", "suitability"]]

    df_contas.columns = ["Conta", "PF/PJ", "Perfil"]

    df_contas["Conta"] = df_contas["Conta"].astype(str)

    df_asset_allocation = df_asset_allocation.merge(df_contas, on="Conta", how="left")

    # Padronizar colunas
    # Receita
    df_asset_allocation["Conta"] = df_asset_allocation["Conta"].astype(str)
    df_asset_allocation["Data"] = pd.to_datetime(df_asset_allocation["Data"])
    df_asset_allocation = df_asset_allocation.sort_values("Data")

    # Advisors
    df_advisors["Conta"] = df_advisors["Conta"].astype(str)
    df_advisors["Data"] = pd.to_datetime(df_advisors["Data"])
    df_advisors = df_advisors.sort_values("Data")

    # Executar o merge asof
    df_asset_allocation_cp = df_asset_allocation.copy()
    df_asset_allocation = pd.merge_asof(
        df_asset_allocation, df_advisors, on="Data", by="Conta", direction="backward"
    )
    temp = pd.merge_asof(
        df_asset_allocation_cp, df_advisors, on="Data", by="Conta", direction="forward"
    )
    df_asset_allocation = fill_retroativo(df_asset_allocation, temp)
    df_asset_allocation = df_asset_allocation.sort_values("Data").reset_index(drop=True)
    df_asset_allocation = remover_antes_entrada(df_asset_allocation, df_contrata_red)

    # Merge Asof para utilizar a coluna "Departamento"
    df_asset_allocation = mergeasof_socios(
        df_asset_allocation, "Comercial", "Data"
    ).drop(["Área", "Unidade de Negócios"], axis=1)

    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    bytes_file = df_asset_allocation.to_csv(sep=";", decimal=",", index=False)

    path = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "csv_pbi",
        "asset_allocation_cloud.csv",
    ]

    response = drive.upload_file(path=path, bytes=bytes_file)

    print("Upload OneDrive: ", response)


def metrica(drive_id):
    def carrega_e_limpa_base_asset_allocation(path: str) -> pd.DataFrame:
        """Esta função carrega a base de asset allocation e realiza algumas
        transformações e limpezas preliminares.

        Args:
            path (str): Path para a base de asset allocation

        Returns:
            pd.DataFrame: Data Frame com a base de asset allocation
        """

        # Colunas que serão carregadas da base.
        ASSET_ALLOCATION_COLUMNS = [
            "Conta",
            "Data",
            "Categoria",
            "Valor Bruto",
            "PL",
            "% Alocado",
        ]

        # Carrega a base.
        df_asset_allocation = pd.read_parquet(path, columns=ASSET_ALLOCATION_COLUMNS)
        df_asset_allocation = df_asset_allocation.astype({"PL": "float64"})

        # Remove contas com PL zerado
        filtro = df_asset_allocation["PL"] == 0.0
        df_asset_allocation = df_asset_allocation.loc[~filtro, :]

        # Remove registros que indicam alocação em mercados que não fazem sentido
        filtro = df_asset_allocation["Categoria"].isin(
            ["MULTIMERCADOS", "RENDA FIXA", "AÇÕES"]
        )
        df_asset_allocation = df_asset_allocation.loc[~filtro, :]

        # Remove todos os registros que não sejam segunda feira de forma que tenhamos apenas um registro por semana
        hoje = datetime.today()
        ultima_semana = hoje - timedelta(weeks=1)
        segundas_feiras = df_asset_allocation["Data"].dt.day_name() == "Monday"
        semana_corrente = df_asset_allocation["Data"] >= ultima_semana
        df_asset_allocation = df_asset_allocation.loc[
            segundas_feiras | semana_corrente, :
        ]

        # Garante que para todas as contas em todas as datas as categorias de ativos que possuem % alocado igual a 0
        # apareçam na base com 0%
        df_asset_allocation = (
            pd.pivot_table(
                data=df_asset_allocation,
                index=["Conta", "Data", "PL"],
                columns="Categoria",
                values="% Alocado",
            )
            .fillna(0)
            .reset_index()
        )

        df_asset_allocation = df_asset_allocation.melt(
            id_vars=["Conta", "Data", "PL"], value_name="% Alocado"
        )

        return df_asset_allocation

    def carrega_e_limpa_base_contas(path: str) -> pd.DataFrame:
        """Esta função carrega a base de contas da camada gold do data lake e realiza algumas
        transformações e limpezas preliminares.

        Args:
            path (str): Caminho para a base contas

        Returns:
            pd.DataFrame: Data Frame com a base contas
        """

        # Colunas que serão carregadas
        CONTAS_COLUMNS = ["carteira", "nome_completo", "suitability", "tipo_cliente"]

        # Carrega a base de dados
        df_contas = pd.read_parquet(path, columns=CONTAS_COLUMNS)

        # Define tipos e renomeia colunas
        df_contas = df_contas.astype(
            {
                "carteira": "str",
                "suitability": "str",
                "tipo_cliente": "str",
                "nome_completo": "str",
            }
        )
        df_contas = df_contas.rename(
            {
                "carteira": "Conta",
                "suitability": "Perfil",
                "tipo_cliente": "Tipo Cliente",
                "nome_completo": "Cliente",
            },
            axis=1,
        )

        # Trata a coluna Nome para que fique apenas o primeiro nome
        df_contas["Cliente"] = df_contas.apply(
            lambda x: x["Cliente"].split()[0], axis=1
        )

        # Os registros vazios são direcionados para o perfil conservador. Alinhamento feito com a equipe de Renda Fixa
        df_contas["Perfil"] = df_contas["Perfil"].fillna("CONSERVADOR")

        # Qualquer outro perfil que não for CONSERVADOR, SOFISTICADO ou MODERADO será removido
        filtro = df_contas["Perfil"].isin(["CONSERVADOR", "SOFISTICADO", "MODERADO"])
        df_contas = df_contas.loc[filtro, :].copy()

        # Renomeia alguns registros da coluna Tipo Cliente
        df_contas["Tipo Cliente"] = df_contas["Tipo Cliente"].replace(
            {
                "INVESTIDOR ESTRANGEIRO INTERNO - PF (PASSAGEIRO)": "PF",
                "INVESTIDOR ESTRANG EXTERNO - CC5": "PF",
            }
        )

        return df_contas

    def carrega_e_limpa_base_indicacao_asset_allocation(drive: str) -> pd.DataFrame:
        """Carrega e limpa a base de indicação de asset allocation que fica salva no
        onedrive com as alocações indicadas pela mesa de renda fixa.

        Args:
            drive (str): ID do drive do Onedrive onde reside o arquivo

        Returns:
            pd.DataFrame: Data Frame com a base de indicação
        """

        # Define as colunas que serão carregadas da base
        INDICACAO_COLUMNS = ["Perfil", "Categoria", "Máximo", "Mínimo", "Data"]

        # Realiza a leitura do arquivo e transforma o mesmo em um data frame
        path = [
            "One Analytics",
            "Renda Fixa",
            "13 - Planilhas de Atualização",
            "Asset_Allocation_dashboard.xlsx",
        ]

        # Ler o conteúdo do arquivo
        df_indicacao = drive.download_file(path=path, to_pandas=True)

        # Seleciona apenas as colunas desejadas
        df_indicacao = df_indicacao.loc[:, INDICACAO_COLUMNS]

        # Transforma as colunas "Categoria" e "Perfil" em caixa alta
        df_indicacao["Categoria"] = df_indicacao["Categoria"].str.upper()
        df_indicacao["Perfil"] = df_indicacao["Perfil"].str.upper()

        return df_indicacao

    def carrega_e_limpa_base_pesos(drive: str) -> dict:
        """Esta função carrega e limpa os pesos que serão aplicados às classes de ativos
        durante o cálculo da métrica. Estes pesos são definidos manualmente pelos advisors
        e são armazenados em uma pasta de trabalho excel que fica salva no onedrive.

        Args:
            drive (str): ID do drive do Onedrive onde reside o arquivo

        Returns:
            dict: Dicionário com os pesos definidos
        """
        # Realiza a leitura do arquivo e transforma o mesmo em um data frame
        path = [
            "One Analytics",
            "Renda Fixa",
            "13 - Planilhas de Atualização",
            "pesos_asset_allocation.xlsx",
        ]

        # Ler o conteúdo do arquivo
        df_pesos = drive.download_file(path=path, to_pandas=True)

        return df_pesos

    def carrega_e_limpa_base_socios(path) -> pd.DataFrame:
        """Carrega e limpa base socios.

        Args:
            path (str): Caminho para a base sócios.

        Returns:
            pd.DataFrame: Data Frame com a base sócios limpa e carregada.
        """
        df_contrata = pd.read_parquet(path)
        df_contrata["Data início"] = pd.to_datetime(df_contrata["Data início"])
        df_contrata["Data Saída"] = pd.to_datetime(df_contrata["Data Saída"])
        df_contrata_red = df_contrata[["Nome_bd", "Data início", "Data Saída"]]
        return df_contrata_red

    def carrega_e_limpa_base_contas_historico(path: str) -> pd.DataFrame:
        """Esta função carrega e limpa a base assessores.

        Args:
            path (str): Caminho para a base assessores

        Returns:
            pd.DataFrame: Data Frame com a base assessores tratada.
        """
        df_advisors = pd.read_parquet(path)

        df_advisors = df_advisors.loc[
            :,
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
            ],
        ].copy()
        df_advisors["Data"] = pd.to_datetime(df_advisors["Data"])
        df_advisors = df_advisors.sort_values("Data")

        df_advisors["Conta"] = df_advisors["Conta"].astype(str)

        return df_advisors

    def rmse_ponderado(
        categoria: pd.Series,
        alocado: pd.Series,
        maximo: pd.Series,
        minimo: pd.Series,
        peso: pd.Series = None,
    ) -> float:
        """Função para cálculo da métrica de asset allocation utilizando o RMSE tradicional. Esta função
        ainda está em etapa de teste e será revisada/ refatorada após a definição
        da métrica oficial

        Args:
            categoria (pd.Series): Categorias das alocações
            alocado (pd.Series): Valor percentual da alocação
            maximo (pd.Series): Valor máximo recomendado para determinada alocação
            minimo (pd.Series): Valor mínimo recomendado para determinada alocação
            peso (pd.Series, optional): Pesos para serem aplicados no cálculo da métrica. Defaults to None.

        Returns:
            float: Métrica calculada
        """
        # Verifica se a alocação atual está acima do limite máximo ou abaixo do limite mínimo
        # caso esteja dentro do limite a alocação passa a ser um dos limites
        limite = np.array(
            [
                u if valor > u else (l if valor < l else valor)
                for valor, l, u in zip(alocado, minimo, maximo)
            ]
        )
        # Realiza o calculo da métrica
        valor = math.sqrt(((limite - alocado) ** 2 * peso).sum())

        # Inverte o resultado para que 100 indique um resultado melhor
        valor = 1 - valor
        return valor * 100

    ## Função recebe o valor do RMSE Ponderado e o valor do exponencial ##
    def funcao_exponencial_rmse(valor: float) -> float:
        """Esta função aplica um fator exponencial de 3.5 no
        valor de entrada.

        Args:
            valor (float): Valor de entrada onde se deseja aplicar o fator exponencial.

        Returns:
            float: Valor de entrada elevado a 3.5
        """
        valor_exponencial = (np.sqrt((valor / 100) ** 7)) * 100
        return valor_exponencial

    # Ler credenciais do OneDrive
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    # Pega o nome do bucket
    bucket_lake_gold = get_bucket_name("lake-gold")
    bucket_lake_silver = get_bucket_name("lake-silver")

    # Caminho e ids para os arquivos necessários
    path_contas = f"s3://{bucket_lake_gold}/btg/contas/"
    path_asset_allocation = f"s3a://{bucket_lake_gold}/api/one/asset_allocation/asset_allocation_historico.parquet"
    path_contas_historico = f"s3a://{bucket_lake_silver}/api/btg/crm/ContasHistorico/ContasHistorico.parquet"
    path_socios = f"s3://{bucket_lake_gold}/api/one/socios/socios.parquet"
    path_save = f"s3://{bucket_lake_gold}/api/one/asset_allocation/metrica_asset_allocation/metrica_asset_allocation.parquet"

    # Carrega a base de asset allocation e contas
    df_asset_allocation = carrega_e_limpa_base_asset_allocation(path_asset_allocation)

    # Carrega a base com os pesos utilizados no cálculo do Asset Allocation
    pesos = carrega_e_limpa_base_pesos(drive)

    # Remove categorias que não possuem pesos
    filtro = df_asset_allocation["Categoria"].isin(pesos["Categoria"])
    df_asset_allocation = df_asset_allocation.loc[filtro, :]

    # Remove contas em determinadas datas onde as alocações não somam 100%
    df_temp = (
        pd.pivot_table(
            data=df_asset_allocation,
            index=["Conta", "Data"],
            columns="Categoria",
            values="% Alocado",
        )
        .fillna(0)
        .reset_index()
    )

    df_temp["Total"] = df_temp[pesos["Categoria"].unique()].sum(axis=1)

    df_asset_allocation = df_asset_allocation.merge(
        df_temp[["Conta", "Data", "Total"]], on=["Conta", "Data"], how="left"
    )
    # Arredonda a coluna Total para que o filtro a seguir possa ser feito corretamente
    df_asset_allocation["Total"] = df_asset_allocation["Total"].round(1)

    filtro = df_asset_allocation["Total"] == 1.0
    df_asset_allocation = df_asset_allocation.loc[filtro, :].drop("Total", axis=1)

    # Carrega a base contas
    df_contas = carrega_e_limpa_base_contas(path_contas)

    # Tras para a base de asset allocation as informações de suitability e tipo do cliente
    df_asset_allocation = df_asset_allocation.merge(df_contas, on="Conta", how="inner")

    # Faz o sort da base para aplicação do merge_asof
    df_asset_allocation = df_asset_allocation.sort_values("Data")

    # Os pesos mais antigos da base de pesos serão replicados de forma retroativa para todas as datas
    # da base de Asset Allocation anteriores a data mais antiga da base de pesos.
    df_temp = pd.merge_asof(
        df_asset_allocation,
        pesos,
        by=["Perfil", "Categoria"],
        on="Data",
        direction="forward",
    )

    # Tras para a base de asset allocation os pesos que serão utilizados no cálculo da métrica
    df_asset_allocation = pd.merge_asof(
        df_asset_allocation,
        pesos,
        by=["Perfil", "Categoria"],
        on="Data",
        direction="backward",
    )

    df_asset_allocation["Peso"] = df_asset_allocation["Peso"].fillna(df_temp["Peso"])

    # Carrega a base de carteiras sugeridas
    df_indicacao = carrega_e_limpa_base_indicacao_asset_allocation(drive)

    # Os Máximos e Mínimos mais antigos da base de indicações serão replicados de forma retroativa para todas as datas
    # da base de Asset Allocation anteriores a data mais antiga da base de indicações.
    df_temp = pd.merge_asof(
        df_asset_allocation,
        df_indicacao,
        by=["Perfil", "Categoria"],
        on="Data",
        direction="forward",
    )

    # Tras para a base de asset allocation as informações referentes ao máximo e mínimo sugerido para alocação
    df_asset_allocation = pd.merge_asof(
        df_asset_allocation,
        df_indicacao,
        by=["Perfil", "Categoria"],
        on="Data",
        direction="backward",
    )

    df_asset_allocation["Máximo"] = df_asset_allocation["Máximo"].fillna(
        df_temp["Máximo"]
    )
    df_asset_allocation["Mínimo"] = df_asset_allocation["Mínimo"].fillna(
        df_temp["Mínimo"]
    )

    # Para as classes de ativos que não possuem sugestão da mesa de renda fixa a sugestão é definida como 0.0
    df_asset_allocation["Máximo"] = df_asset_allocation["Máximo"].fillna(0.0)
    df_asset_allocation["Mínimo"] = df_asset_allocation["Mínimo"].fillna(0.0)
    df_asset_allocation["Peso"] = df_asset_allocation["Peso"].fillna(0.0)

    # Cria a coluna de valor alocado
    df_asset_allocation["Alocação"] = (
        df_asset_allocation["% Alocado"] * df_asset_allocation["PL"]
    )

    # Calcula o rmse ponderado
    df_rmse_ponderado = (
        df_asset_allocation.groupby(["Conta", "Data"], as_index=False)
        .apply(
            lambda x: rmse_ponderado(
                x["Categoria"], x["% Alocado"], x["Máximo"], x["Mínimo"], x["Peso"]
            )
        )
        .rename(columns={None: "RMSE_PONDERADO"})
    )

    # Copiando o dataframe com a métrica do RMSE ponderado
    df_rmse_ponderado_exponencial = df_rmse_ponderado.copy()

    # Aplicando a função e criando uma coluna chamada RMSE Exponencial
    df_rmse_ponderado_exponencial["RMSE_EXPONENCIAL"] = df_rmse_ponderado[
        "RMSE_PONDERADO"
    ].apply(lambda x: funcao_exponencial_rmse(x))
    df_rmse_ponderado_exponencial = df_rmse_ponderado_exponencial.drop(
        "RMSE_PONDERADO", axis=1
    )

    # Tras para a base de asset allocation as métricas calculadas para cada conta em cada dia
    df_asset_allocation_final = df_asset_allocation.merge(
        df_rmse_ponderado, on=["Conta", "Data"], how="left"
    )

    df_asset_allocation_final = df_asset_allocation_final.merge(
        df_rmse_ponderado_exponencial, on=["Conta", "Data"], how="left"
    )

    # Carrega bases para merge_asof
    df_contrata_red = carrega_e_limpa_base_socios(path_socios)
    df_advisors = carrega_e_limpa_base_contas_historico(path_contas_historico)

    # ------------------ Início do merge_asof ------------------ #
    df_asset_allocation_final = df_asset_allocation_final.sort_values("Data")
    df_asset_allocation_final_cp = (
        df_asset_allocation_final.copy()
    )  # Cria Backup para fill com merge_asof reverso
    df_asset_allocation_final = pd.merge_asof(
        df_asset_allocation_final,
        df_advisors,
        on="Data",
        by="Conta",
        direction="backward",
    )  # Cria o merge_asof
    temp = pd.merge_asof(
        df_asset_allocation_final_cp,
        df_advisors,
        on="Data",
        by="Conta",
        direction="forward",
    )  # Cria o merge_asof reverso

    df_asset_allocation_final = fill_retroativo(
        df_asset_allocation_final, temp
    )  # Usa o merge_asof reverso, ffill e bfill para preencher nulos
    df_asset_allocation_final = df_asset_allocation_final.sort_values(
        "Data"
    ).reset_index(
        drop=True
    )  # Ordena

    # Caso exista um registro definido para um sócio antes de sua entrada, essse registro é removido
    df_asset_allocation_final = remover_antes_entrada(
        df_asset_allocation_final, df_contrata_red
    )
    # ------------------ Fim do merge_asof ------------------ #

    # Transforma o nome das colunas em snake case
    df_asset_allocation_final.columns = [
        column.lower() for column in df_asset_allocation_final.columns
    ]
    df_asset_allocation_final.columns = df_asset_allocation_final.columns.str.replace(
        r"[ /]", "_", regex=True
    )

    # Salvando a base de dados
    df_asset_allocation_final.to_parquet(path_save)
