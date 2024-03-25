# Bibliotecas
import pandas as pd
import numpy as np
import requests
import io
from datetime import datetime
import boto3
import json

from utils.OneGraph.drive import OneGraphDrive
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name
from utils.mergeasof import fill_retroativo, remover_antes_entrada
from utils.requests_function import *


def banco_de_dados_rv():
    session = http_session()

    bucket_lake_gold = get_bucket_name("lake-gold")
    bucket_whatsapp = get_bucket_name("whatsapp")

    # get microsoft graph credentials
    secret_dict = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    microsoft_client_id = secret_dict["client_id"]
    microsoft_client_secret = secret_dict["client_secret"]

    tenant_id = "356b3283-f55e-4a9f-a199-9acdf3c79fb7"

    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = f"client_id={microsoft_client_id}&client_secret={microsoft_client_secret}&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default&grant_type=client_credentials"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Cookie": "fpc=AocyqXXzjRBLm2yxwKDTgJgoPQEuAQAAAL8_XdkOAAAA; stsservicecookie=estsfd; x-ms-gateway-slice=estsfd",
    }

    # Criação do Token da Microsoft
    response = session.request("POST", url, headers=headers, data=payload)
    token = response.json()["access_token"]
    drive_id = "b!k-8TlF8nU0qruYdk5YJWRl3CDlGvRCVMvFqk-P2Bh4WkINmc7OQrSIcb_LUwG_M2"

    user_id = "e24ccf9b-82cf-4e41-930c-70606d1a948d"

    # csv_pbi
    folder_hist = "01NZ7UAFX33RUBA3NRYBE3XZYVEJ3VX76P"
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{folder_hist}/children"
    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_csv_pbi = session.request("GET", url, headers=headers, data=payload)
    files_csv_pbi = pd.DataFrame(files_csv_pbi.json()["value"])

    # Histórico
    folder_id = "01NZ7UAFRKH7U6OCZ7SNE35YC34Z7SD3YZ"
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{folder_id}/children"
    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_hist = session.request("GET", url, headers=headers, data=payload)
    files_hist = pd.DataFrame(files_hist.json()["value"])

    # 4 - Base - Receita
    folder_id = "01NZ7UAFUYU2BXKKSA4ZC3FQC64AAQZ2MN"
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{folder_id}/children"
    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_op = session.request("GET", url, headers=headers, data=payload)
    files_op = pd.DataFrame(files_op.json()["value"])

    # Operação - Home Broker
    id_hb = files_op[files_op["name"] == "Operação - Home Broker"]["id"].iloc[0]
    # Acessar a pasta do trader
    url_hb = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{id_hb}/children"
    payload_hb = {}
    headers_hb = {"Authorization": f"Bearer {token}"}
    files_hb = session.request("GET", url_hb, headers=headers_hb, data=payload_hb)
    files_hb = pd.DataFrame(files_hb.json()["value"])

    # Inteligência Operacional/4 - Base - Receita/Operação - Home Broker/Gestão - Mesa RV
    id_gestao = files_hb[files_hb["name"] == "Gestão - Mesa RV"]["id"].iloc[0]
    url_gestao = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{id_gestao}/children"
    payload_gestao = {}
    headers_gestao = {"Authorization": f"Bearer {token}"}
    files_gestao = session.request(
        "GET", url_gestao, headers=headers_gestao, data=payload_gestao
    )
    files_gestao = pd.DataFrame(files_gestao.json()["value"])

    # Novos/Planilhas fora dos novos
    folder_id = "01NZ7UAFUK42E526GV7BELXO6BADDVZZQV"
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{folder_id}/children"
    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_novos_outros = session.request("GET", url, headers=headers, data=payload)
    files_novos_outros = pd.DataFrame(files_novos_outros.json()["value"])

    # Base de Sócios
    df_contrata = pd.read_parquet(
        f"s3://{bucket_lake_gold}/api/one/socios/socios.parquet"
    )
    df_contrata["Data início"] = pd.to_datetime(df_contrata["Data início"])
    df_contrata["Data Saída"] = pd.to_datetime(df_contrata["Data Saída"])
    df_contrata_red = df_contrata[["Nome_bd", "Data início", "Data Saída"]]

    # Lista de traders para filtrar as bases
    lista_traders = df_contrata[df_contrata["Área"] == "MESA RV"]["Nome_bd"].unique()

    df_telefone_socios = df_contrata[["Nome_bd", "Celular Pessoal"]]
    df_telefone_socios["Celular Pessoal"] = df_telefone_socios[
        "Celular Pessoal"
    ].replace({"-": "", "\(": "", "\)": ""}, regex=True)

    # Padronizar o número de telefone com 9 dígitos
    df_telefone_socios["Celular Pessoal"] = (
        df_telefone_socios["Celular Pessoal"].astype(str).str[-9:]
    )

    # Ler o arquivo ContasHistorico
    id_pl = files_csv_pbi[files_csv_pbi["name"] == "ContasHistorico.csv"]["id"].iloc[0]
    url_pl = (
        f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{id_pl}/content"
    )
    payload_pl = {}
    headers_pl = {"Authorization": f"Bearer {token}"}
    contas_historico = session.request(
        "GET", url_pl, headers=headers_pl, data=payload_pl
    )

    df_advisors = pd.read_csv(
        io.BytesIO(contas_historico.content), sep=";", decimal=","
    )
    df_advisors["Conta"] = df_advisors["Conta"].astype(str)
    df_advisors["Data"] = pd.to_datetime(df_advisors["Data"])

    # informações
    id_file = files_hist[files_hist["name"] == "informacoes.csv"]["id"].iloc[0]
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{id_file}/content"
    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    informacoes = session.request("GET", url, headers=headers, data=payload)

    df_informacoes_raw = pd.read_csv(
        io.BytesIO(informacoes.content), sep=";", decimal=","
    )
    df_informacoes_raw["account"] = (
        df_informacoes_raw["account"].astype(str).str.split(".").str[0]
    )
    df_informacoes_raw["mobile_phone_number"] = (
        df_informacoes_raw["mobile_phone_number"].astype(str).str.split(".").str[0]
    )

    # Identificar contas que estão abertas
    df_contas_ativas = df_informacoes_raw[
        df_informacoes_raw["account_activation"] != "ENCERRADA"
    ]
    df_contas_ativas["account"] = df_contas_ativas["account"].astype(str)

    # Importar a base equivalente ao base_btg em cloud
    df_contas_raw = pd.read_parquet(f"s3://{bucket_lake_gold}/btg/contas/")
    df_contas_raw["carteira"] = df_contas_raw["carteira"].astype(str)

    # account_in_out, para identificar a data de abertura e encerramento das contas

    df_account_in_out_raw = pd.read_parquet(f"s3://{bucket_lake_gold}/btg/accounts_in_out/")
    df_account_in_out_raw["Conta"] = df_account_in_out_raw["Conta"].astype(str)

    # Identificar contas que encerraram e não estão na base de contas ativas
    # Dupla conferência, pois algumas contas foram encerradas e depois reabertas
    df_account_out = df_account_in_out_raw[
        (df_account_in_out_raw["Abriu/Fechou"] == "Fechou")
        & (~df_account_in_out_raw["Conta"].isin(df_contas_raw["carteira"]))
    ]

    df_account_out.rename({"Data": "Data Encerramento"}, axis=1, inplace=True)

    df_account_out = df_account_out[["Conta", "Data Encerramento"]]

    # Identificar a data de abertura das contas
    df_account_in = df_account_in_out_raw[
        (df_account_in_out_raw["Abriu/Fechou"] == "Abriu")
    ]

    df_account_in = df_account_in[["Conta", "Data"]]

    # Base de corretagem
    file_id = files_novos_outros[files_novos_outros["name"] == "corretagem.xlsx"][
        "id"
    ].iloc[0]
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{file_id}/content"
    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    corretagem = session.request("GET", url, headers=headers, data=payload)

    df_corretagem = pd.read_excel(io.BytesIO(corretagem.content))

    df_corretagem.rename({"CONTA_CLIENTE": "account"}, axis=1, inplace=True)

    df_corretagem["receita_estimada"] = df_corretagem[
        "NOME_GRUPO_CORRETAGEM"
    ].str.extract("HB_([^/]+)_MESA")
    df_corretagem["receita_estimada"] = (
        df_corretagem["receita_estimada"].str.split("_").str[-1]
    )
    df_corretagem["receita_estimada"] = (
        df_corretagem["receita_estimada"].str.split("%").str[0]
    )

    # Demais formatos da coluna de corretagem
    df_corretagem.loc[
        (df_corretagem["NOME_GRUPO_CORRETAGEM"] == "pAdrao")
        | (df_corretagem["NOME_GRUPO_CORRETAGEM"].isna()),
        "receita_estimada",
    ] = 0.5
    df_corretagem.loc[
        (df_corretagem["NOME_GRUPO_CORRETAGEM"].astype(str).str.contains("%MESA")),
        "receita_estimada",
    ] = (
        df_corretagem["NOME_GRUPO_CORRETAGEM"]
        .str.split("_")
        .str[-1]
        .str.split("%MESA")
        .str[0]
        .str.replace(",", ".")
        .str.strip()
    )

    df_corretagem["receita_estimada"] = df_corretagem["receita_estimada"].astype(float)

    df_corretagem["receita_estimada"] = df_corretagem["receita_estimada"] / 100

    df_corretagem_result = df_corretagem[["account", "receita_estimada"]]
    df_corretagem_result.columns = ["Conta", "Corretagem"]

    # # Ler o arquivo receita_rf (Receita Estimada)
    # df_receita_raw = pd.read_parquet(
    #     f"s3a://{bucket_lake_gold}/api/one/receita_estimada/receita_estimada.parquet"
    # )

    df_receita_raw = pd.read_parquet(
        f"s3://{bucket_lake_gold}/one/receita/estimativa/receita_estimada.parquet"
    )

    df_receita_raw["Conta"] = df_receita_raw["Conta"].astype(str)
    df_receita_raw["Produto"] = df_receita_raw["Produto"].astype(str).str.upper()
    df_receita_raw["Produto"] = df_receita_raw["Produto"].replace(
        "OFERTA PRIMÁRIA", "OFERTA PRIMARIA"
    )

    # Manter apenas a receita de RV
    df_receita_rv = df_receita_raw[
        (df_receita_raw["Tipo"] == "VALORES MOBILIARIOS")
        & (df_receita_raw["Categoria"] == "RENDA VARIAVEL")
        & (
            df_receita_raw["Produto"].isin(
                [
                    "CORRETAGEM BOLSA",
                    "BOLSA",
                    "CORRETAGEM",
                    "OPÇAO ESTRUTURADA",
                    "OPÇÃO ESTRUTURADA",
                    "OPCAO ESTRUTURADA",
                    "OFERTA PRIMARIA",
                ]
            )
        )
        & ~(
            (df_receita_raw["Operacional RV"] == "Home Broker")
            & (df_receita_raw["Tipo Ordem"] == "HB")
        )
    ]

    df_receita_rv = df_receita_rv[
        [
            "Conta",
            "Operacional RV",
            "Categoria",
            "Produto",
            "Ativo",
            "Movimentação",
            "Quantidade",
            "Valor Bruto",
            "Valor Reservado",
            "Valor Obtido",
            "Data",
            "Receita",
            "Tipo Receita",
            "Estrutura",
            "Tipo Ordem",
        ]
    ]

    df_receita_rv.rename({"Valor Bruto": "Valor Bruto/Nocional"}, axis=1, inplace=True)

    df_receita_rv.loc[
        (df_receita_rv["Valor Bruto/Nocional"] < 0), "Valor Bruto/Nocional"
    ] = (df_receita_rv["Valor Bruto/Nocional"] * -1)

    # Ler o arquivo PL
    id_pl = files_csv_pbi[files_csv_pbi["name"] == "PL.csv"]["id"].iloc[0]
    url_pl = (
        f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{id_pl}/content"
    )
    payload_pl = {}
    headers_pl = {"Authorization": f"Bearer {token}"}
    pl = session.request("GET", url_pl, headers=headers_pl, data=payload_pl)

    df_pl_raw = pd.read_csv(io.BytesIO(pl.content), sep=";", decimal=",")
    df_pl_raw["Conta"] = df_pl_raw["Conta"].astype(str)

    # Manter o registro do fim do mês da movimentação
    df_pl_rv = df_pl_raw[df_pl_raw["Mercado"].isin(["RENDA VARIÁVEL", "DERIVATIVOS"])]
    df_pl_rv = df_pl_rv[["Conta", "PL", "Data"]]
    df_pl_rv = df_pl_rv.groupby(["Conta", "Data"], as_index=False, dropna=False)[
        "PL"
    ].sum()
    df_pl_rv.rename({"PL": "PL RV"}, axis=1, inplace=True)

    # Ler o arquivo de estruturadas
    file_id = files_hist[files_hist["name"] == "dados_estruturadas.csv"]["id"].iloc[0]
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{file_id}/content"
    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    estruturadas_hist = session.request("GET", url, headers=headers, data=payload)

    df_estruturadas_raw = pd.read_csv(
        io.BytesIO(estruturadas_hist.content), sep=";", decimal=","
    )

    df_estruturadas_raw["Conta"] = df_estruturadas_raw["Conta"].astype(str)

    df_estruturadas = df_estruturadas_raw.drop_duplicates(
        [
            "Conta",
            "Nome do Produto",
            "Ativo",
            "Data de Início",
            "Tipo Operação",
            "Direção",
        ]
    )

    df_estruturadas = df_estruturadas.groupby(
        ["Conta", "Nome do Produto", "Ativo", "Data de Início", "Data de Vencimento"],
        as_index=False,
        dropna=False,
    ).agg({"Quantidade": "max", "Nocional": "max"})

    df_estruturadas = df_estruturadas[
        [
            "Conta",
            "Nome do Produto",
            "Ativo",
            "Data de Início",
            "Data de Vencimento",
            "Quantidade",
            "Nocional",
        ]
    ]
    df_estruturadas.columns = [
        "Conta",
        "Estrutura",
        "Ativo",
        "Data",
        "Data Vencimento",
        "Quantidade",
        "Nocional",
    ]

    # Ler as bases de estruturadas
    operacionais_rv = [
        "Operação - Bernard Faust",
        "Operação - Gabriel Tavares",
        "Operação - Lucca Ramos",
        "Operação - Rafael Schmidt",
        "Operação - Victor Miranda",
    ]
    id_rv = files_op[files_op["name"].isin(operacionais_rv)]["id"].to_list()

    # Concatenar as operações do boletador de todos os traders
    operacoes_boletador = pd.DataFrame()
    for folder_id in id_rv:
        # Acessar a pasta do trader
        url_trader = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{folder_id}/children"
        payload_trader = {}
        headers_trader = {"Authorization": f"Bearer {token}"}
        files_trader = session.request(
            "GET", url_trader, headers=headers_trader, data=payload_trader
        )
        files_trader = pd.DataFrame(files_trader.json()["value"])

        # Acessar a pasta de Outputs
        id_outputs = files_trader[files_trader["name"] == "Outputs"]["id"].iloc[0]
        url_outputs = f"https://graph.microsoft.com/v1.0/users/{user_id}/drives/{drive_id}/items/{id_outputs}/children"
        payload_outputs = {}
        headers_outputs = {"Authorization": f"Bearer {token}"}
        files_outputs = session.request(
            "GET", url_outputs, headers=headers_outputs, data=payload_outputs
        )
        files_outputs = pd.DataFrame(files_outputs.json()["value"])

        # Ler o arquivo operações_boletador
        boletador_id = files_outputs[
            files_outputs["name"] == "operações_boletador.xlsx"
        ]["id"].iloc[0]
        url_boletador = f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{boletador_id}/content"
        payload_boletador = {}
        headers_boletador = {"Authorization": f"Bearer {token}"}
        boletador_hist = session.request(
            "GET", url_boletador, headers=headers_boletador, data=payload_boletador
        )

        df_boletador = pd.read_excel(io.BytesIO(boletador_hist.content))

        # Concatenar a base do trader com as demais
        operacoes_boletador = pd.concat([operacoes_boletador, df_boletador])

    operacoes_boletador["conta"] = operacoes_boletador["conta"].astype(str)
    operacoes_boletador = operacoes_boletador[operacoes_boletador["ativo"].notnull()]

    df_operacoes_boletador = operacoes_boletador[
        ["conta", "ativo", "direcao", "quantidade", "REGISTRO"]
    ]
    df_operacoes_boletador.columns = [
        "Conta",
        "Ativo",
        "Movimentação",
        "Quantidade",
        "REGISTRO",
    ]

    df_operacoes_boletador["Data"] = pd.to_datetime(
        df_operacoes_boletador["REGISTRO"]
    ).dt.date
    df_operacoes_boletador["Hora"] = pd.to_datetime(
        df_operacoes_boletador["REGISTRO"]
    ).dt.time
    df_operacoes_boletador.drop("REGISTRO", axis=1, inplace=True)
    df_operacoes_boletador.rename({"Hora": "Hora Operação"}, axis=1, inplace=True)

    df_operacoes_boletador["Movimentação"] = df_operacoes_boletador[
        "Movimentação"
    ].replace({"C": "COMPRA", "V": "VENDA"})

    # Ler o arquivo Ativos_Campanhas para identificar a Campanha das operações
    # Ler o arquivo operações_boletador
    file_id = files_gestao[files_gestao["name"] == "Ativos_Campanhas.xlsx"]["id"].iloc[
        0
    ]
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{file_id}/content"
    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    ativos = session.request("GET", url, headers=headers, data=payload)

    df_ativos_raw = pd.read_excel(io.BytesIO(ativos.content))

    df_ativos_raw.rename(
        {"Categoria": "Produto", "Direção": "Movimentação"}, axis=1, inplace=True
    )
    df_ativos_raw["Produto"] = df_ativos_raw["Produto"].astype(str).str.upper()

    # Ler o arquivo Base Clientes - Mesa RV para identificas os clientes ativos e suas respectivas informações de contato
    file_id = files_gestao[files_gestao["name"] == "Base Clientes - Mesa RV.xlsx"][
        "id"
    ].iloc[0]
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{file_id}/content"
    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    clientes_rv = session.request("GET", url, headers=headers, data=payload)

    df_clientes_raw = pd.read_excel(
        io.BytesIO(clientes_rv.content), sheet_name="Base Completa"
    )

    df_clientes_raw["Conta"] = df_clientes_raw["Conta"].astype(str)

    df_clientes_ligacao = df_clientes_raw[df_clientes_raw["Cliente Ativo"] == "SIM"]
    df_clientes_ligacao = df_clientes_ligacao[["Conta", "Operacional RV", "Telefone"]]
    df_clientes_ligacao.rename(
        {"Operacional RV": "interno", "Telefone": "Phones"}, axis=1, inplace=True
    )

    df_clientes_ligacao["Phones"] = df_clientes_ligacao["Phones"].astype(str).str[-9:]

    df_clientes_email = df_clientes_raw[df_clientes_raw["Cliente Ativo"] == "SIM"]
    df_clientes_email = df_clientes_email[["Conta", "Operacional RV", "Email"]]
    df_clientes_email.rename(
        {"Operacional RV": "interno", "Email": "E-mail"}, axis=1, inplace=True
    )

    df_clientes_email["E-mail"] = df_clientes_email["E-mail"].astype(str).str.lower()

    # Ler o arquivo Oportunidades
    oportunidades_id = files_gestao[
        files_gestao["name"]
        == "Oportunidades geradas - Inteligência Estratégica RV.xlsx"
    ]["id"].iloc[0]
    url_oportunidades = f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{oportunidades_id}/content"
    payload_oportunidades = {}
    headers_oportunidades = {"Authorization": f"Bearer {token}"}
    oportunidades = session.request(
        "GET",
        url_oportunidades,
        headers=headers_oportunidades,
        data=payload_oportunidades,
    )

    df_oportunidades_raw = pd.read_excel(
        io.BytesIO(oportunidades.content), sheet_name="Oportunidades"
    )
    df_oportunidades_raw["Conta"] = df_oportunidades_raw["Conta"].astype(str)

    df_oportunidades = df_oportunidades_raw[(df_oportunidades_raw["Data"].notnull())]
    df_oportunidades["Produto"] = df_oportunidades["Tipo"].str.upper()
    df_oportunidades = df_oportunidades[
        [
            "Conta",
            "Data",
            "Estratégia",
            "Produto",
            "Ativo",
            "Volume/Notional",
            "Receita Poten.",
        ]
    ]

    df_oportunidades["Ativo"] = df_oportunidades["Ativo"].astype(str)

    # Separar os ativos das linhas com mais de um ativo
    df_oportunidades = (
        df_oportunidades.set_index(
            [
                "Conta",
                "Data",
                "Estratégia",
                "Produto",
                "Volume/Notional",
                "Receita Poten.",
            ]
        )
        .apply(lambda x: x.str.split("/").explode())
        .reset_index()
    )

    # Contatos
    # Ler o arquivo de ligacoes
    file_id = files_hist[files_hist["name"] == "ligacoes_gravadas.csv"]["id"].iloc[0]
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{file_id}/content"
    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    ligacoes = session.request("GET", url, headers=headers, data=payload)

    df_ligacoes_raw = pd.read_csv(io.BytesIO(ligacoes.content), sep=";", decimal=",")

    df_ligacoes_raw = df_ligacoes_raw[
        (df_ligacoes_raw["Ramal"] != "unknown")
        & (df_ligacoes_raw["Ramal"] != "anonymous")
        & (df_ligacoes_raw["Ramal"] != "SIP/9938858")
    ]

    # AdvisorsNoDupCp
    id_file = files_csv_pbi[files_csv_pbi["name"] == "AdvisorsNoDupCp.csv"]["id"].iloc[
        0
    ]
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{id_file}/content"
    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    advisors_no_dup = session.request("GET", url, headers=headers, data=payload)

    df_advisors_no_dup = pd.read_csv(
        io.BytesIO(advisors_no_dup.content), sep=";", decimal=","
    )
    df_advisors_no_dup["Conta"] = df_advisors_no_dup["Conta"].astype(str)

    df_informacoes_raw["phone_number"] = (
        df_informacoes_raw["phone_number"].astype(str).str.split(".").str[0]
    )
    df_informacoes_tel = df_informacoes_raw[
        ["account", "mobile_phone_number", "phone_number"]
    ]

    df_informacoes_tel = (
        df_informacoes_tel.set_index(["account", "mobile_phone_number"])
        .apply(lambda x: x.str.split(", ").explode())
        .reset_index()
    )

    df_informacoes_tel = (
        df_informacoes_tel.set_index(["account", "phone_number"])
        .apply(lambda x: x.str.split(", ").explode())
        .reset_index()
    )

    df_informacoes_tel.loc[
        df_informacoes_tel["mobile_phone_number"] == "0", "mobile_phone_number"
    ] = df_informacoes_tel["phone_number"]
    df_informacoes_tel["Phones"] = df_informacoes_tel["mobile_phone_number"].fillna(
        df_informacoes_tel["phone_number"]
    )

    df_informacoes_tel_grouped = df_informacoes_tel.groupby(
        "Phones", as_index=False, dropna=False
    )["account"].count()

    df_informacoes_tel_grouped.columns = ["Phones", "Contas Associadas"]

    df_informacoes_tel = df_informacoes_tel[["account", "Phones"]].merge(
        df_informacoes_tel_grouped, on="Phones", how="left"
    )

    df_informacoes_tel.rename({"account": "Conta"}, inplace=True, axis=1)

    df_informacoes_tel.drop("Phones", axis=1, inplace=True)

    df_ligacoes = df_ligacoes_raw[
        (~df_ligacoes_raw["Phones"].isin(df_telefone_socios["Celular Pessoal"]))
    ]

    # One Analytics/Automações/Rotina
    item_id = "01NZ7UAFWR3Q3PJKW7YBCYCMCDDZICPQHH"
    url = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drives/{drive_id}/items/{item_id}/children"

    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    files_rotina = session.request("GET", url, headers=headers, data=payload)
    files_rotina = pd.DataFrame(files_rotina.json()["value"])

    id_file = files_rotina[files_rotina["name"] == "ramais.csv"]["id"].iloc[0]
    url = f"https://graph.microsoft.com/v1.0/users/e24ccf9b-82cf-4e41-930c-70606d1a948d/drive/items/{id_file}/content"
    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    ramais = session.request("GET", url, headers=headers, data=payload)

    df_ramais = pd.read_csv(io.BytesIO(ramais.content), sep=",", decimal=".")

    df_ramais["Number"] = df_ramais["Number"].astype(float)
    df_ramais["Number"] = df_ramais["Number"].astype(str)
    df_ligacoes["Ramal"] = df_ligacoes["Ramal"].astype(float)
    df_ligacoes["Ramal"] = df_ligacoes["Ramal"].astype(str)

    df_ligacoes = df_ligacoes.merge(
        df_ramais, how="left", left_on="Ramal", right_on="Number"
    )

    df_ligacoes = df_ligacoes.rename(
        columns={"Name": "nome", "interno": "interno_base"}
    )
    df_ligacoes["interno"] = df_ligacoes["interno_base"].fillna(df_ligacoes["nome"])

    df_ligacoes = df_ligacoes[
        [
            "Uniqueid",
            "Data",
            "Hora",
            "Status",
            "Phones",
            "interno",
            "CustomerId",
            "id_account",
        ]
    ]
    df_ligacoes.rename({"Hora": "Hora Ligação"}, axis=1, inplace=True)

    df_ligacoes = df_ligacoes[(df_ligacoes["Status"] == "Atendida")]

    df_ligacoes["CustomerId"] = df_ligacoes["CustomerId"].str.split(".").str[0]
    df_ligacoes = df_ligacoes[df_ligacoes["CustomerId"].notnull()]
    df_ligacoes["CustomerId"] = df_ligacoes["CustomerId"].astype(int)

    temp = df_advisors_no_dup[["Conta", "UserID"]]
    temp = temp[temp["UserID"].notnull()]
    temp["UserID"] = temp["UserID"].astype(int)

    df_ligacoes_result = df_ligacoes.merge(
        temp, left_on="CustomerId", right_on="UserID", how="left"
    )

    df_ligacoes_result = df_ligacoes_result[
        (df_ligacoes_result["interno"].isin(lista_traders))
    ]

    # Padronizar o formato do número de telefone
    df_ligacoes_result["Phones"] = df_ligacoes_result["Phones"].astype(str).str[-9:]

    # Separar a base em clientes ativos em RV e identificar a conta principal
    df_ligacoes_clientes = df_ligacoes_result[
        (df_ligacoes_result["Phones"].isin(df_clientes_ligacao["Phones"]))
    ]

    df_ligacoes_clientes = df_ligacoes_clientes.drop_duplicates(
        ["Conta", "Data", "Hora Ligação"]
    )

    df_ligacoes_clientes = df_ligacoes_clientes.merge(
        df_clientes_ligacao, on=["Phones", "interno"], how="left"
    )

    df_ligacoes_clientes = df_ligacoes_clientes[
        (df_ligacoes_clientes["Conta_x"] == df_ligacoes_clientes["Conta_y"])
    ]

    df_ligacoes_clientes["Contas Associadas"] = 1

    df_ligacoes_clientes.drop("Conta_y", axis=1, inplace=True)
    df_ligacoes_clientes.rename({"Conta_x": "Conta"}, axis=1, inplace=True)

    # Base dos demais clientes
    df_ligacoes_others = df_ligacoes_result[
        (~df_ligacoes_result["Phones"].isin(df_clientes_ligacao["Phones"]))
        & (df_ligacoes_result["Conta"].notnull())
        & (df_ligacoes_result["interno"].notnull())
    ]

    df_ligacoes_others = df_ligacoes_others.merge(
        df_informacoes_tel, on="Conta", how="left"
    )

    df_ligacoes_result = pd.concat([df_ligacoes_clientes, df_ligacoes_others])

    df_ligacoes_result = df_ligacoes_result[
        ["Data", "Hora Ligação", "interno", "Conta", "Contas Associadas"]
    ]
    df_ligacoes_result.rename({"interno": "Interno"}, axis=1, inplace=True)
    df_ligacoes_result["Ligação"] = "Sim"

    # Ler o arquivo de emails
    file_id = files_hist[files_hist["name"] == "emails_comerciais.csv"]["id"].iloc[0]
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{file_id}/content"
    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    emails = session.request("GET", url, headers=headers, data=payload)

    df_emails_raw = pd.read_csv(io.BytesIO(emails.content), sep=";", decimal=",")

    # Identificar o e-mail dos sócios para diferenciar o sócio do cliente
    df_contrata_email = df_contrata[["Nome_bd", "E-mail"]]
    df_contrata_email["E-mail"] = df_contrata_email["E-mail"].replace(
        "investimentos.one", "oneinv.com"
    )

    df_emails = df_emails_raw.copy()

    df_emails["from"] = df_emails["from"].replace(
        "investimentos.one", "oneinv.com", regex=True
    )
    df_emails["mail_to"] = df_emails["mail_to"].replace(
        "investimentos.one", "oneinv.com", regex=True
    )

    df_emails.loc[
        df_emails["from"].astype(str).str.contains("oneinv.com"), "interno"
    ] = df_emails["from"]
    df_emails.loc[
        ~df_emails["from"].astype(str).str.contains("oneinv.com"), "email"
    ] = df_emails["from"]

    df_emails.loc[
        df_emails["mail_to"].astype(str).str.contains("oneinv.com"), "interno"
    ] = df_emails["mail_to"]
    df_emails.loc[
        ~df_emails["mail_to"].astype(str).str.contains("oneinv.com"), "email"
    ] = df_emails["mail_to"]

    df_emails = df_emails[(df_emails["email"].notnull())]

    df_emails = df_emails[["sentDateTime", "interno", "email"]]

    df_emails.rename(
        {
            "sentDateTime": "Data",
        },
        inplace=True,
        axis=1,
    )

    df_informacoes_email = df_informacoes_raw[["account", "email"]]

    df_informacoes_email["E-mail"] = (
        df_informacoes_email["email"].astype(str).str.lower()
    )

    df_informacoes_email_grouped = df_informacoes_email.groupby(
        "E-mail", as_index=False, dropna=False
    )["account"].count()

    df_informacoes_email_grouped.columns = ["E-mail", "Contas Associadas"]

    df_informacoes_email = df_informacoes_email[["account", "E-mail"]].merge(
        df_informacoes_email_grouped, on="E-mail", how="left"
    )

    df_informacoes_email.rename({"account": "Conta"}, axis=1, inplace=True)

    df_emails["E-mail"] = df_emails["email"].astype(str).str.lower()

    df_emails_result = df_emails.merge(
        df_informacoes_email[["E-mail", "Conta"]], on="E-mail", how="left"
    )

    # df_emails_result = df_emails.copy()

    df_contrata_email["E-mail"] = df_contrata_email["E-mail"].astype(str).str.lower()
    df_emails_result["E-mail"] = df_emails_result["interno"].astype(str).str.lower()

    df_emails_result = df_emails_result.merge(
        df_contrata_email, on="E-mail", how="left"
    )

    df_emails_result = df_emails_result[["Data", "Conta", "email", "Nome_bd"]]
    df_emails_result.columns = ["Data", "Conta", "E-mail", "interno"]

    # Separar a base em clientes ativos em RV e identificar a conta principal
    df_emails_contas = df_emails_result[
        (df_emails_result["E-mail"].isin(df_clientes_email["E-mail"]))
    ]

    df_emails_contas = df_emails_contas.merge(
        df_clientes_email, on=["E-mail", "interno"], how="left"
    )

    df_emails_contas = df_emails_contas[
        (df_emails_contas["Conta_x"] == df_emails_contas["Conta_y"])
    ]

    df_emails_contas["Contas Associadas"] = 1

    df_emails_contas.drop("Conta_y", axis=1, inplace=True)
    df_emails_contas.rename({"Conta_x": "Conta"}, axis=1, inplace=True)

    # Base dos demais clientes
    df_email_others = df_emails_result[
        (~df_emails_result["E-mail"].isin(df_clientes_email["E-mail"]))
        & (df_emails_result["Conta"].notnull())
        & (df_emails_result["interno"].notnull())
    ]

    df_email_others = df_email_others.merge(
        df_informacoes_email, on="Conta", how="left"
    )

    df_emails_result = pd.concat([df_emails_contas, df_email_others])

    df_emails_result = df_emails_result[
        (df_emails_result["interno"].isin(lista_traders))
        & (df_emails_result["Conta"].notnull())
    ]

    df_emails_result = df_emails_result[
        ["Data", "Conta", "interno", "Contas Associadas"]
    ]
    df_emails_result.columns = ["Data", "Conta", "Interno", "Contas Associadas"]

    df_emails_result = df_emails_result.drop_duplicates(
        ["Conta", "Data", "Interno"], keep="last"
    )

    df_emails_result["E-mail"] = "Sim"

    s3_client = boto3.client("s3")
    s3 = boto3.resource("s3")
    my_bucket = s3.Bucket(bucket_whatsapp)
    bucket_list = []
    for file in my_bucket.objects.all():
        file_name = file.key
        if file_name.find(".net") != -1:
            bucket_list.append(file.key)
    bucket_list = [i.split("/")[2] for i in bucket_list]
    length_bucket_list = len(bucket_list)
    pre_df = [i.split(".")[0].split("@") for i in bucket_list]
    interacoes_wpp = pd.DataFrame(pre_df, columns=["Data", "Whatsapp"])
    interacoes_wpp["Whatsapp"] = interacoes_wpp["Whatsapp"].str[-9:]

    df_informacoes_wpp = df_informacoes_raw[["account", "mobile_phone_number"]]
    df_informacoes_wpp.columns = ["Conta", "Whatsapp"]

    df_informacoes_wpp["Conta"] = (
        df_informacoes_wpp["Conta"].astype(str).str.split(".").str[0]
    )
    df_informacoes_wpp["Whatsapp"] = df_informacoes_wpp["Whatsapp"].astype(str).str[-9:]

    df_wpp = interacoes_wpp.merge(df_informacoes_wpp, on="Whatsapp", how="left")

    df_wpp = df_wpp[(df_wpp["Conta"].notnull())]

    df_wpp["Whatsapp"] = "Sim"

    # Ler o arquivo Net New Money Detalhado
    id_nnm = files_csv_pbi[files_csv_pbi["name"] == "Net New Money Detalhado.csv"][
        "id"
    ].iloc[0]
    url_nnm = (
        f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{id_nnm}/content"
    )
    payload_nnm = {}
    headers_nnm = {"Authorization": f"Bearer {token}"}
    nnm = session.request("GET", url_nnm, headers=headers_nnm, data=payload_nnm)

    df_nnm_raw = pd.read_csv(io.BytesIO(nnm.content), sep=";", decimal=",")

    # NNM
    df_nnm = df_nnm_raw[
        (df_nnm_raw["Mercado"] == "CONTA CORRENTE")
        & (df_nnm_raw["Operacional RV"] != "Home Broker")
    ]
    df_nnm = df_nnm.groupby(["Conta", "Data"], as_index=False, dropna=False)[
        "Captação"
    ].sum()
    df_nnm.rename({"Captação": "NNM"}, axis=1, inplace=True)

    # STVM
    # Tudo que é diferente de CONTA CORRENTE é STVM ou Previdência
    df_stvm = df_nnm_raw[df_nnm_raw["Mercado"] == "RENDA VARIÁVEL"]
    df_stvm = df_stvm.groupby(["Conta", "Data"], as_index=False, dropna=False)[
        "Captação"
    ].sum()
    df_stvm.rename({"Captação": "STVM"}, axis=1, inplace=True)

    # Juntas base de receita com as informações do boletador
    df_operacoes_boletador = df_operacoes_boletador.drop_duplicates(
        ["Conta", "Ativo", "Movimentação", "Quantidade", "Data"], keep="last"
    )

    df_receita_rv["Conta"] = df_receita_rv["Conta"].astype(str)
    df_receita_rv["Data"] = pd.to_datetime(df_receita_rv["Data"])
    df_operacoes_boletador["Data"] = pd.to_datetime(df_operacoes_boletador["Data"])

    df_receita_bolsa = df_receita_rv[
        (df_receita_rv["Produto"].isin(["CORRETAGEM BOLSA", "BOLSA", "CORRETAGEM"]))
    ]

    df_receita_bolsa = df_receita_bolsa.merge(
        df_operacoes_boletador, on=["Conta", "Ativo", "Data"], how="left"
    )

    df_corretagem_result["Conta"] = df_corretagem_result["Conta"].astype(str)

    df_receita_bolsa = df_receita_bolsa.merge(
        df_corretagem_result, on="Conta", how="left"
    )

    df_receita_bolsa["Movimentação"] = df_receita_bolsa["Movimentação_x"].fillna(
        df_receita_bolsa["Movimentação_y"]
    )
    df_receita_bolsa["Quantidade"] = df_receita_bolsa["Quantidade_x"].fillna(
        df_receita_bolsa["Quantidade_y"]
    )

    # Remover colunas que não serão mais utilizadas
    df_receita_bolsa = df_receita_bolsa.drop(
        ["Movimentação_x", "Movimentação_y", "Quantidade_x", "Quantidade_y"], axis=1
    )

    # Identificar a estratégia da movimentação
    df_ativos_bolsa = df_ativos_raw[
        df_ativos_raw["Produto"].isin(["CORRETAGEM BOLSA", "BOLSA", "CORRETAGEM"])
    ]

    # Merge para identificar a Estratégia das operações
    df_receita_bolsa = df_receita_bolsa.sort_values("Data")
    df_ativos_bolsa = df_ativos_bolsa.sort_values("Data Entrada")

    df_receita_bolsa = pd.merge_asof(
        df_receita_bolsa,
        df_ativos_bolsa,
        left_on="Data",
        right_on="Data Entrada",
        by="Ativo",
        direction="backward",
    )

    df_receita_bolsa["Movimentação"] = df_receita_bolsa["Movimentação_x"].fillna(
        df_receita_bolsa["Movimentação_y"]
    )
    df_receita_bolsa.drop(["Movimentação_x", "Movimentação_y"], axis=1, inplace=True)

    df_receita_bolsa = df_receita_bolsa.drop_duplicates(
        [
            "Conta",
            "Ativo",
            "Valor Bruto/Nocional",
            "Data",
            "Receita",
            "Tipo Receita",
            "Estrutura",
            "Tipo Ordem",
            "Hora Operação",
            "Corretagem_x",
            "Movimentação",
            "Quantidade",
        ],
        keep="last",
    )

    print(df_receita_bolsa.head())
    print(len(df_receita_bolsa))

    df_receita_bolsa.loc[
        (df_receita_bolsa["Data"] < df_receita_bolsa["Data Entrada"])
        | (df_receita_bolsa["Data"] > df_receita_bolsa["Data Fim"]),
        "Estratégia",
    ] = np.nan

    df_receita_bolsa["Produto"] = df_receita_bolsa["Produto_x"].fillna(
        df_receita_bolsa["Produto_y"]
    )

    # Identificar se foi uma oportunidade interna (gerada pelo Rodrigo)
    df_receita_bolsa["Data"] = pd.to_datetime(df_receita_bolsa["Data"])
    df_oportunidades["Data"] = pd.to_datetime(df_oportunidades["Data"])

    df_receita_bolsa = df_receita_bolsa.sort_values("Data")
    df_oportunidades = df_oportunidades.sort_values("Data")

    df_receita_bolsa = pd.merge_asof(
        df_receita_bolsa,
        df_oportunidades,
        on="Data",
        by=["Conta", "Ativo", "Produto"],
        direction="backward",
        tolerance=pd.Timedelta("7d"),
    )

    df_receita_bolsa.loc[
        df_receita_bolsa["Estratégia_y"].notnull(), "Oportunidade Interna"
    ] = "Sim"

    df_receita_bolsa["Estratégia"] = df_receita_bolsa["Estratégia_x"].fillna(
        df_receita_bolsa["Estratégia_y"]
    )
    df_receita_bolsa["Estratégia"] = df_receita_bolsa["Estratégia"].fillna("Outros")

    # A Estratégia (Campanha) de operações HB deve ser "Outros"
    df_receita_bolsa.loc[(df_receita_bolsa["Tipo Ordem"] == "HB"), "Estratégia"] = (
        "Outros"
    )

    df_receita_bolsa = df_receita_bolsa[
        [
            "Conta",
            "Operacional RV",
            "Categoria",
            "Produto",
            "Ativo",
            "Movimentação",
            "Quantidade",
            "Valor Bruto/Nocional",
            "Data",
            "Receita",
            "Tipo Receita",
            "Hora Operação",
            "Estratégia",
            "Tipo Ordem",
            "Oportunidade Interna",
            "Volume/Notional",
            "Receita Poten.",
        ]
    ]

    df_receita_estruturadas = df_receita_rv[
        (
            df_receita_rv["Produto"].isin(
                ["OPÇÃO ESTRUTURADA", "OPÇAO ESTRUTURADA", "OPCAO ESTRUTURADA"]
            )
        )
    ]

    df_receita_estruturadas["Estrutura_aux"] = (
        df_receita_estruturadas["Ativo"].astype(str).str.split(" - ").str[-1]
    )
    df_receita_estruturadas["Ativo"] = (
        df_receita_estruturadas["Ativo"].astype(str).str.split(" - ").str[0]
    )
    df_receita_estruturadas["Estrutura"] = df_receita_estruturadas["Estrutura"].fillna(
        df_receita_estruturadas["Estrutura_aux"]
    )
    df_receita_estruturadas["Data"] = pd.to_datetime(df_receita_estruturadas["Data"])

    df_receita_estruturadas = df_receita_estruturadas.drop("Estrutura_aux", axis=1)

    df_estruturadas["Data"] = pd.to_datetime(df_estruturadas["Data"])

    df_receita_estruturadas = df_receita_estruturadas.merge(
        df_estruturadas, on=["Conta", "Ativo", "Data"], how="left"
    )

    df_receita_estruturadas = df_receita_estruturadas[
        [
            "Conta",
            "Operacional RV",
            "Categoria",
            "Produto",
            "Ativo",
            "Movimentação",
            "Quantidade_y",
            "Nocional",
            "Data",
            "Receita",
            "Tipo Receita",
            "Estrutura_y",
            "Tipo Ordem",
        ]
    ]

    df_receita_estruturadas.columns = [
        "Conta",
        "Operacional RV",
        "Categoria",
        "Produto",
        "Ativo",
        "Movimentação",
        "Quantidade",
        "Valor Bruto/Nocional",
        "Data",
        "Receita",
        "Tipo Receita",
        "Estrutura",
        "Tipo Ordem",
    ]

    # Indentificar a campanha das estruturadas
    df_ativos_estruturadas = df_ativos_raw[
        df_ativos_raw["Produto"].isin(
            ["OPÇÃO ESTRUTURADA", "OPÇAO ESTRUTURADA", "OPCAO ESTRUTURADA"]
        )
    ]

    df_ativos_estruturadas["Ativo"] = df_ativos_estruturadas["Ativo"].astype(str)
    df_ativos_estruturadas["Estrutura (De)"] = df_ativos_estruturadas[
        "Estrutura (De)"
    ].astype(str)
    df_receita_estruturadas["Estrutura"] = df_receita_estruturadas["Estrutura"].astype(
        str
    )

    for campanha in df_ativos_estruturadas.index:
        if df_ativos_estruturadas["Ativo"][campanha].upper() != "TODOS":
            df_receita_estruturadas.loc[
                (
                    df_receita_estruturadas["Estrutura"].str.upper()
                    == df_ativos_estruturadas["Estrutura (De)"][campanha].upper()
                )
                & (
                    df_receita_estruturadas["Ativo"]
                    == df_ativos_estruturadas["Ativo"][campanha]
                )
                & (
                    df_receita_estruturadas["Data"]
                    >= df_ativos_estruturadas["Data Entrada"][campanha]
                )
                & (
                    df_receita_estruturadas["Data"]
                    <= df_ativos_estruturadas["Data Fim"][campanha]
                ),
                "Estratégia",
            ] = df_ativos_estruturadas["Estratégia"][campanha]

        else:
            df_receita_estruturadas.loc[
                (
                    df_receita_estruturadas["Estrutura"].str.upper()
                    == df_ativos_estruturadas["Estrutura (De)"][campanha].upper()
                )
                & (
                    df_receita_estruturadas["Data"]
                    >= df_ativos_estruturadas["Data Entrada"][campanha]
                )
                & (
                    df_receita_estruturadas["Data"]
                    <= df_ativos_estruturadas["Data Fim"][campanha]
                ),
                "Estratégia",
            ] = df_ativos_estruturadas["Estratégia"][campanha]

    df_receita_estruturadas.loc[
        df_receita_estruturadas["Ativo"] == "PTAX", "Estratégia"
    ] = "Dólar"
    df_receita_estruturadas["Estratégia"] = df_receita_estruturadas[
        "Estratégia"
    ].fillna("Outros")

    # Unir resultado com Ofertas Primárias
    # Identificar se foi uma oportunidade interna (gerada pelo Rodrigo)
    df_receita_ipo = df_receita_rv[(df_receita_rv["Produto"] == "OFERTA PRIMARIA")]

    df_receita_ipo["Data"] = pd.to_datetime(df_receita_ipo["Data"])
    df_oportunidades["Data"] = pd.to_datetime(df_oportunidades["Data"])

    df_receita_ipo = df_receita_ipo.sort_values("Data")
    df_oportunidades = df_oportunidades.sort_values("Data")

    df_receita_ipo = pd.merge_asof(
        df_receita_ipo,
        df_oportunidades,
        on="Data",
        by=["Conta", "Ativo", "Produto"],
        direction="nearest",
        tolerance=pd.Timedelta("30d"),
    )

    df_receita_ipo.loc[
        df_receita_ipo["Estratégia"].notnull(), "Oportunidade Interna"
    ] = "Sim"
    df_receita_ipo["Estratégia"] = df_receita_ipo["Estratégia"].fillna("Emissão")

    df_receita_derivativo = df_receita_raw[
        (df_receita_raw["Tipo"] == "VALORES MOBILIARIOS")
        & (df_receita_raw["Categoria"] == "RENDA VARIAVEL")
        & (df_receita_raw["Produto"].isin(["DERIVATIVO"]))
        & ~(
            (df_receita_raw["Operacional RV"] == "Home Broker")
            & (df_receita_raw["Tipo Ordem"] == "HB")
        )
    ]

    df_receita_derivativo = df_receita_derivativo[
        [
            "Conta",
            "Operacional RV",
            "Categoria",
            "Produto",
            "Ativo",
            "Movimentação",
            "Quantidade",
            "Valor Bruto",
            "Valor Reservado",
            "Valor Obtido",
            "Data",
            "Receita",
            "Tipo Receita",
            "Estrutura",
            "Tipo Ordem",
        ]
    ]

    df_receita_derivativo.rename(
        {"Valor Bruto": "Valor Bruto/Nocional"}, axis=1, inplace=True
    )

    df_result_all = pd.concat(
        [
            df_receita_bolsa,
            df_receita_estruturadas,
            df_receita_ipo,
            df_receita_derivativo,
            df_ligacoes_result,
            df_emails_result,
            df_wpp,
            df_nnm,
            df_stvm,
        ]
    )

    df_result_all["Data"] = pd.to_datetime(df_result_all["Data"])

    df_account_in["Data"] = pd.to_datetime(df_account_in["Data"])

    df_result_all = df_result_all.merge(df_account_out, on=["Conta"], how="left")

    df_result_all = df_result_all[
        (df_result_all["Data Encerramento"].isna())
        | (
            pd.to_datetime(df_result_all["Data"])
            < pd.to_datetime(df_result_all["Data Encerramento"])
        )
    ]

    df_result_all = df_result_all.merge(
        df_account_in, on=["Conta", "Data"], how="outer", indicator=True
    )

    df_result_all.loc[
        df_result_all["_merge"].isin(["both", "right_only"]), "Primeiro Registro"
    ] = "Sim"

    df_pl_declarado = df_contas_raw[["carteira", "pl_declarado"]]
    df_pl_declarado.columns = ["Conta", "PL Declarado"]
    df_pl_declarado["Conta"] = df_pl_declarado["Conta"].astype(str)

    df_result_all["Conta"] = df_result_all["Conta"].astype(str)

    df_result_all = df_result_all.merge(df_pl_declarado, on="Conta", how="left")

    # Merge asof para identificar comercial e operacionais
    df_result_all = df_result_all.drop("Operacional RV", axis=1)
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

    # Adicionar PL RV na base
    df_result_all["Data"] = pd.to_datetime(df_result_all["Data"])
    df_result_all = df_result_all.sort_values("Data")

    df_pl_rv["Data"] = pd.to_datetime(df_pl_rv["Data"])
    df_pl_rv = df_pl_rv.sort_values("Data")

    df_result_all = pd.merge_asof(
        df_result_all, df_pl_rv, on="Data", by="Conta", direction="forward"
    )

    df_result = df_result_all[
        [
            "Data",
            "Conta",
            "Nome",
            "Operacional RV",
            "Primeiro Registro",
            "PL Declarado",
            "PL RV",
            "Categoria",
            "Produto",
            "Estratégia",
            "Estrutura",
            "Ativo",
            "Movimentação",
            "Quantidade",
            "Valor Bruto/Nocional",
            "Valor Reservado",
            "Valor Obtido",
            "Hora Operação",
            "Tipo Ordem",
            "Receita",
            "Tipo Receita",
            "Oportunidade Interna",
            "Volume/Notional",
            "Receita Poten.",
            "Ligação",
            "Hora Ligação",
            "E-mail",
            "Contas Associadas",
            "Interno",
            "Whatsapp",
            "NNM",
            "STVM",
        ]
    ]

    df_result.rename(
        {
            "Estratégia": "Campanha",
            "Volume/Notional": "Valor Oportunidade",
            "Receita Poten.": "Receita Oportunidade",
        },
        axis=1,
        inplace=True,
    )

    df_result["Nome"] = df_result["Nome"].astype(str).str.split().str[0]

    df_result["Categoria_aux"] = df_result["Produto"].replace(
        {
            "SWAP": "DERIVATIVO",
            "OPÇÃO ESTRUTURADA": "DERIVATIVO",
            "OPÇAO ESTRUTURADA": "DERIVATIVO",
            "OPCAO ESTRUTURADA": "DERIVATIVO",
        }
    )

    df_result = df_result[
        ~(
            (df_result["Operacional RV"] == "Home Broker")
            & (df_result["Tipo Ordem"] == "HB")
            & (df_result["Interno"].isna())
            & (df_result["Primeiro Registro"] != "Sim")
        )
    ]

    # Pegar o dia 1° do mês atual no ano passado
    now = datetime.now()
    date_12m = datetime(now.year - 1, now.month, 1)
    date_12m = datetime.strftime(date_12m, format="%Y-%m-%d")

    df_result = df_result[(df_result["Data"] >= date_12m)]

    # Adicionar Objetivos na base
    # Ler o arquivo de Objetivos (atualizado pelo Rodrigo)
    file_id = files_gestao[files_gestao["name"] == "Objetivos RV.xlsx"]["id"].iloc[0]
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/drive/items/{file_id}/content"
    payload = {}
    headers = {"Authorization": f"Bearer {token}"}
    objetivos = session.request("GET", url, headers=headers, data=payload)

    df_objetivos_raw = pd.read_excel(io.BytesIO(objetivos.content))

    df_objetivos = df_objetivos_raw[
        ["Data", "Operacional RV", "Categoria_aux", "Produto", "Obj. Receita"]
    ]

    df_objetivos.rename(
        {"Obj. Receita": "Objetivo", "Categoria_aux": "Categoria_aux"},
        axis=1,
        inplace=True,
    )

    df_result_objetivos = pd.concat([df_result, df_objetivos])

    df_result_objetivos["Hora Ligação"] = df_result_objetivos["Hora Ligação"].astype(
        str
    )
    df_result_objetivos["Hora Operação"] = df_result_objetivos["Hora Operação"].astype(
        str
    )

    df_result_objetivos["Quantidade"] = (
        df_result_objetivos["Quantidade"].astype(str).str.strip()
    )
    df_result_objetivos["Quantidade"] = df_result_objetivos["Quantidade"].replace(
        "", np.nan
    )

    df_result_objetivos["Quantidade"] = df_result_objetivos["Quantidade"].astype(float)

    # df_result_objetivos.to_parquet(
    #     f"s3a://{bucket_lake_gold}/api/one/receita_estimada/banco_de_dados_rv.parquet",
    #     index=False,
    # )

    # Credenciais da Graph API da Microsoft
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    # Fazer upload para o OneDrive
    with io.BytesIO() as buffer:
        df_result_objetivos.to_csv(
            buffer, sep=";", index=False, decimal=",", encoding="latin1"
        )
        bytes_file = buffer.getvalue()

    path = [
        "One Analytics",
        "Inteligência Operacional",
        "4 - Base - Receita",
        "Operação - Home Broker",
        "Gestão - Mesa RV",
        "banco_de_dados_rv.csv",
    ]
    # Fazer upload do arquivo
    response = drive.upload_file(path=path, bytes=bytes_file)

    print(f"Response:", response)