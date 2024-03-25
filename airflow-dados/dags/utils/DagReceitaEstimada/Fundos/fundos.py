import pandas as pd
import json
from datetime import datetime

from utils.OneGraph.drive import OneGraphDrive
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name

from utils.DagReceitaEstimada.funcoes_auxiliares import (
    posicao,
    extrair_data_maxima_da_receita,
)
from utils.DagReceitaEstimada.Fundos.funcoes_auxiliares import (
    tratamento_fund_guide,
    fundos_sem_margem,
)


def fundos(drive_id):
    IMPOSTO = 0.9335

    # Credenciais da Graph API da Microsoft
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    # Considero a data máxima de Renda Fixa,
    # porque na prévia os fundos aparecem com o último dia do mês em questão
    # e não até a data que realmente é.
    # Além disso, fundos e renda fixa estão no mesmo relatório de receita
    max_date = extrair_data_maxima_da_receita(
        tipo="VALORES MOBILIARIOS", categoria="RENDA FIXA", produto="SECUNDARIO"
    )

    # Ler o Fund Guide
    # Definir o caminho do arquivo
    path_fund_guide = [
        "One Analytics",
        "Análises",
        "Receita",
        "Infos",
        "Fund Guide.xlsm",
    ]

    # Ler o conteúdo do arquivo
    kwargs = {
        "sheet_name": "Fundos Completo (B2C+Assessor)",
        "skiprows": 2,
        "usecols": ["Fundo", "CNPJ", "Margem Adm"],
    }

    df_fund_guide = drive.download_file(path=path_fund_guide, to_pandas=True, **kwargs)

    df_fund_guide = tratamento_fund_guide(df_fund_guide)

    # Ler a base Outros Fundos para identificar ROA dos fundos que não estão no fund_guide
    # Definir o caminho do arquivo
    path_outros_fundos = [
        "One Analytics",
        "Renda Fixa",
        "13 - Planilhas de Atualização",
        "Outros Fundos.xlsx",
    ]

    kwargs = {"sheet_name": "Fundos"}

    # Ler o conteúdo do arquivo
    df_outros_fundos = drive.download_file(
        path=path_outros_fundos, to_pandas=True, **kwargs
    )

    df_outros_fundos.rename({"Margem Adm": "receita_estimada"}, axis=1, inplace=True)

    df_outros_fundos["CNPJ"] = (
        df_outros_fundos["CNPJ"]
        .astype(str)
        .str.replace(".", "")
        .str.replace("-", "")
        .str.replace("/", "")
    )

    # Concatenar as bases para identificar a margem dos fundos
    df_roa_fundos = pd.concat([df_fund_guide, df_outros_fundos])

    # Remover duplicadas, priorizando o input da planilha
    df_roa_fundos = df_roa_fundos.drop_duplicates("CNPJ", keep="last")

    # Separar a base de posição a partir da data da última receita de fundos
    df_posicao = posicao(max_date)

    df_posicao["data_interface"] = pd.to_datetime(df_posicao["data_interface"])

    # Filtrar o dataframe de posição
    df_posicao_fundos = df_posicao[
        (df_posicao["market"] == "FUNDOS")
        & (df_posicao["data_interface"].dt.date > max_date.date())
    ]

    # Padronizar o CNPJ
    df_posicao_fundos["cnpj"] = (
        df_posicao_fundos["cnpj"]
        .astype(str)
        .str.replace(".", "")
        .str.replace("-", "")
        .str.replace("/", "")
    )

    # Identificar a margem de adm dos fundos
    df_fundos = df_posicao_fundos.merge(
        df_roa_fundos, left_on="cnpj", right_on="CNPJ", how="left", indicator=True
    )

    # Definir as variáveis para envio do e-mail de fundos sem margem
    kwargs = {
        "drive_id": drive_id,
        "item_id": "d64bcd7f-c47a-4343-b0b4-73f4a8e927a5",  # tech@investimentos.one
        "destinatarios": [
            "back.office@investimentos.one",
            "nathalia.montandon@investimentos.one",
            "samuel.machado@investimentos.one",
        ],
        "titulo": "[Receita Estimada] Fundos sem Margem de Administração",
        "previa": "Preencher Margem de Administração dos Fundos",
    }

    # Enviar o e-mail, se for o caso
    fundos_sem_margem(df_fundos, **kwargs)

    # Calcular receita estimada de fundos baseado na posição dia a dia
    # Margem de Adm no Fund Guide é anual, mas aqui precisamos utilizar a diária
    df_fundos["repasse"] = ((1 + df_fundos["receita_estimada"]) ** (1 / 252)) - 1
    df_fundos["repasse"] = df_fundos["repasse"] * IMPOSTO

    # Calcular a receita
    df_fundos["Receita"] = df_fundos["gross_value"] * df_fundos["repasse"]

    # Preencher colunas
    df_fundos.loc[df_fundos["Receita"].notnull(), "Tipo Receita"] = "Receita Estimada"
    df_fundos["Tipo"] = "VALORES MOBILIARIOS"
    df_fundos["Categoria"] = "FUNDOS"
    df_fundos["Produto"] = "ADMINISTRAÇAO"

    df_fundos = df_fundos.drop("CNPJ", axis=1)

    # Renomear colunas
    cols_names = {
        "account": "Conta",
        "product": "Ativo",
        "cnpj": "CNPJ",
        "amount": "Quantidade",
        "gross_value": "Valor Bruto",
        "data_interface": "Data Movimentação",
        "receita_estimada": "ROA",
        "repasse": "Repasse",
        "Receita": "Receita Estimada",
    }

    df_fundos.rename(cols_names, axis=1, inplace=True)

    # Reordenar colunas
    df_fundos = df_fundos[
        [
            "Conta",
            "Tipo",
            "Categoria",
            "Produto",
            "Ativo",
            "CNPJ",
            "Data Movimentação",
            "Quantidade",
            "Valor Bruto",
            "ROA",
            "Repasse",
            "Receita Estimada",
            "Tipo Receita",
        ]
    ]

    # Definir variáveis de data
    now = datetime.now()
    date = now.date().strftime("%Y-%m-%d")
    date_aux = now.date().strftime("%Y%m%d")

    bucket_lake_silver = get_bucket_name("lake-silver")

    df_fundos.to_parquet(
        f"s3://{bucket_lake_silver}/one/receita/estimativa/fundos/date={date}/fundos_{date_aux}.parquet"
    )
