import pandas as pd
import os
from datetime import datetime
import pandas_market_calendars as mcal
import json
import boto3
import awswrangler as wr

from utils.OneGraph.drive import OneGraphDrive
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name

from utils.DagReceitaEstimada.Validacao.funcoes_auxiliares import (
    last_modified_date_onedrive,
    ultimo_dia_util,
    diferenca_entre_dias,
    calculo_diferenca,
    table_style,
)
from utils.DagReceitaEstimada.funcoes_auxiliares import (
    send_email_outlook,
    real_br_money_mask,
)


def validacao(drive_id):
    # Credenciais da Graph API da Microsoft
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    # Identificar última atualização do Consolidado-Historico
    query_final = f"""
        SELECT MAX(data_processamento) AS max_date
        FROM tb_consolidado_historico_gold
    """

    # Realiza o carregamento da base
    bucket_output = "aws-athena-query-results-us-east-1-600664643760"
    session = boto3.session.Session(region_name="us-east-1")
    data_ultima_atualizacao = wr.athena.read_sql_query(
        query_final,
        database="btg",
        s3_output=f"s3://{bucket_output}",
        boto3_session=session,
    )

    data_ultima_atualizacao = data_ultima_atualizacao.iloc[0, 0]
    data_ultima_atualizacao = pd.to_datetime(data_ultima_atualizacao)

    # Definir variáveis de data
    now = datetime.now()
    date = now.date().strftime("%Y-%m-%d")

    last_week = now - pd.DateOffset(days=7)
    last_week = last_week.date().strftime("%Y-%m-%d")

    bmf_raw = mcal.get_calendar("BMF")
    bmf_raw = bmf_raw.schedule(
        start_date=last_week,
        end_date=date,
    )

    last_day = bmf_raw[["market_open"]].iloc[-2][0]
    last_day = last_day.strftime("%Y-%m-%d")

    bucket_lake_silver = get_bucket_name("lake-silver")

    # Ler receita_estimada
    df_hoje = pd.read_parquet(
        f"s3://{bucket_lake_silver}/one/receita/estimativa/receita_estimada/date={date}"
    )

    df_ontem = pd.read_parquet(
        f"s3://{bucket_lake_silver}/one/receita/estimativa/receita_estimada/date={last_day}"
    )

    # Definir inicio do último mês
    today = datetime.now().date()
    last_month = today - pd.DateOffset(months=1)
    last_month = datetime(last_month.year, last_month.month, 1)

    df_hoje["Data"] = pd.to_datetime(df_hoje["Data"])
    df_ontem["Data"] = pd.to_datetime(df_ontem["Data"])

    df_diferencas = calculo_diferenca(
        df_hoje=df_hoje[df_hoje["Data"] >= last_month],
        df_ontem=df_ontem[df_ontem["Data"] >= last_month],
        group_cols=["Tipo", "Categoria"],
    )

    menor_diferenca = df_diferencas["Dif"].min()

    # Conferir se a receita diminuiu
    # Estabeleci o valor de 1000, pois podem ocorrer pequenas reduções na receita de um dia
    # para o outro decorrentes de ajustes esperados, nesses casos não é necessário validar.
    # Já para valores maiores é fundamental entender o motivo.
    if menor_diferenca < -1000:
        # Caso tenha chego relatório de receita desde a última atualização da receita estimada,
        # indica que a ferramenta estava superestimando a receita.
        # Por isso, é necessário conferir se chegou relatório de receita em D-1.
        # Se não for o caso (if) quer dizer que a estimativa diminuiu.
        # Nesse caso é essencial validar o motivo da redução.
        if data_ultima_atualizacao < ultimo_dia_util():
            print("Receita Estimada Diminuiu!!!")
            return False
        else:
            aviso = '<br><strong style="color:#FF0000;">IMPORTANTE!</strong> Receita de ontem estava maior do que a de hoje. Analisar tabelas para entender.<br>'
            print("Receita estimada estava acima do realizado")
    else:
        aviso = ""

    # Identificar diferenças na receita
    df_diferenca_entre_dias = diferenca_entre_dias(
        df_hoje=df_hoje,
        df_ontem=df_ontem,
        lista_cols=["Tipo Receita", "Tipo", "Categoria", "Produto"],
        limite_dif=0.12,
    )

    # Tratar colunas de valor monetário
    money_cols = ["Receita_hoje", "Receita_ontem", "Dif"]
    for col in money_cols:
        df_diferenca_entre_dias[col] = df_diferenca_entre_dias[col].apply(
            lambda x: "R$ " + real_br_money_mask(x) if pd.notnull(x) else "-"
        )

    # Tratar colunas de percentual
    df_diferenca_entre_dias["% Dif"] = df_diferenca_entre_dias["% Dif"].apply(
        lambda x: real_br_money_mask(x * 100) + "%" if pd.notnull(x) else "-"
    )

    # Preencher campos vazios
    df_diferenca_entre_dias = df_diferenca_entre_dias.fillna("-")

    # Criar colunas que não existem no dataframe
    null_columns = ["Tipo", "Categoria", "Produto"]
    for col in null_columns:
        if col not in df_diferenca_entre_dias.columns.to_list():
            df_diferenca_entre_dias[col] = "-"

    # Ordenar as linhas de forma que apareça primeiro os valores macro
    # A ordem está "ao contrário" para que as primeiras linhas sejam menos detalhadas do que as subsequentes
    df_diferenca_entre_dias = df_diferenca_entre_dias.sort_values(
        ["Tipo Receita", "Produto", "Categoria", "Tipo"],
        ascending=[False, True, True, True],
    )

    # Reordenar as colunas
    df_diferenca_entre_dias = df_diferenca_entre_dias[
        [
            "Tipo Receita",
            "Tipo",
            "Categoria",
            "Produto",
            "Receita_hoje",
            "Receita_ontem",
            "Dif",
            "% Dif",
        ]
    ]

    # Renomear as colunas
    df_diferenca_entre_dias.columns = [
        "Tipo Receita",
        "Tipo",
        "Categoria",
        "Produto",
        "Receita até Hoje",
        "Receita até D-1",
        "Diferença (R$)",
        "Diferença (%)",
    ]

    # Remover colunas em que todos os valores são "-", ou seja, vazias
    df_diferenca_entre_dias = df_diferenca_entre_dias.loc[
        :, df_diferenca_entre_dias.ne("-").any()
    ]

    # Filtras Diferenças Expressivas
    DIFERENCAS_EXPRESSIVAS = 0.2
    df_diferencas_expressivas = df_diferencas[
        (df_diferencas["% Dif"] > DIFERENCAS_EXPRESSIVAS) | (df_diferencas["% Dif"] < 0)
    ]

    # Tratar colunas de valor monetário
    money_cols = ["Receita_hoje", "Receita_ontem", "Dif"]
    for col in money_cols:
        df_diferencas_expressivas[col] = df_diferencas_expressivas[col].apply(
            lambda x: "R$ " + real_br_money_mask(x) if pd.notnull(x) else "-"
        )

    # Tratar colunas de percentual
    df_diferencas_expressivas["% Dif"] = df_diferencas_expressivas["% Dif"].apply(
        lambda x: real_br_money_mask(x * 100) + "%" if pd.notnull(x) else "-"
    )

    # Reordenar as colunas
    df_diferencas_expressivas = df_diferencas_expressivas[
        [
            "Tipo",
            "Categoria",
            "Receita_hoje",
            "Receita_ontem",
            "Dif",
            "% Dif",
        ]
    ]

    # Renomear as colunas
    df_diferencas_expressivas.columns = [
        "Tipo",
        "Categoria",
        "Receita até Hoje",
        "Receita até D-1",
        "Diferença (R$)",
        "Diferença (%)",
    ]

    # Identificar data máxima de cada tipo
    df_hoje["Data"] = pd.to_datetime(df_hoje["Data"])

    df_grouped = df_hoje.groupby(["Tipo Receita", "Tipo"], as_index=False).agg(
        {"Data": "max", "Receita": "sum"}
    )

    df_grouped = df_grouped.pivot_table(
        values=["Receita", "Data"],
        index=["Tipo"],
        columns=["Tipo Receita"],
        aggfunc={"Receita": "sum", "Data": "max"},
    )

    df_grouped = df_grouped.reset_index(drop=False)
    df_grouped = df_grouped.sort_values(by=("Data", "Receita Realizada"))

    # Tratar colunas de data
    date_cols = [("Data", "Receita Realizada"), ("Data", "Receita Estimada")]
    for col in date_cols:
        df_grouped[col] = pd.to_datetime(df_grouped[col])
        df_grouped[col] = df_grouped[col].dt.strftime("%d/%m/%Y")
        df_grouped[col] = df_grouped[col].fillna("-")

    # Transformar colunas de valor monetários
    money_cols = [("Receita", "Receita Realizada"), ("Receita", "Receita Estimada")]
    for col in money_cols:
        df_grouped[col] = df_grouped[col].apply(
            lambda x: "R$ " + real_br_money_mask(x) if pd.notnull(x) else "-"
        )

    # Salvar bases
    # s3
    bucket_lake_gold = get_bucket_name("lake-gold")

    df_hoje.to_parquet(
        f"s3://{bucket_lake_gold}/one/receita/estimativa/receita_estimada.parquet"
    )

    # OneDrive
    bytes_file = df_hoje.to_csv(sep=";", decimal=",", index=False)

    path = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "csv_pbi",
        "receita_estimada.csv",
    ]

    response = drive.upload_file(path=path, bytes=bytes_file)

    print("Upload OneDrive: ", response)

    # Enviar e-mail com o consolidado
    # Definir path do template
    path_html = os.path.join(
        os.environ.get("BASE_PATH_DAGS"),
        "utils",
        "DagReceitaEstimada",
        "templates",
    )

    # Ler o template
    template = open(
        os.path.join(path_html, "validacao.html"),
        encoding="utf-8",
    ).read()

    # Substituir valores no template
    template_values = {
        "{DATA_HOJE}": datetime.now().date().strftime("%d/%m/%Y"),
        "{AVISO}": aviso,
        "{DATA_MAXIMA_POR_TIPO}": table_style(df_grouped),
        "{DIFERENCA_ENTRE_DIAS}": table_style(df_diferenca_entre_dias),
        "{DIFERENCAS_EXPRESSIVAS_VAR}": real_br_money_mask(DIFERENCAS_EXPRESSIVAS * 100)
        + "%",
        "{DIFERENCAS_EXPRESSIVAS}": table_style(df_diferencas_expressivas),
    }

    for old, new in template_values.items():
        template = template.replace(old, new)

    # Definir parâmetros do envio
    kwargs = {
        "drive_id": drive_id,
        "item_id": "d64bcd7f-c47a-4343-b0b4-73f4a8e927a5",  # tech@investimentos.one
        "destinatarios": [
            "nathalia.montandon@investimentos.one",
            "samuel.machado@investimentos.one",
        ],
        "titulo": "[Receita Estimada] Consolidado",
        "previa": "Informações da Receita Estimada",
    }

    # Enviar o e-mail
    send_email_outlook(template=template, **kwargs)
