import pandas as pd
import json
from datetime import datetime
import pandas_market_calendars as mcal
import boto3
import io
import os
from deltalake import write_deltalake, DeltaTable

from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name
from utils.OneGraph.drive import OneGraphDrive
from utils.migracoes_de_base import padroniza_posicao


def email_list(destinatarios: str) -> list:
    """Função para gerar lista de destinatários no formato necessário para fazer o post na Graph API da Microsoft.
    Args:
        destinatarios (str): string contendo os destinatários. Os e-mails devem estar separados por ";"
    Returns:
        list: lista contendo os destinatários que receberão o e-mail
    """
    to = []

    # Iterar sobre o total de e-mails para criar um dicionário para cada destinatário
    # e, posteriormente, adiciona-lo na lista
    for email in destinatarios:
        result = {"emailAddress": {"address": email}}
        to.append(result)

    return to


def send_email_outlook(
    drive_id: str,
    item_id: str,
    destinatarios: list,
    titulo: str,
    template: str,
    previa: str,
    destinatarios_cc: list = [],
):
    """Função para envio de e-mails via Graph API
    Args:
        drive_id (str): ID do drive do Onedrive onde reside o arquivo
        item_id (str): ID de objeto do remetente do e-mail
        destinatarios (list): stakeholders do projeto que devem ser informados sobre atualizações do projeto
        destinatarios_cc (list): stakeholders "secundarios" do projeto que devem acompanhar as atualizações
        titulo (str): título do e-mail
        template (str): template HTML do e-mail
    """
    # Aplicar a função para criar a lista de destinatários
    destinatarios = email_list(destinatarios)
    destinatarios_cc = email_list(destinatarios_cc)

    path_html = os.path.join(
        os.environ.get("BASE_PATH_DAGS"),
        "utils",
        "DagReceitaEstimada",
        "templates",
    )

    # Template base do e-mail
    template_raw = open(
        os.path.join(path_html, "template.html"),
        encoding="utf-8",
    ).read()

    # Subsituir o texto que aparece antes do e-mail ser aberto
    template_raw = template_raw.replace("{PREVIA_EMAIL}", previa)

    # Subsitituir o conteúdo do e-mail
    template = template_raw.replace("{CONTEUDO}", template)

    # Criar JSON contendo os parâmetros do envio
    json_email = {
        "message": {
            "subject": titulo,
            "body": {
                "contentType": "HTML",
                "content": template,
            },
            "toRecipients": destinatarios,
            "ccRecipients": destinatarios_cc,
        }
    }

    # Enviar o e-mail
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)
    response = drive.send_email_endpoint(item_id=item_id, json=json_email)


def business_days(date: datetime, final: [datetime, str] = "last_day") -> int:
    """Função para calcular número de dias úteis no período de acordo com o calendário da B3

    Args:
        date (datetime): primeiro dia do intervalo
        final (datetime, str], optional): Último dia do intervalo. Será o dia atual ou o último dia do mês. Defaults to 'last_day'.

    Returns:
        int: número de dias úteis no intervalo especificado
    """
    # Utilizar calendário de dias úteis da B3
    bmf = mcal.get_calendar("BMF")

    # Caso a opção seja o último dia do mês atual
    # Extrair a dia do fechamento do mês para calcular
    # o número de dias úteis no mês em questão
    if final == "last_day":
        month = pd.bdate_range(
            start=f"{datetime.now().year}-{datetime.now().month}-01",
            end=f"{datetime.now().year}-12-31",
            freq="CBM",
            holidays=bmf.holidays().holidays[2000:],
        )

        month = month.to_frame()
        month = month[0].map(lambda x: x.strftime("%Y-%m-%d")).tolist()

        # Último dia útil do mês atual
        final = month[0]

    # Calcular o número de dias úteis no período
    early = bmf.schedule(start_date=date, end_date=final)
    cal = mcal.date_range(early, frequency="1D")

    return len(cal)


def col_to_datetime(df: pd.DataFrame, datetime_columns: list) -> pd.DataFrame:
    """Função para converter colunas de data no tipo correto, mantendo apenas ano, mês e dia

    Args:
        df (pd.DataFrame): dataframe que contém as colunas que serão tratadas
        datetime_columns (list): lista de colunas que serão convertidas

    Returns:
        pd.DataFrame: dataframe com as colunas de data convertidas
    """
    for col in datetime_columns:
        # Remover informações adicionais da coluna de data
        # Fiz dessa forma, pois podem existir diversos formatos de data
        df[col] = df[col].astype(str).str.split(" ").str[0]

        # Remover hora, o tipo será object
        df[col] = pd.to_datetime(df[col]).dt.date

        # Converter em datetime
        df[col] = pd.to_datetime(df[col])

    return df


def posicao(max_date: pd.Timestamp) -> pd.DataFrame:
    """Ler a base de posição nos meses selecionados

    Args:
        max_date (pd.Timestamp): data do último relatório de receita. Serve de base para "filtrar" a base de posição

    Returns:
        pd.DataFrame: dataframe contendo as posições no período selecionado
    """
    now = datetime.now()
    partitions = {
        (str(i.year), str(i.month))
        for i in pd.date_range(max_date, now.date(), freq="M")
    }
    partitions.add((str(now.year), str(now.month)))
    partition_filters = [
        [("ano_particao", "=", yearmonth[0]), ("mes_particao", "=", yearmonth[1])]
        for yearmonth in list(partitions)
    ]

    bucket_gold = get_bucket_name("lake-gold")
    dt_posicao = DeltaTable(f"s3://{bucket_gold}/btg/posicao/")
    df = dt_posicao.to_pandas(filters=partition_filters)
    df = padroniza_posicao(df)

    return df


def get_most_recent_file(**kwargs) -> pd.DataFrame:
    """Função para identificar e ler o arquivo mais recente de determinado bukcet

    Returns:
        pd.DataFrame: dataframe mais recente do bucket selecionado
    """
    s3 = boto3.client("s3")

    # Listar os arquivos a partir dos filtros especificados (bucket, prefixo, data de início, ...)
    files = s3.list_objects_v2(**kwargs)

    # Transformar a lista em dataframe
    df_files = pd.DataFrame(files["Contents"])

    # Extrair o path (prefixo + nome) do arquivo mais recente
    key = df_files[df_files["LastModified"] == df_files["LastModified"].max()][
        "Key"
    ].iloc[0]

    # Transformar a base em dataframe
    bucket = kwargs["Bucket"]
    df = pd.read_parquet(f"s3://{bucket}/{key}")

    return df


def real_br_money_mask(my_value: [float, str, int]) -> str:
    """Converter números para o padrão BR (000.000,00)

    Args:
        my_value (float, str, int]): valor que será convertido

    Returns:
        str: valor no novo formato
    """
    a = "{:,.2f}".format(float(my_value))
    b = a.replace(",", "v")
    c = b.replace(".", ",")

    return c.replace("v", ".")


def remover_trades_internos_duplicados(df: pd.DataFrame) -> pd.DataFrame:
    """Função para remover trades internos que também aparecem na base de movimentação

    Args:
        df (pd.DataFrame): dataframe contendo todas as movimentações, incluindo trades internos

    Returns:
        pd.DataFrame: dataframe resultante, sem trades internos duplicados
    """
    # Separar os trades internos
    df_trades_internos = df[df["Trade Interno"] == True]

    # Unir base sem trades internos com a base de trades internos
    # para identificar os registros que aparecem em ambos os dataframes
    df_result = df[df["Trade Interno"] != True].merge(
        df_trades_internos,
        on=[
            "Conta",
            "Tipo",
            "Categoria",
            "Produto",
            "Ativo",
            "Data",
        ],
        how="outer",
        indicator=True,
        suffixes=("", "_trades"),
    )

    # Manter apenas os registros não duplicados
    df_result = df_result[df_result["_merge"] == "left_only"]

    return pd.concat([df_result, df_trades_internos])


def extrair_data_maxima_da_receita(
    tipo: str = "all",
    categoria: str = "all",
    produto: str = "all",
    intermediador: str = "all",
    return_date: bool = True,
) -> pd.Timestamp:
    """Função para extrair a data do registro mais recente na base de receita.
    De acordo com os filtros especificados, seja tipo, categoria e/ou produto.

    Args:
        tipo (str, optional): tipo a ser filtrado, o default é selecionar tudo. Defaults to 'all'.
        categoria (str, optional): categoria a ser filtrada, o default é selecionar tudo. Defaults to 'all'.
        produto (str, optional): produto a ser filtrado, o default é selecionar tudo. Defaults to 'all'.
        intermediador (str, optional): intermediador a ser filtrado (principalmente para câmbio), o default é selecionar tudo.Defaults to 'all'.
        return_date (bool): parâmetro para definir se o retorno será apenas a data mais recente (True) ou todas as datas (False) que atendem os critérios especificados. Defaults to True

    Returns:
        pd.Timestamp: data do registro mais recente, de acordo com os filtros
    """
    df = pd.read_json(
        "s3://prd-dados-lake-landing-oneinvest/api/one/receita/data_maxima_por_produto.json",
        orient="index",
    )

    filtro_tipo = True if tipo == "all" else df["Tipo"] == tipo
    filtro_categoria = True if categoria == "all" else df["Categoria"] == categoria
    filtro_produto = True if produto == "all" else df["Produto_1"] == produto
    filtro_intermediador = (
        True
        if intermediador == "all"
        else df["Intermediador"].astype(str).str.contains(intermediador)
    )

    if (
        isinstance(filtro_tipo, bool)
        & isinstance(filtro_categoria, bool)
        & isinstance(filtro_produto, bool)
        & isinstance(filtro_intermediador, bool)
    ):
        pass
    else:
        df = df[filtro_tipo & filtro_categoria & filtro_produto & filtro_intermediador]

    df["Data"] = pd.to_datetime(df["Data"])

    if return_date:
        data_maxima = df["Data"].max()

        return data_maxima

    return df


def tratamento_movimentacao(df: pd.DataFrame, duplicated_cols: list) -> pd.DataFrame:
    """Função para tratar e remover duplicadas da base de movimentação
        duplicated_cols:
            RV -> ['account', 'launch', 'stock', 'gross_value', 'purchase_date']
            RF -> ['account', 'product', 'stock', 'indexer', 'maturity_date', 'purchase_date']
            COE -> 'all'

    Args:
        df (pd.DataFrame): base de movimentação
        duplicated_cols (list): colunas para usar no drop_duplicates

    Returns:
        pd.DataFrame: base de movimentação tratada
    """
    # Alterar tipo das colunas
    df["account"] = df["account"].astype(str)
    df["stock"] = df["stock"].astype(str)
    df["launch"] = df["launch"].astype(str)
    df["purchase_date"] = pd.to_datetime(df["purchase_date"])
    df["maturity_date"] = pd.to_datetime(df["maturity_date"])

    # Ordenar para remover duplicadas, mantendo apenas os registros mais recentes
    df = df.sort_values("purchase_date")
    df = df.drop_duplicates(duplicated_cols, keep="last")

    return df


def movimentacao_rv(drive: str, max_date: pd.Timestamp) -> pd.DataFrame:
    """Função para ler e tratar base de movimentação, mantendo apenas Renda Variável

    Args:
        drive (str): ID do drive do Onedrive onde reside o arquivo
        max_date (pd.Timestamp): data do último relatório de receita contendo Renda Variável

    Returns:
        pd.DataFrame: base de movimentação tratada
    """
    # Ler a base de movimentação do Histórico
    # Definir o caminho do arquivo
    path_movimentacao = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "movimentacao.csv",
    ]

    # Ler o conteúdo do arquivo
    movimentacao = drive.download_file(path=path_movimentacao).content
    df_movimentacao_raw = pd.read_csv(io.BytesIO(movimentacao), sep=";", decimal=",")

    # Padronizar base de movimentação
    df_movimentacao_raw = tratamento_movimentacao(
        df_movimentacao_raw,
        duplicated_cols=["account", "launch", "stock", "gross_value", "purchase_date"],
    )

    # Filtrar apenas as movimentações que devem ser mantidas
    df_movimentacao = df_movimentacao_raw[
        (df_movimentacao_raw["market"] == "RENDA VARIÁVEL")
        & (df_movimentacao_raw["launch"].isin(["COMPRA", "VENDA"]))
        & (df_movimentacao_raw["purchase_date"] > max_date)
    ]

    return df_movimentacao


def write_delta_table(
    df: pd.DataFrame,
    partition_col: str,
    dt_path: str,
    mode: str,
    partition_by: list = ["ano", "mes"],
):
    """Função para criar as colunas de partição e salvar em Delta

    Args:
        df (pd.DataFrame): dataframe que será salvo
        partition_col (str): nome da coluna que será usada como partição
        dt_path (str): caminho para salvar a tabela
        mode (str): modo que a tabela deve ser salva
        partition_by (list, optional): nome das partições que serão criadas (ano, mes e/ou dia). Defaults to ["ano", "mes"].
    """
    # Se a partition_col for um padrão invés do nome da coluna
    if partition_col not in df.columns:
        # Adiciona colunas de partições
        # De: 'ano_particao=2023/mes_particao=9'
        # Para: [('ano_particao', '=', '2023'), ('mes_particao', '=', '9')]
        partitions = []
        for part in partition_col.split("/"):
            split = part.split("=")
            part_name = split[0]
            part_value = split[1]

            df[part_name] = part_value
            partitions.append((part_name, "=", part_value))

        print("Colunas de partições adicionadas ao DataFrame")

        # Adiciona o nome das colunas à lista de partições
        partition_by = [part[0] for part in partitions]

    # Se a partition_col for o nome de uma coluna
    # Criar as partições a partir da coluna especificada
    else:
        for part in partition_by:
            if part == "ano":
                df["ano"] = pd.to_datetime(df[partition_col]).dt.year
            if part == "mes":
                df["mes"] = pd.to_datetime(df[partition_col]).dt.month
            if part == "dia":
                df["dia"] = pd.to_datetime(df[partition_col]).dt.day

        print("Colunas de partições adicionadas ao DataFrame")

    # Escreve no S3
    write_deltalake(
        dt_path,
        df,
        mode=mode,
        partition_by=partition_by,
    )
    print(f"Append realizado no bucket {dt_path}")
