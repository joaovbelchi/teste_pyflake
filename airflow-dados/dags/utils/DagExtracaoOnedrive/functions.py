from dateutil import parser
from datetime import date, timedelta
from utils.OneGraph.drive import OneGraphDrive
from utils.OneGraph.exceptions import DriveItemNotFoundException
from typing import List
import json
from utils.secrets_manager import get_secret_value
from utils.functions import email_list
import boto3
import pandas as pd


def verifica_arquivo_modificado_onedrive(
    drive_id: str, onedrive_path: List[str], days_dif: int = 0
) -> bool:
    """Verifica se um arquivo do OneDrive foi modificado na mesmo dia
    da execução desta função

    Args:
        drive_id (str): ID do drive do Onedrive onde reside o
        arquivo
        onedrive_path (List[str]): Lista contendo nomes de pastas e nome
        do arquivo no último item, formando o path. Exemplo:
        ['pasta_1', 'pasta_2', 'arquivo.csv']
        days_dif (int): Máximo de número de dias corridos em que a base pode ficar sem atualizar

    Returns:
        bool: Verdadeiro se o arquivo existe e foi modificado no
        mesmo dia de execução desta função. Falso, caso contrário
    """
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    try:
        rsp = drive.get_drive_item_metadata(path=onedrive_path)
    except DriveItemNotFoundException:
        print(f"Nenhum arquivo encontrado para o path '{onedrive_path}'")

    last_modified = parser.parse(rsp.json()["lastModifiedDateTime"])
    today = date.today()

    if last_modified.date() >= (today - timedelta(days=days_dif)):
        print(f"Arquivo atualizado encontrado. lastModifiedDateTime: {last_modified}")
        return True

    print(
        f"Arquivo encontrado, mas desatualizado. lastModifiedDateTime: {last_modified}"
    )

    return False


def transferencia_onedrive_para_s3(
    drive_id: str, onedrive_path: List[str], s3_bucket: str, s3_object_key: str
) -> None:
    """Faz o download de um arquivo localizado no Onedrive
    e realiza o upload do conteúdo para um objeto em um
    bucket do S3

    Args:
        drive_id (str): ID do drive do Onedrive onde reside o
        arquivo
        onedrive_path (List[str]): Lista contendo nomes de pastas e nome
        do arquivo no último item, formando o path. Exemplo:
        ['pasta_1', 'pasta_2', 'arquivo.csv']
        s3_bucket (str): Nome do bucket do S3 para onde se deseja
        realizar o upload
        s3_object_key (str): Nome desejado para o objeto no S3 (não
        incluir nome do bucket aqui. É da primeira pasta pra frente)
    """
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)
    bytes = drive.download_file(path=onedrive_path).content
    print("Download realizado de arquivo realizado.")

    client = boto3.client("s3")
    client.put_object(Body=bytes, Bucket=s3_bucket, Key=s3_object_key)
    print("Upload de arquivo realizado.")


def enviar_email_base_desatualizada(
    drive_id: str,
    item_id: str,
    planilha: str,
    days_dif: int,
    destinatarios: list,
):
    """Enviar e-mail informando que a base em questão não foi atualizada no período estabelecido

    Args:
        drive_id (str): ID do drive do Onedrive onde reside o arquivo
        item_id (str): ID de objeto do remetente do e-mail
        planilha (str): Nome da planilha em questão
        days_dif (int): Número de dias sem atualizar para que seja necessário alertar
        send_email (bool): Booleano para determinar se é ou não para enviar o e-mail
        destinatarios (list): Responsáveis pela planilha que deverão ser avisados sobre a necessidade de atualiza-la
    """
    # Aplicar a função para criar a lista de destinatários
    destinatarios = email_list(destinatarios)

    # Criar JSON contendo os parâmetros do envio
    json_email = {
        "message": {
            "subject": "Atenção! Base desatualizada",
            "body": {
                "contentType": "HTML",
                "content": f"A planilha {planilha} precisa de atenção, pois não foi atualizada nos últimos {days_dif} dias.",
            },
            "toRecipients": destinatarios,
        }
    }

    if len(destinatarios) > 0:
        credentials = json.loads(
            get_secret_value("apis/microsoft_graph", append_prefix=True)
        )
        drive = OneGraphDrive(drive_id=drive_id, **credentials)
        response = drive.send_email_endpoint(item_id=item_id, json=json_email)


def leitura_relatorios_run(
    s3_bucket_landing: str, s3_object_key: str, sheet_name: str, **kwargs
) -> pd.DataFrame:
    """Função para carregamento de arquivos contendo as informações dos ativos
    de Renda Fixa disponíveis (RUN)

    Args:
        s3_bucket_landing (str): Nome do bucket da camada Landing do Lake
        s3_object_key (str): Chave do objeto que se deseja carregar
        sheet_name (str): Nome da aba que será utilizada

    Returns:
        DataFrame: Dataframe pronto para processamento
    """
    df = pd.read_excel(
        f"s3://{s3_bucket_landing}/{s3_object_key}",
        sheet_name=sheet_name,
        dtype_backend="pyarrow",
        **kwargs,
    )

    if sheet_name == "Título Público":
        cols_names = {
            "TAXA PORTAL DAS 10hs¹": "TAXA PORTAL DAS 10hs",
            "RECEITA ESTIMADA³": "RECEITA ESTIMADA",
            "RECEITA ESTIMADA²": "RECEITA ESTIMADA",
        }

        # Identificar a coluna que divide o lado direito do esquerdo
        second_null_column = df.columns[df.isna().all()].to_list()[-1]

        # Separar o dataframe da esquerda e o da direita
        df_esquerda = df.loc[:, :second_null_column]
        df_direita = df.loc[:, second_null_column:]

        df = pd.DataFrame()
        for df_aux in [df_esquerda, df_direita]:
            # Identificar as linhas de cabeçalho
            segunda_coluna = df_aux.columns[1]
            index_header = (
                df_aux[df_aux[segunda_coluna] == "VENCIMENTO"]
                .dropna(how="all")
                .index.tolist()
            )

            for index in index_header:
                # Se for o quadrante superior, filtrar até o início do quadrante inferior
                if index == index_header[0]:
                    df_temp = df_aux[
                        (df_aux.index >= index) & (df_aux.index < index_header[1])
                    ]

                # Se não selecionar o final do dataframe
                else:
                    df_temp = df_aux[(df_aux.index >= index)]

                # Substituir os nomes das colunas
                df_temp.replace(cols_names, inplace=True)

                # Remover as linhas duplicadas, por exemplo igual ao cabeçalho
                df_temp = df_temp.drop_duplicates()

                # Remover linhas com menos de 3 valores preenchidos
                df_temp = df_temp.dropna(thresh=3)

                # Transformar a primeira linha em cabeçalho
                df_temp.columns = df_temp.iloc[0].values
                df_temp = df_temp.reset_index(drop=True)
                df_temp.drop(index=0, axis=0, inplace=True)

                # Remover as colunas vazias
                df_temp = df_temp.dropna(how="all", axis=1)

                # Definir o nome do produto
                # A linha do produto está acima do cabeçalho
                product_row = df_aux[df_aux.index == index - 1].reset_index(drop=True)

                # Identificar o nome da segunda coluna do dataframe
                col = df_aux.columns.to_list()[1]

                # Selecionar o nome do produto
                product = product_row.dropna(how="all", axis=1).iloc[0][col]

                # Criar a coluna de produto
                df_temp["Produto"] = product

                # Concatenar os dataframes
                df = pd.concat([df, df_temp])

    else:
        # Remover as linhas duplicadas, por exemplo igual ao cabeçalho
        df = df.drop_duplicates()

        # Remover linhas com menos de 3 valores preenchidos
        df = df.dropna(thresh=3)

        # Transformar a primeira linha em cabeçalho
        df.columns = df.iloc[0].values
        df = df.reset_index(drop=True)
        df.drop(index=0, axis=0, inplace=True)

        # Remover as colunas vazias
        df = df.dropna(how="all", axis=1)

    df = df.reset_index(drop=True)

    return df
