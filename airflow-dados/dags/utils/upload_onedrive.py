import requests
import json
import pandas as pd
import io
import pyarrow as pa
import pyarrow.parquet as pq
from utils.requests_function import http_session


def upload_onedrive(
    df,
    file_name,
    token,
    itemId,
    driveId="b!k-8TlF8nU0qruYdk5YJWRl3CDlGvRCVMvFqk-P2Bh4WkINmc7OQrSIcb_LUwG_M2",
):
    """Realiza o upload de um DataFrame para o OneDrive no formato escolhido por meio da API Graph.
    Retorna uma mensagem confirmando se o upload do arquivo foi concluído corretamente.

    Args:
        df (pandas DataFrame): DataFrame que será enviado para o OneDrive.
        file_name (str): Nome do arquivo a ser salvo no OneDrive,
                        incluir o formato do arquivo que deseja salvar ('.csv', '.xlsx' ou '.parquet').
        token (str): Token de acesso à API Graph para autenticação.
        itemId (str): ID do item (pasta) no OneDrive onde o arquivo será salvo.
        driveId (str): ID do drive (OneDrive) onde o arquivo será salvo,
                        caso não especificado será o do tenant da @investimentos.one


    Returns:
        str: Uma mensagem indicando o resultado do upload. Se o upload for bem-sucedido,
             a mensagem será "Upload realizado com sucesso. O arquivo 'file_name' foi ATUALIZADO/CRIADO no local de destino."
             Caso contrário, a mensagem conterá uma notificação de falha e o código de status da requisição.
    """

    # Validar se o formato do arquivo fornecido é suportado pela função
    file_format = file_name.split(".")[-1]
    if file_format not in ["csv", "xlsx", "parquet"]:
        raise ValueError(
            "Formato de arquivo não suportado. Lembre de incluir '.csv', '.xlsx' ou '.parquet'."
        )

    # Criar uma sessão para upload na pasta do OneDrive especificada
    url = f"https://graph.microsoft.com/v1.0/drives/{driveId}/items/{itemId}:/{file_name}:/createUploadSession"

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "Item": {"@microsoft.graph.conflictBehavior": "replace", "name": f"{file_name}"}
    }
    session = http_session()
    response = session.request("POST", url, headers=headers, data=json.dumps(payload))

    # Converter o df para bytes
    if file_format == "csv":
        # Para csv quando não especifica o path o df vira uma variável
        bytes_file = df.to_csv(
            sep=";", decimal=",", index=False
        ).encode("utf-8")
        content_type = "text/csv"
    elif file_format == "xlsx":
        # Utilizar um buffer para conversão
        with io.BytesIO() as buffer:
            df.to_excel(buffer, index=False)
            bytes_file = buffer.getvalue()
        content_type = (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:  # file_format == 'parquet'
        # Utilizar um buffer para conversão
        with io.BytesIO() as buffer:
            df.to_parquet(buffer)
            bytes_file = buffer.getvalue()
        content_type = "application/octet-stream"

    # Extrair o tamanho do df em bytes
    size_df = len(bytes_file)
    # Definir o tamanho máximo de cada parte (em bytes)
    limit_size = 50000000
    part_size = 25000000

    if (
        size_df < limit_size
    ):  # Caso o arquivo seja menor que 50 MB será baixado de uma vez em uma única requisição
        # Salvar a base de uma vez na pasta
        headers_upload = {
            "Authorization": f"Bearer {token}",
            "Content-Range": f"bytes 0-{size_df-1}/{size_df}",
            "Content-Type": content_type,
        }
        session = http_session()
        response_upload = session.request(
            "PUT", response.json()["uploadUrl"], headers=headers_upload, data=bytes_file
        )

        # Verificar se o upload foi bem-sucedido e retornar mensagem apropriada
        if response_upload.status_code == 200:
            message = f"Upload realizado com sucesso. O arquivo {file_name} foi ATUALIZADO no local de destino."
        elif response_upload.status_code == 201:
            message = f"Upload realizado com sucesso. O arquivo {file_name} foi CRIADO no local de destino."
        else:
            message = f"O upload do arquivo '{file_name}' falhou. Código de status: {response_upload.status_code}."

    else:  # Caso o arquivo seja maior que 50 MB será baixado em várias requisição de 25 MB cada
        # Inicializa o byte inicial
        start_byte = 0

        # Divide o arquivo em partes e faz upload de cada parte
        while start_byte < size_df:
            end_byte = min(start_byte + part_size - 1, size_df - 1)

            # Constrói os cabeçalhos para esta parte
            content_range = f"bytes {start_byte}-{end_byte}/{size_df}"
            content_length = end_byte - start_byte + 1
            part_bytes_file = bytes_file[start_byte : end_byte + 1]

            headers_upload = {
                "Authorization": f"Bearer {token}",
                "Content-Length": str(content_length),
                "Content-Range": content_range,
                "Content-Type": content_type,
            }

            # Faz o upload da parte
            session = http_session()
            response_upload = session.request(
                "PUT",
                response.json()["uploadUrl"],
                headers=headers_upload,
                data=part_bytes_file,
            )

            # Verifica se a resposta é 202 (correspondente a parte ser enviada com sucesso porém o download total ainda não ser concluído) e continua fazendo upload das partes
            if response_upload.status_code == 202:
                print(f"Parte {start_byte}-{end_byte} enviada com sucesso.")
            elif response_upload.status_code in (200, 201):
                print(f"Última parte {start_byte}-{end_byte} enviada com sucesso.")
            else:
                # Se não for 200, 201 ou 202, algo deu errado, registrar um erro.
                print(
                    f"Erro no envio da parte {start_byte}-{end_byte}. Código de status: {response_upload.status_code}."
                )

            # Atualiza o byte inicial para a próxima parte
            start_byte = end_byte + 1

        # Após o upload de todas as partes, retorna a resposta final
        if response_upload.status_code == 200:
            message = f"Upload realizado com sucesso. O arquivo {file_name} foi ATUALIZADO no local de destino."
        elif response_upload.status_code == 201:
            message = f"Upload realizado com sucesso. O arquivo {file_name} foi CRIADO no local de destino."
        else:
            message = f"O upload do arquivo '{file_name}' falhou. Código de status: {response_upload.status_code}."

    return message
