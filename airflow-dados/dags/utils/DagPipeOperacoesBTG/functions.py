import pandas as pd
import requests
import boto3
from io import BytesIO
from utils.bucket_names import get_bucket_name
from datetime import datetime
import pytz


def _excecao_webhooks_possiveis(webhook: str) -> None:
    """Verifica se o webhook está previsto no pipeline.

    Args:
        webhook (str): Webhook escolhido.

    Raises:
        Exception: Exceção caso o webhook não esteja previsto no pipline.
    """
    lista_webhooks_possiveis = ["operations-search", "pre-operation"]
    if webhook not in lista_webhooks_possiveis:
        raise Exception(
            f"Os webhooks possíveis até o momento para este pipeline são '{str(lista_webhooks_possiveis).strip['['].strip[']']}'."
        )


def salva_dados_webhook_landing(webhook: str) -> None:
    """Salva dados de webhook escolhido na landing.

    Args:
        webhook (str): Webhook escolhido.
    """

    _excecao_webhooks_possiveis(webhook)

    dict_validacoes_resposta_webhooks = {
        "operations-search": {
            "chave_tipo": "Content-Type",
            "valor_tipo": "application/zip",
            "extensao": "zip",
            "response": "operacoes",
        },
        "pre-operation": {
            "chave_tipo": "Content-Type",
            "valor_tipo": "application/zip",
            "extensao": "zip",
            "response": "pre_operacoes",
        },
    }

    df_webhook_response = pd.read_json(
        f"s3://{get_bucket_name('lake-landing')}/btg/webhooks/responses/{dict_validacoes_resposta_webhooks[webhook]['response']}.json",
    )
    try:
        link_dados = df_webhook_response.loc["url"]["response"]
    except Exception as e:
        raise Exception(
            f"Erro: {e} -- verifique o que ocorreu com a resposta do webhook."
        )

    bucket_name = f"{get_bucket_name('lake-landing')}"

    ts_requisicao = (
        datetime.utcnow()
        .replace(tzinfo=pytz.utc)
        .astimezone(pytz.timezone("America/Sao_Paulo"))
    )
    ano_requisicao = str(ts_requisicao.year)
    mes_requisicao = str(ts_requisicao.month)
    str_ts_requisicao = ts_requisicao.strftime("%Y%m%dT%H%M%S")

    caminho_arquivo_bucket = f"btg/webhooks/downloads/{webhook}/ano={ano_requisicao}/mes={mes_requisicao}/webhook_{str_ts_requisicao}.{dict_validacoes_resposta_webhooks[webhook]['extensao']}"

    response = requests.get(link_dados)
    data_bytes = response.content

    cabecalho_valida_tipo_arquivo = dict_validacoes_resposta_webhooks[webhook][
        "chave_tipo"
    ]
    valor_cabecalho_valida_tipo_arquivo = dict_validacoes_resposta_webhooks[webhook][
        "valor_tipo"
    ]
    try:
        valor_cabecalho_requisicao = response.headers[cabecalho_valida_tipo_arquivo]
    except KeyError:
        raise Exception(
            f"A chave '{cabecalho_valida_tipo_arquivo}' que usamos para validar o tipo de arquivo não foi encontrada. Verifique a requisição ou possíveis novas especificações do webhook."
        )
    except Exception as e:
        raise Exception(e)
    else:
        if valor_cabecalho_requisicao == valor_cabecalho_valida_tipo_arquivo:
            print(
                "Arquivo no formato certo, seguiremos com o salvamento do arquivo na landing!"
            )
            s3 = boto3.client("s3")
            s3.upload_fileobj(BytesIO(data_bytes), bucket_name, caminho_arquivo_bucket)
            print(
                f"Dados enviados com sucesso para o S3 em s3://{bucket_name}/{caminho_arquivo_bucket}"
            )
        else:
            raise Exception(
                f"O valor '{valor_cabecalho_valida_tipo_arquivo}' esperado no cabeçalho '{cabecalho_valida_tipo_arquivo}' não foi encontrado (encontramos '{valor_cabecalho_requisicao}'). Verifique possíveis novas especificações do webhook"
            )
