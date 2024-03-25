import pymsteams
from airflow.models import Variable
import json


def msteams_task_failed(context: dict, webhook: str = None) -> None:
    """Função chamada automaticamente em caso de
    falha na execução de tasks do Airflow.
    Alertas são enviados para canl do Microsoft
    Teams.

    Args:
        context (dict): Template Variables do Airflow:
        https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
        webhook: Webhook do canl do Teams para o qual se deseja enviar alertas.
        Se nenhum for definido, enviará no canal geral de quebras de pipelines
    """
    if not webhook:
        webhook = Variable.get("msteams_webhook_alertas")

    ti = context["task_instance"]
    dag = context["dag"]
    owner = dag.default_args["owner"]
    email = dag.default_args["email"][0]

    text_list = [
        f"**DAG:** {ti.dag_id}",
        f"**Task:** {ti.task_id}",
        f"**Execution Date:** {ti.execution_date}",
        f"**Responsável:** {owner}",
    ]
    log_url = ti.log_url.replace("localhost", Variable.get("airflow_host"))

    # Mensagem com detalhes do erro
    myTeamsMessage = pymsteams.connectorcard(webhook)
    myTeamsMessage.title(f"Falha na Execução de Task")
    myTeamsMessage.text("<br>".join(text_list))
    myTeamsMessage.addLinkButton("Logs", log_url)
    myTeamsMessage.send()

    # Mensagem com menção do Teams
    myTeamsMessage = pymsteams.connectorcard(webhook)
    myTeamsMessage.payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "body": [{"type": "TextBlock", "text": f"<at>{owner}</at>"}],
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.0",
                    "msteams": {
                        "entities": [
                            {
                                "type": "mention",
                                "text": f"<at>{owner}</at>",
                                "mentioned": {"id": f"{email}", "name": f"{owner}"},
                            }
                        ]
                    },
                },
            }
        ],
    }
    myTeamsMessage.send()


def msteams_refresh_pbi(params: dict, webhook: str):
    """Envia mensagem para canal de equipe no Teams contendo
    informações sobre a Dag Run que originou a mensagem. Fluxos
    de Power Automate devem monitorar esses canais e acionar uma
    atualização de Dashboard sempre que uma mensagem nova é criada

    Args:
        params (dict): Parâmetros enviados pela DAG 'upstream'
        contendo informações para identificar quem originou a mensagem
        webhook (str): Webhook do canal do Teams. Cada Dataset do PBI
        deveria possuir um canal diferente com seu próprio webhook
    """

    text_list = [
        f"**DAG:** {params['dag_id']}",
        f"**Task:** {params['task_id']}",
        f"**Execution Date:** {params['execution_date']}",
    ]

    # Mensagem com detalhes da Dag Run que originou a mensagem
    myTeamsMessage = pymsteams.connectorcard(webhook)
    myTeamsMessage.title(f"Refresh Automatizado")
    myTeamsMessage.text("<br>".join(text_list))
    myTeamsMessage.send()


def msteams_qualidade_dados(log_qualidade: str, webhook: str):
    indented = json.dumps(json.loads(log_qualidade), indent=4, ensure_ascii=False)

    # Mensagem com detalhes de problemas de qualidade de dados
    myTeamsMessage = pymsteams.connectorcard(webhook)
    myTeamsMessage.title(f"Problema de Qualidade")
    myTeamsMessage.text(indented)
    myTeamsMessage.send()
