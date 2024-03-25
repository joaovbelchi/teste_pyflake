from utils.bucket_names import get_bucket_name
from datetime import datetime
import json
from utils.secrets_manager import get_secret_value
from airflow.models import Variable
import pytz
from utils.OneGraph.drive import OneGraphDrive


def gera_context_script():
    dict_credencias_microsft = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    id_drive_gui = Variable.get("guilherme_drive_id")

    context_script = {
        "colunas_pl": {
            "Conta": {
                "nome": "conta",
                "tipo": str,
            },
            "PL": {"nome": "pl", "tipo": float},
        },
        "id_microsoft_tecnologia": "d64bcd7f-c47a-4343-b0b4-73f4a8e927a5",
        "bucket_lake_bronze": get_bucket_name("lake-bronze"),
        "bucket_lake_silver": get_bucket_name("lake-silver"),
        "bucket_lake_gold": get_bucket_name("lake-gold"),
        "datetime_hoje_brasil": datetime.now()
        .replace(tzinfo=pytz.utc)
        .astimezone(pytz.timezone("America/Sao_Paulo")),
        "dict_credencias_microsft": dict_credencias_microsft,
        "drive_microsoft": OneGraphDrive(
            dict_credencias_microsft["client_id"],
            dict_credencias_microsft["client_secret"],
            dict_credencias_microsft["tenant_id"],
            id_drive_gui,
        ),
        "id_drive_gui": id_drive_gui,
        "parametro_relevancia_rf": 0.01,
        "caminho_arquivo_monitoramento_rf": [
            "One Analytics",
            "Banco de dados",
            "Histórico",
            "arquivo_monitoramento_rf.csv",
        ],
    }

    return context_script
