from utils.bucket_names import get_bucket_name
from datetime import datetime
import json
from utils.secrets_manager import get_secret_value
from utils.OneGraph.drive import OneGraphDrive
from airflow.models import Variable
import pytz


def gera_context_script():
    dict_credencias_microsft = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive_id_gui = Variable.get("guilherme_drive_id")

    drive_microsoft = OneGraphDrive(
        dict_credencias_microsft["client_id"],
        dict_credencias_microsft["client_secret"],
        dict_credencias_microsft["tenant_id"],
        drive_id_gui,
    )

    context_script = {
        "id_microsoft_backoffice": "ac1dec21-e969-46eb-87b8-aca609787837",
        "id_microsoft_tecnologia": "d64bcd7f-c47a-4343-b0b4-73f4a8e927a5",
        "bucket_lake_bronze": get_bucket_name("lake-bronze"),
        "bucket_lake_silver": get_bucket_name("lake-silver"),
        "bucket_lake_gold": get_bucket_name("lake-gold"),
        "datetime_hoje_brasil": datetime.utcnow()
        .replace(tzinfo=pytz.utc)
        .astimezone(pytz.timezone("America/Sao_Paulo")),
        "dict_credencias_microsft": dict_credencias_microsft,
        "drive_microsoft": drive_microsoft,
        "lista_caminhos_reservas": [
            "One Analytics",
            "Inteligência Operacional",
            "9 - Back Office",
            "12 - Ofertas Públicas - BTG",
            "3. Código",
            "Relatorios",
            "Reserva",
        ],
        "lista_caminhos_pushes": [
            "One Analytics",
            "Inteligência Operacional",
            "9 - Back Office",
            "12 - Ofertas Públicas - BTG",
            "3. Código",
            "Relatorios",
            "Push",
        ],
        "lista_caminhos_salvar_reservas": [
            "One Analytics",
            "Inteligência Operacional",
            "9 - Back Office",
            "12 - Ofertas Públicas - BTG",
            "3. Código",
            "acompanhamento_reservas.csv",
        ],
        "lista_caminhos_salvar_pushes": [
            "One Analytics",
            "Inteligência Operacional",
            "9 - Back Office",
            "12 - Ofertas Públicas - BTG",
            "3. Código",
            "acompanhamento_pushes.csv",
        ],
    }
    return context_script
