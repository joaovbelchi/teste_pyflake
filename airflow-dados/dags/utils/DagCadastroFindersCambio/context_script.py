from utils.bucket_names import get_bucket_name
from datetime import datetime
import json
from utils.secrets_manager import get_secret_value
import pytz


def gera_context_script():
    dict_credencias_microsft = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )

    context_script = {
        "id_microsoft_tecnologia": "d64bcd7f-c47a-4343-b0b4-73f4a8e927a5",
        "bucket_lake_gold": get_bucket_name("lake-gold"),
        "bucket_lake_landing": get_bucket_name("lake-landing"),
        "datetime_hoje_brasil": datetime.utcnow()
        .replace(tzinfo=pytz.utc)
        .astimezone(pytz.timezone("America/Sao_Paulo")),
        "dict_credencias_microsft": dict_credencias_microsft,
        "link_app_cadastro_finder": "https://cadastrofinders.oneholding.com.br/Cadastrar_escalonamento_finders",
        "link_app_cadastro_indicacao_finder": "https://cadastrofinders.oneholding.com.br/Associar_cliente_finder",
    }

    return context_script
