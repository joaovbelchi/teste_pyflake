from utils.usual_data_transformers import (
    formata_numero_para_texto_monetario,
)
from utils.bucket_names import get_bucket_name
from datetime import datetime, timedelta
import json
from utils.secrets_manager import get_secret_value
from pandas import Int64Dtype
from airflow.models import Variable
from utils.OneGraph.drive import OneGraphDrive


def gera_context_script():
    dict_credencias_microsft = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    guilherme_drive_id = Variable.get("guilherme_drive_id")

    # ### DESCOMENTE PARA RODAR LOCAL
    # guilherme_drive_id = (
    #     "b!4Mc9fMTjgUaepXg-3gt94-UT15JpzDhNmb5QWUCac1if1Sv5LzNnS6_1MnjapCcC"
    # )
    # ###

    drive_microsoft = OneGraphDrive(
        dict_credencias_microsft["client_id"],
        dict_credencias_microsft["client_secret"],
        dict_credencias_microsft["tenant_id"],
        guilherme_drive_id,
    )

    context_script = {
        "colunas_socios": {
            "Nome_bd": {
                "nome": "nome_assessor",
                "tipo": str,
            },
            "E-mail": {
                "nome": "email_assessor",
                "tipo": str,
            },
            "Área": {
                "nome": "area",
                "tipo": str,
            },
            "Celular Pessoal": {
                "nome": "telefone_assessor",
                "tipo": str,
            },
        },
        "colunas_saldo_em_conta": {
            "Conta": {
                "nome": "conta",
                "tipo": str,
            },
            "Saldo": {
                "nome": "saldo",
                "tipo": float,
            },
        },
        "colunas_contas_btg": {
            "nr_conta": {
                "nome": "conta",
                "tipo": str,
            },
            "nome_completo": {
                "nome": "nome_cliente",
                "tipo": str,
            },
        },
        "colunas_contas_assessores": {
            "Conta": {
                "nome": "conta",
                "tipo": str,
            },
            "Comercial": {
                "nome": "nome_assessor",
                "tipo": str,
            },
            "Operacional RF": {
                "nome": "nome_operacional_rf",
                "tipo": str,
            },
            "Operacional RV": {
                "nome": "nome_operacional_rv",
                "tipo": str,
            },
        },
        "colunas_pl": {
            "Conta": {
                "nome": "conta",
                "tipo": str,
            },
            "PL": {"nome": "pl", "tipo": float},
        },
        "colunas_dias_de_saldo_positivo": {
            "account": {
                "nome": "conta",
                "tipo": str,
            },
            "positive_days": {"nome": "dias_positivos", "tipo": Int64Dtype()},
        },
        "colunas_dias_de_saldo_negativo": {
            "account": {
                "nome": "conta",
                "tipo": str,
            },
            "negative_days": {"nome": "dias_negativos", "tipo": Int64Dtype()},
        },
        "colunas_formato_envio_email_assessor": {
            "conta": {
                "nome": "Conta",
                "tipo": str,
            },
            "nome_cliente": {"nome": "Nome", "tipo": str},
            "saldo": {
                "nome": "Saldo",
                "tipo": float,
                "formatar": lambda x: "R$ " + formata_numero_para_texto_monetario(x),
            },
            "nome_assessor": {"nome": "Comercial", "tipo": str},
            "nome_operacional_rf": {"nome": "Operacional RF", "tipo": str},
            "nome_operacional_rv": {"nome": "Operacional RV", "tipo": str},
            "dias_negativos": {"nome": "Dias negativos", "tipo": Int64Dtype()},
            "pl": {
                "nome": "PL",
                "tipo": float,
                "formatar": lambda x: "R$ " + formata_numero_para_texto_monetario(x),
            },
            "percent_pl": {
                "nome": "% Saldo Negativo / PL",
                "tipo": float,
                "formatar": lambda x: "{:,.2%}".format(x)
                .replace(".", ",")
                .replace(",", "."),
            },
        },
        "colunas_formato_envio_email_positivos": {
            "conta": {
                "nome": "Conta",
                "tipo": str,
            },
            "nome_cliente": {"nome": "Nome", "tipo": str},
            "saldo": {
                "nome": "Saldo",
                "tipo": float,
                "formatar": lambda x: "R$ " + formata_numero_para_texto_monetario(x),
            },
            "nome_assessor": {"nome": "Comercial", "tipo": str},
            "nome_operacional_rf": {"nome": "Operacional RF", "tipo": str},
            "nome_operacional_rv": {"nome": "Operacional RV", "tipo": str},
            "dias_positivos": {"nome": "Dias positivos", "tipo": Int64Dtype()},
            "pl": {
                "nome": "PL",
                "tipo": float,
                "formatar": lambda x: "R$ " + formata_numero_para_texto_monetario(x),
            },
            "percent_pl": {
                "nome": "% Saldo Negativo / PL",
                "tipo": float,
                "formatar": lambda x: "{:,.2%}".format(x)
                .replace(".", ",")
                .replace(",", "."),
            },
        },
        "id_microsoft_backoffice": "ac1dec21-e969-46eb-87b8-aca609787837",
        "id_microsoft_tecnologia": "d64bcd7f-c47a-4343-b0b4-73f4a8e927a5",
        "bucket_lake_bronze": get_bucket_name("lake-bronze"),
        "bucket_lake_silver": get_bucket_name("lake-silver"),
        "bucket_lake_gold": get_bucket_name("lake-gold"),
        "datetime_hoje_brasil": datetime.now() - timedelta(hours=3),
        "dict_credencias_microsft": dict_credencias_microsft,
        "drive_microsoft": drive_microsoft,
        "lista_caminhos_saldo_novos": [
            "One Analytics",
            "Banco de dados",
            "Novos",
            "Saldo em CC (D 0).xlsx",
        ],
        "lista_caminhos_saldo_historico": [
            "One Analytics",
            "Banco de dados",
            "Histórico",
            "Saldo em CC (D 0).csv",
        ],
        "lista_caminhos_saldos_negativos": [
            "One Analytics",
            "Banco de dados",
            "Outputs",
            "output_contador_negativo.xlsx",
        ],
        "lista_filtros_sem_assessor": [
            "ONE INVESTIMENTOS",
            "NAO CONTATAR",
            "HOME BROKER",
        ],
    }

    return context_script
