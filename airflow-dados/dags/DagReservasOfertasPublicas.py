from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from utils.msteams import msteams_task_failed

from utils.Commons.DagsReservasOfertasPublicasPushesOfertasPublicas.functions import (
    gera_base_reservas_ou_pushes_consolidados,
    gera_base_emails_pushes_reservas,
    monitora_arquivos_individuais,
)

default_args = {
    "owner": "Gustavo Sousa",
    "start_date": datetime(2023, 10, 25),
    "on_failure_callback": msteams_task_failed,
    "email": ["gustavo.sousa@investimentos.one"],
    "retries": 0,
}

with DAG(
    dag_id="Dag_Reservas_Ofertas_Publicas",
    default_args=default_args,
    schedule_interval="0,15,30,45 11-23 * * 1-5",
    tags=[
        "reservas",
        "oferta_publica",
        "backoffice",
    ],
    catchup=False,
    description="Pipeline para atualizar dados de Reservas de Ofertas Públicas",
    max_active_runs=1,
    max_active_tasks=2,
) as dag:
    verifica_arquivos_reservas = BranchPythonOperator(
        task_id="verifica_arquivos_reservas",
        python_callable=monitora_arquivos_individuais,
        op_kwargs={"base": "reservas"},
    )
    nao_faz_nada = EmptyOperator(task_id="nao_faz_nada")
    atualiza_consolidado_reservas = PythonOperator(
        task_id="atualiza_consolidado_reservas",
        python_callable=gera_base_reservas_ou_pushes_consolidados,
        op_kwargs={"base": "reservas"},
    )
    atualiza_base_emails_reservas = PythonOperator(
        task_id="atualiza_base_emails_reservas",
        python_callable=gera_base_emails_pushes_reservas,
        op_kwargs={"base": "reservas"},
    )

(
    verifica_arquivos_reservas
    >> atualiza_consolidado_reservas
    >> atualiza_base_emails_reservas
)
verifica_arquivos_reservas >> nao_faz_nada
