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
    dag_id="Dag_Pushes_Ofertas_Publicas",
    default_args=default_args,
    schedule_interval="0,15,30,45 11-23 * * 1-5",
    tags=[
        "pushes",
        "oferta_publica",
        "backoffice",
    ],
    catchup=False,
    description="Pipeline para atualizar dados de Pushes de Ofertas Públicas e disparar os e-mails necessários",
    max_active_runs=1,
    max_active_tasks=2,
) as dag:
    verifica_arquivos_pushes = BranchPythonOperator(
        task_id="verifica_arquivos_pushes",
        python_callable=monitora_arquivos_individuais,
        op_kwargs={"base": "pushes"},
    )
    nao_faz_nada = EmptyOperator(task_id="nao_faz_nada")
    atualiza_consolidado_pushes = PythonOperator(
        task_id="atualiza_consolidado_pushes",
        python_callable=gera_base_reservas_ou_pushes_consolidados,
        op_kwargs={"base": "pushes"},
    )
    atualiza_base_emails_pushes = PythonOperator(
        task_id="atualiza_base_emails_pushes",
        python_callable=gera_base_emails_pushes_reservas,
        op_kwargs={"base": "pushes"},
    )

(verifica_arquivos_pushes >> atualiza_consolidado_pushes >> atualiza_base_emails_pushes)
verifica_arquivos_pushes >> nao_faz_nada
