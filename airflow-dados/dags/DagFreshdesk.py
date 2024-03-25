from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from utils.msteams import msteams_task_failed

from utils.datasets import AGENTES_SILVER, TICKETS_LANDING, TICKETS_SILVER, TICKETS_GOLD
from airflow.operators.empty import EmptyOperator


def failure_callback_func(context):
    msteams_task_failed(context)


# DEFAULT SETTINGS APPLIED  TO ALL  TASKS
default_args = {
    "owner": "Nathalia Montandon",
    "start_date": datetime(2023, 10, 25),
    "email": ["nathalia.montandon@investimentos.one"],
    "on_failure_callback": failure_callback_func,
    "retries": 0,
    "kubernetes_conn_id": "con_eks_dados",
    "namespace": "processing",
}

## DAG  Crawler
with DAG(
    dag_id="DagFreshdesk",
    default_args=default_args,
    schedule_interval="30 12 * * 1-5",
    tags=["dev", "dados", "one", "backoffice"],
    catchup=False,
    description="Pipeline para atualizar base de dados do Freshdesk",
    max_active_runs=1,
) as dag:
    atualizar_dados_freshdesk = SparkKubernetesOperator(
        task_id="dag_atualizar_freshdesk",
        params=dict(
            app_name="one-atualizar-freshdesk",
            mainApplicationFile="local:///app/one-freshdesk.py",
        ),
        application_file="ConfigSpark6G.yaml",
        do_xcom_push=True,
    )

    monitor_atualizar_freshdesk = SparkKubernetesSensor(
        task_id="monitor_atualizar-freshdesk",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_atualizar_freshdesk')['metadata']['name'] }}",
    )

    dataset_atualizar_freshdesk = EmptyOperator(
        task_id="dataset_atualizar_freshdesk",
        outlets=[AGENTES_SILVER, TICKETS_LANDING, TICKETS_SILVER, TICKETS_GOLD],
    )

atualizar_dados_freshdesk >> monitor_atualizar_freshdesk >> dataset_atualizar_freshdesk
