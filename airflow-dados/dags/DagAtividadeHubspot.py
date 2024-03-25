from airflow import DAG
from datetime import datetime
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from utils.msteams import msteams_task_failed


def failure_callback_func(context):
    msteams_task_failed(context)


default_args = {
    "owner": "Fred Lamas",
    "start_date": datetime(2023, 10, 25),
    "email": ["frederick.lamas@investimentos.one"],
    "on_failure_callback": failure_callback_func,
    "retries": 0,
    "kubernetes_conn_id": "con_eks_dados",
    "namespace": "processing",
}

with DAG(
    dag_id="Dag_Atividades_Hubspot",
    default_args=default_args,
    schedule_interval="30 15 * * 1-5",
    tags=[
        "pythonoperator",
        "dev",
        "dados",
        "one",
        "btg",
        "microsoft",
        "onedrive",
        "hubspot",
        "tarefas",
        "nps",
        "vencimentos",
        "resgate fundos",
        "negativado",
        "dividendos",
    ],
    catchup=False,
    description="Script que envia tarefas ao hubspot de resgate, vencimentos, nps, dividendos, negativados",
) as dag:
    atividades_hubspot = SparkKubernetesOperator(
        task_id="dag_atividades_hubspot",
        params=dict(
            app_name="spark-atividade-hubspot",
            mainApplicationFile="local:///app/one-atividades-hubspot.py",
        ),
        application_file="ConfigSpark6G.yaml",
        do_xcom_push=True,
    )

    monitor_atividades_hubspot = SparkKubernetesSensor(
        task_id="monitor_dag_atividades_hubspot",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_atividades_hubspot')['metadata']['name'] }}",
    )

atividades_hubspot >> monitor_atividades_hubspot
