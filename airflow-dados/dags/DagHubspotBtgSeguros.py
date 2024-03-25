from airflow import DAG
from datetime import datetime
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.operators.empty import EmptyOperator

from utils.msteams import msteams_task_failed
from utils.datasets import SEGUROS_GOLD


def failure_callback_func(context):
    msteams_task_failed(context)


default_args = {
    "owner": "Eduardo Queiroz",
    "start_date": datetime(2023, 5, 16),
    "email": ["eduardo.queiroz@investimentos.one"],
    "on_failure_callback": failure_callback_func,
    "retries": 0,
    "kubernetes_conn_id": "con_eks_dados",
    "namespace": "processing",
}

with DAG(
    dag_id="Dag_Hubspot_Btg_Seguros",
    default_args=default_args,
    schedule=[SEGUROS_GOLD],
    tags=[
        "pythonoperator",
        "dev",
        "dados",
        "one",
        "btg",
        "hubspot",
        "onedrive",
        "Seguros",
    ],
    catchup=False,
    description="Pipeline para criar o arquivo planilha_seguros, com dados do hubspot e btg referente a área de seguros, e salvar estas informações via API Graph.",
) as dag:
    dag_salva_planilha_seguros = SparkKubernetesOperator(
        task_id="dag_hubspot_btg_seguros",
        params=dict(
            app_name="one-hubspot-btg-planilha-seguros",
            mainApplicationFile="local:///app/one-hubspot-btg-planilha-seguros.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_salva_planilha_seguros = SparkKubernetesSensor(
        task_id="monitor_dag_hubspot_btg_seguros",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_hubspot_btg_seguros')['metadata']['name'] }}",
    )

dag_salva_planilha_seguros >> monitor_salva_planilha_seguros
