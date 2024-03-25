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
    "owner": "Nathalia Montandon",
    "start_date": datetime(2023, 10, 25),
    "email": ["nathalia.montandon@investimentos.one"],
    "on_failure_callback": failure_callback_func,
    "retries": 0,
    "kubernetes_conn_id": "con_eks_dados",
    "namespace": "processing",
}

with DAG(
    dag_id="Dag_Taxas_Diarias_Corporate",
    default_args=default_args,
    schedule_interval="0 15 * * 1-5",
    tags=[
        "pythonoperator",
        "dev",
        "dados",
        "one",
        "btg",
        "email",
        "microsoft",
        "corporate",
        "taxas_diarias",
        "credito_bancario",
    ],
    catchup=False,
    description="Pipeline para envio do e-mail de taxas diarias para clientes do corporate",
    max_active_runs=1,
) as dag:
    taxas_diarias_corporate = SparkKubernetesOperator(
        task_id="dag_taxas_diarias_corporate",
        params=dict(
            app_name="one-taxas-diarias-corporate",
            mainApplicationFile="local:///app/one-corporate-envio-taxas-diarias.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_taxas_diarias_corporate = SparkKubernetesSensor(
        task_id="monitor_taxas_diarias_corporate",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_taxas_diarias_corporate')['metadata']['name'] }}",
    )

taxas_diarias_corporate >> monitor_taxas_diarias_corporate
