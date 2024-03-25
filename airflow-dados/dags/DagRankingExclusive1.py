from airflow import DAG
from datetime import datetime
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from utils.msteams import msteams_task_failed

from utils.datasets import RANKING_EXCLUSIVE1
from airflow.operators.empty import EmptyOperator


def failure_callback_func(context):
    msteams_task_failed(context)


default_args = {
    "owner": "João Pedro Coelho",
    "start_date": datetime(2023, 10, 2, 15, 45),
    "on_failure_callback": failure_callback_func,
    "email": ["joaopedro.coelho@investimentos.one"],
    "retries": 0,
    "kubernetes_conn_id": "con_eks_dados",
    "namespace": "processing",
}

with DAG(
    dag_id="Dag_Ranking_Exclusive1",
    default_args=default_args,
    schedule_interval="45 15 * * 1",
    tags=[
        "pythonoperator",
        "dev",
        "dados",
        "one",
        "growth",
        "ranking",
        "exclusive1",
        "email",
        "gold",
    ],
    catchup=False,
    description="Pipeline para atualizar Pontuação Semanal dos Exclusive 1 e gerar Ranking",
    max_active_runs=1,
) as dag:
    # --------- Task para atualizar base do Ranking Exclusive 1 ---------#
    ranking_exclusive1 = SparkKubernetesOperator(
        task_id="submit_ranking_exclusive1",
        params=dict(
            app_name="one-growth-ranking-exclusive1",
            mainApplicationFile="local:///app/one-growth-ranking-exclusive1.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_ranking_exclusive1 = SparkKubernetesSensor(
        task_id="monitor_ranking_exclusive1",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='submit_ranking_exclusive1')['metadata']['name'] }}",
    )

    dataset_ranking_exclusive1 = EmptyOperator(
        task_id="dataset_ranking_exclusive1",
        outlets=[RANKING_EXCLUSIVE1],
    )

    # --------- Task para enviar e-mail com o Ranking Exclusive 1 ---------#
    envio_ranking_exclusive1 = SparkKubernetesOperator(
        task_id="submit_envio_ranking_exclusive1",
        params=dict(
            app_name="one-growth-ranking-exclusive1-email",
            mainApplicationFile="local:///app/one-growth-ranking-exclusive1-email.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_envio_ranking_exclusive1 = SparkKubernetesSensor(
        task_id="monitor_envio_ranking_exclusive1",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='submit_envio_ranking_exclusive1')['metadata']['name'] }}",
    )

    ranking_exclusive1 >> monitor_ranking_exclusive1 >> dataset_ranking_exclusive1
    (
        dataset_ranking_exclusive1
        >> envio_ranking_exclusive1
        >> monitor_envio_ranking_exclusive1
    )
