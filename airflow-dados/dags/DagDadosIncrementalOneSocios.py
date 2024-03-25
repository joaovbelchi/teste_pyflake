from airflow import DAG
from datetime import datetime
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from utils.msteams import msteams_task_failed

from utils.datasets import SOCIOS_LANDING, SOCIO_SILVER, N_ASSESSORES, SOCIO_GOLD
from airflow.operators.empty import EmptyOperator


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

## DAG  Crawler
with DAG(
    dag_id="Dag_Carga_Incremental_One_Socios",
    default_args=default_args,
    schedule_interval="0 3,15 * * 1-5",
    tags=["pythonoperator", "dev", "dados", "one", "btg", "socios"],
    catchup=False,
    description="Pipeline para coleta e atualização da base de socios",
    # max_active_runs=1
) as dag:
    # ---------- TASK SOCIOS LANDING ----------#
    socios_landing = SparkKubernetesOperator(
        task_id="dag_landing_socios",
        params=dict(
            app_name="one-incremental-landing-socios",
            mainApplicationFile="local:///app/one-incremental-one-landing-socios.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_socios_landing = SparkKubernetesSensor(
        task_id="monitor_landing_socios",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_landing_socios')['metadata']['name'] }}",
    )

    dataset_socios_landing = EmptyOperator(
        task_id="dataset_socios_landing", outlets=[SOCIOS_LANDING]
    )

    # ---------- TASK SOCIOS SILVER ----------#
    socios_silver = SparkKubernetesOperator(
        task_id="dag_silver_socios",
        params=dict(
            app_name="one-incremental-silver-socios",
            mainApplicationFile="local:///app/one-incremental-one-silver-socios.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_socios_silver = SparkKubernetesSensor(
        task_id="monitor_silver_socios",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_silver_socios')['metadata']['name'] }}",
    )

    dataset_socios_silver = EmptyOperator(
        task_id="dataset_socios_silver", outlets=[SOCIO_SILVER]
    )

    # ---------- TASK SOCIOS GOLD ----------#
    socios_gold = SparkKubernetesOperator(
        task_id="dag_gold_socios",
        params=dict(
            app_name="one-incremental-gold-socios",
            mainApplicationFile="local:///app/one-incremental-one-gold-socios.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_socios_gold = SparkKubernetesSensor(
        task_id="monitor_gold_socios",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_gold_socios')['metadata']['name'] }}",
    )

    dataset_socios_gold = EmptyOperator(
        task_id="dataset_socios_gold", outlets=[N_ASSESSORES, SOCIO_GOLD]
    )

    # ---------- TASK SOCIOS GOLD UPLOAD ONEDRIVE ----------#
    socios_gold_upload_onedrive = SparkKubernetesOperator(
        task_id="dag_gold_socios_upload_onedrive",
        params=dict(
            app_name="one-gold-socios-upload-onedrive",
            mainApplicationFile="local:///app/one-gold-socios-upload-onedrive.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_socios_gold_upload_onedrive = SparkKubernetesSensor(
        task_id="monitor_gold_socios_upload_onedrive",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_gold_socios_upload_onedrive')['metadata']['name'] }}",
    )


socios_landing >> monitor_socios_landing >> dataset_socios_landing >> socios_silver
socios_silver >> monitor_socios_silver >> dataset_socios_silver >> socios_gold
socios_gold >> monitor_socios_gold >> dataset_socios_gold >> socios_gold_upload_onedrive
socios_gold_upload_onedrive >> monitor_socios_gold_upload_onedrive
