from airflow import DAG
from datetime import datetime
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from utils.msteams import msteams_task_failed

from utils.datasets import (
    BASE_NPS_LANDING,
    BASE_INDICACOES_LANDING,
    BASE_ABORDAGENS_LANDING,
    BASE_NPS_SILVER,
    BASE_INDICACOES_SILVER,
    BASE_ABORDAGENS_SILVER,
    BASE_NPS_GOLD,
    BASE_INDICACOES_GOLD,
    BASE_ABORDAGENS_GOLD,
)
from airflow.operators.empty import EmptyOperator


def failure_callback_func(context):
    msteams_task_failed(context)


default_args = {
    "owner": "Fred Lamas",
    "start_date": datetime(2023, 8, 16),
    "email": ["frederick.lamas@investimentos.one"],
    "on_failure_callback": failure_callback_func,
    "retries": 0,
    "kubernetes_conn_id": "con_eks_dados",
    "namespace": "processing",
}

with DAG(
    dag_id="Dag_Nps_Full",
    default_args=default_args,
    schedule_interval="30 13 * * 1-5",
    tags=[
        "pythonoperator",
        "dev",
        "dados",
        "one",
        "btg",
        "nps",
        "microsoft",
        "onedrive",
        "Landing",
        "Silver",
        "Gold",
    ],
    catchup=False,
    description="Script que envia o arquivo Base_Nps, Base_Nps_indicaçoes, Base_Nps_abordagens do OneDrive para um bucket do s3 na landing, além de salvar na silver e na gold tratado",
) as dag:
    # -------TASK NPS LANDING-------------#
    nps_landing = SparkKubernetesOperator(
        task_id="dag_nps_landing",
        params=dict(
            app_name="spark-one-nps-landing",
            mainApplicationFile="local:///app/one-s3-btg-nps-landing.py",
        ),
        application_file="ConfigSpark4G.yaml",
        do_xcom_push=True,
    )

    monitor_nps_landing = (
        SparkKubernetesSensor(
            task_id="monitor_dag_nps_landing",
            attach_log=True,
            application_name="{{ task_instance.xcom_pull(task_ids='dag_nps_landing')['metadata']['name'] }}",
        ),
    )

    dataset_nps_landing = EmptyOperator(
        task_id="dataset_nps_landing", outlets=[BASE_NPS_LANDING]
    )

    # -------TASK NPS_INDICAÇOES LANDING-------------#
    nps_indicacoes_landing = SparkKubernetesOperator(
        task_id="dag_nps_indicacoes_landing",
        params=dict(
            app_name="spark-one-nps-indicacoes-landing",
            mainApplicationFile="local:///app/one-s3-btg-nps-indicacoes-landing.py",
        ),
        application_file="ConfigSpark4G.yaml",
        do_xcom_push=True,
    )

    monitor_nps_indicacoes_landing = (
        SparkKubernetesSensor(
            task_id="monitor_dag_nps_indicacoes_landing",
            attach_log=True,
            application_name="{{ task_instance.xcom_pull(task_ids='dag_nps_indicacoes_landing')['metadata']['name'] }}",
        ),
    )

    dataset_nps_indicacoes_landing = EmptyOperator(
        task_id="dataset_nps_indicacoes_landing", outlets=[BASE_INDICACOES_LANDING]
    )

    # -------TASK NPS_ABORDAGENS LANDING-------------#
    nps_abordagens_landing = SparkKubernetesOperator(
        task_id="dag_nps_abordagens_landing",
        params=dict(
            app_name="spark-one-nps-abordagens-landing",
            mainApplicationFile="local:///app/one-s3-btg-nps-abordagens-landing.py",
        ),
        application_file="ConfigSpark4G.yaml",
        do_xcom_push=True,
    )

    monitor_nps_abordagens_landing = (
        SparkKubernetesSensor(
            task_id="monitor_dag_nps_abordagens_landing",
            attach_log=True,
            application_name="{{ task_instance.xcom_pull(task_ids='dag_nps_abordagens_landing')['metadata']['name'] }}",
        ),
    )

    dataset_nps_abordagens_landing = EmptyOperator(
        task_id="dataset_nps_abordagens_landing", outlets=[BASE_ABORDAGENS_LANDING]
    )

    # -------TASK NPS SILVER-------------#
    nps_silver = SparkKubernetesOperator(
        task_id="dag_nps_silver",
        params=dict(
            app_name="spark-one-nps-silver",
            mainApplicationFile="local:///app/one-s3-btg-nps-silver.py",
        ),
        application_file="ConfigSpark4G.yaml",
        do_xcom_push=True,
    )

    monitor_nps_silver = (
        SparkKubernetesSensor(
            task_id="monitor_dag_nps_silver",
            attach_log=True,
            application_name="{{ task_instance.xcom_pull(task_ids='dag_nps_silver')['metadata']['name'] }}",
        ),
    )

    dataset_nps_silver = EmptyOperator(
        task_id="dataset_nps_silver", outlets=[BASE_NPS_SILVER]
    )

    # -------TASK NPS_INDICAÇOES SILVER-------------#
    nps_indicacoes_silver = SparkKubernetesOperator(
        task_id="dag_nps_indicacoes_silver",
        params=dict(
            app_name="spark-one-nps-indicacoes-silver",
            mainApplicationFile="local:///app/one-s3-btg-nps-indicacoes-silver.py",
        ),
        application_file="ConfigSpark4G.yaml",
        do_xcom_push=True,
    )

    monitor_nps_indicacoes_silver = (
        SparkKubernetesSensor(
            task_id="monitor_dag_nps_indicacoes_silver",
            attach_log=True,
            application_name="{{ task_instance.xcom_pull(task_ids='dag_nps_indicacoes_silver')['metadata']['name'] }}",
        ),
    )

    dataset_nps_indicacoes_silver = EmptyOperator(
        task_id="dataset_nps_indicacoes_silver", outlets=[BASE_INDICACOES_SILVER]
    )

    # -------TASK NPS_ABORDAGENS SILVER-------------#
    nps_abordagens_silver = SparkKubernetesOperator(
        task_id="dag_nps_abordagens_silver",
        params=dict(
            app_name="spark-one-nps-abordagens-silver",
            mainApplicationFile="local:///app/one-s3-btg-nps-abordagens-silver.py",
        ),
        application_file="ConfigSpark4G.yaml",
        do_xcom_push=True,
    )

    monitor_nps_abordagens_silver = (
        SparkKubernetesSensor(
            task_id="monitor_dag_nps_abordagens_silver",
            attach_log=True,
            application_name="{{ task_instance.xcom_pull(task_ids='dag_nps_abordagens_silver')['metadata']['name'] }}",
        ),
    )

    dataset_nps_abordagens_silver = EmptyOperator(
        task_id="dataset_nps_abordagens_silver", outlets=[BASE_ABORDAGENS_SILVER]
    )

    # -------TASK NPS GOLD-------------#
    nps_gold = SparkKubernetesOperator(
        task_id="dag_nps_gold",
        params=dict(
            app_name="spark-one-nps-gold",
            mainApplicationFile="local:///app/one-s3-btg-nps-gold.py",
        ),
        application_file="ConfigSpark6G.yaml",
        do_xcom_push=True,
    )

    monitor_nps_gold = (
        SparkKubernetesSensor(
            task_id="monitor_dag_nps_gold",
            attach_log=True,
            application_name="{{ task_instance.xcom_pull(task_ids='dag_nps_gold')['metadata']['name'] }}",
        ),
    )

    dataset_nps_gold = EmptyOperator(
        task_id="dataset_nps_gold", outlets=[BASE_NPS_GOLD]
    )

    # -------TASK NPS_INDICAÇOES GOLD-------------#
    nps_indicacoes_gold = SparkKubernetesOperator(
        task_id="dag_nps_indicacoes_gold",
        params=dict(
            app_name="spark-one-nps-indicacoes-gold",
            mainApplicationFile="local:///app/one-s3-btg-nps-indicacoes-gold.py",
        ),
        application_file="ConfigSpark6G.yaml",
        do_xcom_push=True,
    )

    monitor_nps_indicacoes_gold = (
        SparkKubernetesSensor(
            task_id="monitor_dag_nps_indicacoes_gold",
            attach_log=True,
            application_name="{{ task_instance.xcom_pull(task_ids='dag_nps_indicacoes_gold')['metadata']['name'] }}",
        ),
    )

    dataset_nps_indicacoes_gold = EmptyOperator(
        task_id="dataset_nps_indicacoes_gold", outlets=[BASE_INDICACOES_GOLD]
    )

    # -------TASK NPS_ABORDAGENS GOLD-------------#
    nps_abordagens_gold = SparkKubernetesOperator(
        task_id="dag_nps_abordagens_gold",
        params=dict(
            app_name="spark-one-nps-abordagens-gold",
            mainApplicationFile="local:///app/one-s3-btg-nps-abordagens-gold.py",
        ),
        application_file="ConfigSpark6G.yaml",
        do_xcom_push=True,
    )

    monitor_nps_abordagens_gold = SparkKubernetesSensor(
        task_id="monitor_dag_nps_abordagens_gold",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_nps_abordagens_gold')['metadata']['name'] }}",
    )

    dataset_nps_abordagens_gold = EmptyOperator(
        task_id="dataset_nps_abordagens_gold", outlets=[BASE_ABORDAGENS_GOLD]
    )
(
    nps_landing
    >> monitor_nps_landing
    >> dataset_nps_landing
    >> nps_silver
    >> monitor_nps_silver
    >> dataset_nps_silver
    >> nps_gold
    >> monitor_nps_gold
    >> dataset_nps_gold
)
(
    nps_indicacoes_landing
    >> monitor_nps_indicacoes_landing
    >> dataset_nps_indicacoes_landing
    >> nps_indicacoes_silver
    >> monitor_nps_indicacoes_silver
    >> dataset_nps_indicacoes_silver
    >> nps_indicacoes_gold
    >> monitor_nps_indicacoes_gold
    >> dataset_nps_indicacoes_gold
)
(
    nps_abordagens_landing
    >> monitor_nps_abordagens_landing
    >> dataset_nps_abordagens_landing
    >> nps_abordagens_silver
    >> monitor_nps_abordagens_silver
    >> dataset_nps_abordagens_silver
    >> nps_abordagens_gold
    >> monitor_nps_abordagens_gold
    >> dataset_nps_abordagens_gold
)
