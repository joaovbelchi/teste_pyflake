from airflow import DAG
from datetime import datetime
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from utils.msteams import msteams_task_failed

from utils.datasets import PONTUACAO_ATIVOS_RUN
from airflow.operators.empty import EmptyOperator


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

## DAG  Crawler
with DAG(
    dag_id="Dag_pontuacao_ativos_run",
    default_args=default_args,
    schedule_interval="0 16 * * 1-5",
    tags=[
        "pythonoperator",
        "dev",
        "dados",
        "one",
        "btg",
        "microsoft",
        "RUN",
        "sugestao_de_realocacao",
        "comerciais",
        "advisors",
    ],
    catchup=False,
    description="Pipeline para atualizar a pontuação dos ativos do RUN",
    max_active_runs=1,
) as dag:
    pontuacao_ativos_run = SparkKubernetesOperator(
        task_id="dag_pontuacao_ativos_run",
        params=dict(
            app_name="one-pontuacao-ativos-run",
            mainApplicationFile="local:///app/one-rf-pontuacao-ativos-run.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_pontuacao_ativos_run = SparkKubernetesSensor(
        task_id="monitor_pontuacao_ativos_run",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_pontuacao_ativos_run')['metadata']['name'] }}",
    )

    dataset_pontuacao_ativos_run = EmptyOperator(
        task_id="dataset_pontuacao_ativos_run", outlets=[PONTUACAO_ATIVOS_RUN]
    )

    # #---------- TASK UPLOAD ONEDRIVE ----------#
    pontuacao_ativos_run_onedrive = SparkKubernetesOperator(
        task_id="dag_pontuacao_ativos_run_onedrive",
        params=dict(
            app_name="one-pontuacao-ativos-run-onedrive",
            mainApplicationFile="local:///app/one-rf-pontuacao-ativos-run-onedrive.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_pontuacao_ativos_run_onedrive = SparkKubernetesSensor(
        task_id="monitor_pontuacao_ativos_run_onedrive",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_pontuacao_ativos_run_onedrive')['metadata']['name'] }}",
    )

    # #---------- TASK AVISOS NOVOS ATIVOS ---------#
    aviso_novos_ativos = SparkKubernetesOperator(
        task_id="dag_aviso_novos_ativos",
        params=dict(
            app_name="one-aviso-novos-ativos",
            mainApplicationFile="local:///app/one-rf-aviso-novos-ativos.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_aviso_novos_ativos = SparkKubernetesSensor(
        task_id="monitor_aviso_novos_ativos",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_aviso_novos_ativos')['metadata']['name'] }}",
    )

(
    pontuacao_ativos_run
    >> monitor_pontuacao_ativos_run
    >> dataset_pontuacao_ativos_run
    >> pontuacao_ativos_run_onedrive
    >> monitor_pontuacao_ativos_run_onedrive
    >> aviso_novos_ativos
    >> monitor_aviso_novos_ativos
)
