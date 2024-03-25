from airflow import DAG
from datetime import datetime
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from utils.msteams import msteams_task_failed

from utils.datasets import PRE_OPERACOES, OPERACOES_WEBHOOK
from airflow.operators.empty import EmptyOperator
from utils.functions import trigger_webhook_btg, verifica_objeto_s3
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.python import PythonSensor
from utils.bucket_names import get_bucket_name
from airflow.models import Variable


def failure_callback_func(context):
    msteams_task_failed(context)


def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


default_args = {
    "owner": "Nathalia Montandon",
    "start_date": datetime(2022, 5, 17),
    "email": ["nathalia.montandon@investimentos.one"],
    "on_failure_callback": failure_callback_func,
    "kubernetes_conn_id": "con_eks_dados",
    "namespace": "processing",
    "retries": 0,
    "timeout": 360,
    "mode": "poke",
    "poke_interval": 20,
}

with DAG(
    dag_id="Dag_Alerta_Bloqueio_Judicial",
    default_args=default_args,
    schedule_interval="0,10,20,30,40,50 11-22 * * 1-5",
    tags=[
        "sparkoperator",
        "dev",
        "dados",
        "one",
        "btg",
        "microsoft",
        "bloqueio_judicial",
        "alerta",
        "email",
    ],
    catchup=False,
    description="Pipeline para alertar em caso de possível resgate de ativos com bloquieo judicial",
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:
    branch_trigger_webhook = BranchPythonOperator(
        task_id="branch_decide_webhook",
        python_callable=lambda x: (
            "trigger_webhook_pre_operacoes"
            if int(x) in [0, 30]
            else "trigger_webhook_operacoes"
        ),
        op_args=["{{ data_interval_end.minute }}"],
    )

    task_trigger_webhook_pre_operacoes = PythonOperator(
        task_id=f"trigger_webhook_pre_operacoes",
        python_callable=trigger_webhook_btg,
        op_kwargs={
            "endpoint": "https://api.btgpactual.com/api-pre-operation/api/v1/pre-operation"
        },
    )

    task_trigger_webhook_operacoes = PythonOperator(
        task_id=f"trigger_webhook_operacoes",
        python_callable=trigger_webhook_btg,
        op_kwargs={
            "endpoint": "https://api.btgpactual.com/api-operations-search/api/v1/operations-search"
        },
    )

    sensor_aguarda_response_operacoes = PythonSensor(
        task_id=f"aguarda_response_operacoes",
        python_callable=verifica_objeto_s3,
        op_kwargs={
            "bucket": get_bucket_name("lake-landing"),
            "object_key": "btg/webhooks/responses/operacoes.json",
            "min_last_modified": "{{ data_interval_end }}",
        },
        on_failure_callback=qualidade_task_failed,
    )

    sensor_aguarda_response_pre_operacoes = PythonSensor(
        task_id=f"aguarda_response_pre_operacoes",
        python_callable=verifica_objeto_s3,
        op_kwargs={
            "bucket": get_bucket_name("lake-landing"),
            "object_key": "btg/webhooks/responses/pre_operacoes.json",
            "min_last_modified": "{{ data_interval_end }}",
        },
        on_failure_callback=qualidade_task_failed,
    )

    email_bloqueio_judicial = SparkKubernetesOperator(
        task_id="dag_email_bloqueio_judicial",
        params=dict(
            app_name="one-corporate-alerta-bloqueio-judicial",
            mainApplicationFile="local:///app/one-corporate-alerta-bloqueio-judicial.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
        trigger_rule="none_failed_min_one_success",
    )

    monitor_email_bloqueio_judicial = SparkKubernetesSensor(
        task_id="monitor_email_bloqueio_judicial",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_email_bloqueio_judicial')['metadata']['name'] }}",
    )

    dataset_email_bloqueio_judicial = EmptyOperator(
        task_id="dataset_email_bloqueio_judicial",
        outlets=[PRE_OPERACOES, OPERACOES_WEBHOOK],
    )

(
    branch_trigger_webhook
    >> task_trigger_webhook_pre_operacoes
    >> sensor_aguarda_response_pre_operacoes
    >> email_bloqueio_judicial
)
(
    branch_trigger_webhook
    >> task_trigger_webhook_operacoes
    >> sensor_aguarda_response_operacoes
    >> email_bloqueio_judicial
)
(
    email_bloqueio_judicial
    >> monitor_email_bloqueio_judicial
    >> dataset_email_bloqueio_judicial
)
