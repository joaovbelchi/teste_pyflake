from airflow import DAG
from datetime import datetime
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from utils.msteams import msteams_task_failed
from utils.functions import trigger_webhook_btg, verifica_objeto_s3
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from utils.bucket_names import get_bucket_name


def failure_callback_func(context):
    msteams_task_failed(context)


default_args = {
    "owner": "Nathalia Montandon",
    "start_date": datetime(2023, 9, 18),
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
    dag_id="Dag_Rentabilidade",
    default_args=default_args,
    schedule_interval="15 14 * * 1-5",
    tags=["sparkoperator", "dev", "dados", "one", "btg", "s3", "Rentabilidade"],
    catchup=False,
    description="Pipeline para salvar a base de rentabilidade no s3",
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:
    task_trigger_webhook = PythonOperator(
        task_id=f"trigger_webhook_rentabilidade",
        python_callable=trigger_webhook_btg,
        op_kwargs={
            "endpoint": "https://api.btgpactual.com/api-partner-report-hub/api/v1/report/profitability"
        },
    )

    sensor_aguarda_response = PythonSensor(
        task_id=f"aguarda_response_rentabilidade",
        python_callable=verifica_objeto_s3,
        op_kwargs={
            "bucket": get_bucket_name("lake-landing"),
            "object_key": "btg/webhooks/responses/rentabilidade.json",
            "min_last_modified": "{{ data_interval_end }}",
        },
    )

    rentabilidade = SparkKubernetesOperator(
        task_id="dag_rentabilidade",
        params=dict(
            app_name="one-operacional-rentabilidade",
            mainApplicationFile="local:///app/one-operacional-rentabilidade-detalhada.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_rentabilidade = SparkKubernetesSensor(
        task_id="monitor_rentabilidade",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_rentabilidade')['metadata']['name'] }}",
    )

(
    task_trigger_webhook
    >> sensor_aguarda_response
    >> rentabilidade
    >> monitor_rentabilidade
)
