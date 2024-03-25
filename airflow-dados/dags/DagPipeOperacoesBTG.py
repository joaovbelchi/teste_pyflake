from airflow import DAG
from datetime import datetime
from utils.msteams import msteams_task_failed
from utils.functions import trigger_webhook_btg, verifica_objeto_s3
from utils.DagPipeOperacoesBTG.functions import salva_dados_webhook_landing
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from utils.bucket_names import get_bucket_name
from airflow.models import Variable


def failure_callback_func(context):
    msteams_task_failed(context)


def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


default_args = {
    "owner": "Gustavo Sousa",
    "start_date": datetime(2024, 3, 4),
    "email": ["gustavo.sousa@investimentos.one"],
    "on_failure_callback": failure_callback_func,
    "retries": 0,
}

with DAG(
    dag_id="Dag_Pipeline_Operacoes",
    default_args=default_args,
    schedule_interval="0,10,20,30,40,50 11-22 * * 1-5",
    tags=["operations", "operacoes", "monitoramento", "asset_allocation"],
    catchup=False,
    description="Pipeline para salvar dados de operações de clientes com maior frequência",
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:
    task_trigger_webhook_pre_operation = PythonOperator(
        task_id="task_trigger_webhook_pre_operation",
        python_callable=trigger_webhook_btg,
        op_kwargs={
            "endpoint": "https://api.btgpactual.com/api-pre-operation/api/v1/pre-operation"
        },
    )

    trigger_webhook_operations_search = PythonOperator(
        task_id="trigger_webhook_operations_search",
        python_callable=trigger_webhook_btg,
        op_kwargs={
            "endpoint": "https://api.btgpactual.com/api-operations-search/api/v1/operations-search"
        },
    )

    sensor_aguarda_response_operations_search = PythonSensor(
        task_id="aguarda_response_operacoes",
        python_callable=verifica_objeto_s3,
        op_kwargs={
            "bucket": get_bucket_name("lake-landing"),
            "object_key": "btg/webhooks/responses/operacoes.json",
            "min_last_modified": "{{ data_interval_end }}",
        },
        on_failure_callback=qualidade_task_failed,
    )

    sensor_aguarda_response_pre_operation = PythonSensor(
        task_id=f"aguarda_response_pre_operacoes",
        python_callable=verifica_objeto_s3,
        op_kwargs={
            "bucket": get_bucket_name("lake-landing"),
            "object_key": "btg/webhooks/responses/pre_operacoes.json",
            "min_last_modified": "{{ data_interval_end }}",
        },
        on_failure_callback=qualidade_task_failed,
    )

    task_salva_pre_operacoes_landing = PythonOperator(
        task_id="task_salva_pre_operacoes_landing",
        python_callable=salva_dados_webhook_landing,
        op_kwargs={"webhook": "pre-operation"},
    )

    task_salva_operacoes_landing = PythonOperator(
        task_id="task_salva_operacoes_landing",
        python_callable=salva_dados_webhook_landing,
        op_kwargs={"webhook": "operations-search"},
    )

(
    task_trigger_webhook_pre_operation
    >> sensor_aguarda_response_operations_search
    >> task_salva_pre_operacoes_landing
)

(
    trigger_webhook_operations_search
    >> sensor_aguarda_response_pre_operation
    >> task_salva_operacoes_landing
)
