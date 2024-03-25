from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from datetime import datetime

from utils.msteams import msteams_task_failed

from utils.bucket_names import get_bucket_name

from utils.functions import (
    verifica_qualidade_landing,
    transferencia_landing_para_bronze,
    parse_ts_dag_run,
)

from utils.datasets import (
    BASE_EMAILS_SILVER,
)

from utils.DagBaseEmails.schemas import SCHEMA_BASE_EMAILS

from utils.DagBaseEmails.functions import (
    requisicao_por_usuario,
    leitura_landing,
    tratamento_base_emails,
)


def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


default_args = {
    "owner": "Nathalia Montandon",
    "email": ["nathalia.montandon@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2024, 2, 27),
    "retries": 0,
    "timeout": 3600,
    "mode": "reschedule",
}

timestamp_dagrun = "{{ parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger) }}"
ano_particao = "{{ parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger, return_datetime=True).strftime('%Y') }}"
mes_particao = "{{ parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger, return_datetime=True).strftime('%m').lstrip('0') }}"

drive_id = "{{ var.value.get('guilherme_drive_id') }}"

s3_bucket_landing = get_bucket_name("lake-landing")
s3_bucket_bronze = get_bucket_name("lake-bronze")
s3_bucket_silver = get_bucket_name("lake-silver")

s3_table_key = "one/api_graph/users/mail/list_messages"
s3_partitions_key = f"ano_particao={ano_particao}/mes_particao={mes_particao}"
s3_filename = f"emails_{timestamp_dagrun}.json"

with DAG(
    "Dag_Base_Emails",
    default_args=default_args,
    schedule_interval="0 0 * * 1-5",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    render_template_as_native_obj=True,
    user_defined_macros={"parse_ts_dag_run": parse_ts_dag_run},
    description="DAG para atualizar o registro de e-mails.",
) as dag:
    operator_requisicao = PythonOperator(
        task_id="requisicao_graph_api",
        python_callable=requisicao_por_usuario,
        op_kwargs={
            "drive_id": drive_id,
            "s3_bucket": s3_bucket_landing,
            "s3_table_key": s3_table_key,
            "s3_partitions_key": s3_partitions_key,
            "s3_bucket_save": s3_bucket_silver,
            "timestamp_dagrun": timestamp_dagrun,
        },
    )

    qualidade_landing = PythonOperator(
        task_id=f"qualidade_landing",
        python_callable=verifica_qualidade_landing,
        op_kwargs={
            "s3_bucket": s3_bucket_landing,
            "s3_table_key": s3_table_key,
            "s3_partitions_key": s3_partitions_key,
            "s3_filename": s3_filename,
            "schema": SCHEMA_BASE_EMAILS,
            "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
            "funcao_leitura_landing": leitura_landing,
            "kwargs_funcao_leitura_landing": {
                "s3_bucket": s3_bucket_landing,
                "s3_table_key": s3_table_key,
                "s3_partitions_key": s3_partitions_key,
                "timestamp_dagrun": timestamp_dagrun,
            },
        },
        on_failure_callback=qualidade_task_failed,
    )

    move_para_bronze = PythonOperator(
        task_id="transfer_bronze",
        python_callable=transferencia_landing_para_bronze,
        op_kwargs={
            "s3_bucket_bronze": s3_bucket_bronze,
            "s3_table_key": s3_table_key,
            "s3_partitions_key": s3_partitions_key,
            "schema": SCHEMA_BASE_EMAILS,
            "data_interval_end": timestamp_dagrun,
            "funcao_leitura_landing": leitura_landing,
            "kwargs_funcao_leitura_landing": {
                "s3_bucket": s3_bucket_landing,
                "s3_table_key": s3_table_key,
                "s3_partitions_key": s3_partitions_key,
                "timestamp_dagrun": timestamp_dagrun,
                "expand_cols": True,
            },
        },
    )

    tratamento_silver = PythonOperator(
        task_id="tratamento_silver",
        python_callable=tratamento_base_emails,
        op_kwargs={
            "s3_bucket_read": s3_bucket_bronze,
            "s3_table_key": s3_table_key,
            "s3_partitions_key": s3_partitions_key,
            "s3_bucket_save": s3_bucket_silver,
            "timestamp_dagrun": timestamp_dagrun,
        },
        outlets=BASE_EMAILS_SILVER,
    )

    operator_requisicao >> qualidade_landing >> move_para_bronze >> tratamento_silver
