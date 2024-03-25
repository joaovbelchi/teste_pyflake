from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.msteams import msteams_task_failed
from utils.DagSeguros.relatorios_de_comissao import relatorios_de_comissao


def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


default_args = {
    "owner": "Nathalia Montandon",
    "email": ["nathalia.montandon@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2023, 12, 22),
    "retries": 0,
}

with DAG(
    "Dag_Seguros",
    default_args=default_args,
    schedule_interval="30 11 * * 1-5",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:
    operator_comissao = PythonOperator(
        task_id="operator_comissao",
        python_callable=relatorios_de_comissao,
        op_kwargs={"drive_id": "{{ var.value.get('guilherme_drive_id') }}"},
    )

    operator_comissao
