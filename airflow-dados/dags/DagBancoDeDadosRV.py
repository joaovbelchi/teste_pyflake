from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

from utils.msteams import msteams_task_failed
from utils.datasets import RECEITA_ESTIMADA

from utils.DagBancoDeDadosRV.functions import banco_de_dados_rv


def failure_callback_func(context):
    msteams_task_failed(context)


default_args = {
    "owner": "Nathalia Montandon",
    "email": ["nathalia.montandon@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2024, 2, 16),
    "retries": 0,
}

with DAG(
    dag_id="Dag_Banco_De_Dados_RV",
    default_args=default_args,
    schedule=[RECEITA_ESTIMADA],
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    description="DAG para atualizar banco de dados RV",
    render_template_as_native_obj=True,
) as dag:
    operator_banco_de_dados_rv = PythonOperator(
        task_id="operator_banco_de_dados_rv",
        python_callable=banco_de_dados_rv,
    )

    operator_banco_de_dados_rv
