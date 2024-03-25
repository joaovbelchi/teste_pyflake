from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from datetime import datetime

from utils.msteams import msteams_task_failed
from utils.DagPosicaoTopPicks.atualizar_posicao_top_picks import (
    atualizar_posicao_top_picks,
)


default_args = {
    "owner": "Nathalia Montandon",
    "email": ["nathalia.montandon@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2023, 11, 10),
    "retries": 0,
}

with DAG(
    "Dag_Posicao_Top_Picks",
    default_args=default_args,
    schedule_interval="30 23 * * 5",
    catchup=False,
    max_active_runs=1,
) as dag:
    operator_atualizar_posicoes = PythonOperator(
        task_id="operator_atualizar_posicoes",
        python_callable=atualizar_posicao_top_picks,
        op_kwargs={"drive_id": "{{ var.value.get('guilherme_drive_id') }}"},
    )

    operator_atualizar_posicoes
