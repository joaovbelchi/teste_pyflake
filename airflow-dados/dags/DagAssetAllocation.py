from airflow import DAG
from datetime import datetime
from utils.msteams import msteams_task_failed
from utils.datasets import (
    ASSET_ALLOCATION_HISTORICO,
    ASSET_ALLOCATION_MOVIMENTACAO,
    ASSET_ALLOCATION_METRICA,
)
from airflow.operators.python import PythonOperator
from utils.DagAssetAllocation.functions import incremental, movimentacao, metrica
from utils.executor_configs import k8s_executor_config_override


def failure_callback_func(context):
    msteams_task_failed(context)


drive_id = "{{ var.value.get('guilherme_drive_id') }}"

default_args = {
    "owner": "Nathalia Montandon",
    "start_date": datetime(2023, 10, 25),
    "email": ["nathalia.montandon@investimentos.one"],
    "on_failure_callback": failure_callback_func,
    "retries": 0,
}

with DAG(
    dag_id="Dag_Asset_Allocation",
    default_args=default_args,
    schedule_interval="30 14 * * 1-5",
    tags=[
        "pythonoperator",
        "dev",
        "dados",
        "one",
        "asset_allocation",
        "movimentacao",
        "microsoft",
        "gold",
    ],
    catchup=False,
    description="Pipeline para atualizar Asset Allocation relacionando com Movimentações",
    max_active_runs=1,
) as dag:
    # Task para atualizar a base de receita estimada
    task_incremental = PythonOperator(
        task_id="incremental",
        python_callable=incremental,
        op_kwargs={"drive_id": drive_id},
        outlets=[ASSET_ALLOCATION_HISTORICO],
        executor_config=k8s_executor_config_override(cpu=1, memory=6000),
    )

    # Task para atualizar o banco de dados de renda variável
    task_movimentacao = PythonOperator(
        task_id="movimentacao",
        python_callable=movimentacao,
        op_kwargs={"drive_id": drive_id},
        outlets=[ASSET_ALLOCATION_MOVIMENTACAO],
        executor_config=k8s_executor_config_override(cpu=1, memory=8000),
    )

    # Task para atualizar o banco de dados de renda variável
    task_metrica = PythonOperator(
        task_id="metrica",
        python_callable=metrica,
        op_kwargs={"drive_id": drive_id},
        outlets=[ASSET_ALLOCATION_METRICA],
        executor_config=k8s_executor_config_override(cpu=1, memory=14000),
    )

    task_incremental >> task_movimentacao >> task_metrica
