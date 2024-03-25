from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.msteams import msteams_refresh_pbi, msteams_task_failed
from datetime import datetime

default_args = {
    "owner": "Felipe Nogueira",
    "start_date": datetime(2023, 9, 15),
    "email": ["felipe.nogueira@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "retries": 0,
}

dag = DAG(
    "Dag_Refresh_PBI_Growth_TeamLeaders",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

check_time_task = PythonOperator(
    task_id="envia_mensagem_msteams",
    python_callable=msteams_refresh_pbi,
    op_kwargs={"webhook": "{{ var.value.get('msteams_webhook_pbi_TL') }}"},
    dag=dag,
)
