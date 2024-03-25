from utils.msteams import msteams_task_failed
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.python import PythonSensor

from utils.DagRoaOne.roa import define_parametros_roa_realizado_ou_estimado
from utils.functions import verifica_objeto_s3
from utils.bucket_names import get_bucket_name


default_args = {
    "owner": "Fred Lamas",
    "email": ["frederick.lamas@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2024, 3, 4),
    "retries": 0,
}


data_interval_end = "{{ data_interval_end.strftime('%Y%m%dT%H%M%S') }}"
last_hour = (
    "{{ data_interval_end - macros.dateutil.relativedelta.relativedelta(hours=1)}}"
)

with DAG(
    "dag_roa_one",
    default_args=default_args,
    schedule_interval="15 14,21 * * 1-5",
    catchup=False,
    description="Pipeline que cria uma base para calculo do ROA a partir do auc dia dia e receita",
    max_active_runs=1,
    max_active_tasks=1,
    render_template_as_native_obj=True,
) as dag:

    branch_roa = BranchPythonOperator(
        task_id="branch_decide_roa",
        python_callable=lambda x: (
            "aguarda_receita_estimada" if int(x) in [14, 15] else "atualizacao_base_roa"
        ),
        op_args=["{{ data_interval_end.hour }}"],
    )

    sensor_receita_estimada = PythonSensor(
        task_id=f"aguarda_receita_estimada",
        python_callable=verifica_objeto_s3,
        timeout=3600,
        soft_fail=True,
        op_kwargs={
            "bucket": get_bucket_name("lake-gold"),
            "object_key": "one/receita/estimativa/receita_estimada.parquet",
            "min_last_modified": last_hour,
        },
    )

    task_roa_estimado = PythonOperator(
        task_id="atualizacao_roa_estimado",
        python_callable=define_parametros_roa_realizado_ou_estimado,
        op_kwargs={
            "tipo": "estimado",
            "timestamp_dagrun": data_interval_end,
        },
    )

    task_base_roa = PythonOperator(
        task_id="atualizacao_base_roa",
        python_callable=define_parametros_roa_realizado_ou_estimado,
        op_kwargs={
            "tipo": "realizado",
            "timestamp_dagrun": data_interval_end,
        },
    )

(branch_roa >> sensor_receita_estimada >> task_roa_estimado)
(branch_roa >> task_base_roa)
