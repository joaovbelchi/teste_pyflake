from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from airflow.operators.python import PythonOperator
from utils.functions import (
    verifica_qualidade_landing,
    transferencia_landing_para_bronze,
)
from utils.DagExtracaoMetadadosAirflow.functions import (
    leitura_rest_api_airflow,
    extracao_metadados_event_logs,
    parse_task_execution_logs,
    escrita_gold,
    extracao_metadados_generico,
)
from utils.DagExtracaoMetadadosAirflow.schemas import (
    SCHEMA_AIRFLOW_TASK_INSTANCES,
    SCHEMA_AIRFLOW_DAG_RUNS,
    SCHEMA_AIRFLOW_DAGS,
    SCHEMA_AIRFLOW_EVENT_LOGS,
    SCHEMA_AIRFLOW_TASK_EXECUTIONS,
)
from utils.msteams import msteams_task_failed
from utils.bucket_names import get_bucket_name
import pandas as pd


def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


default_args = {
    "owner": "Felipe Nogueira",
    "email": ["felipe.nogueira@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2023, 11, 8),
    "retries": 0,
}

folder_prefix = "airflow/rest_api"
data_interval_end = "{{ data_interval_end.strftime('%Y%m%dT%H%M%S') }}"
ano_particao = "{{ data_interval_end.year }}"
mes_particao = "{{ data_interval_end.month }}"

s3_bucket_landing = get_bucket_name("lake-landing")
s3_bucket_bronze = get_bucket_name("lake-bronze")
s3_bucket_gold = get_bucket_name("lake-gold")

min_end_date = "{{ (dag_run.logical_date - macros.timedelta(days=params['n_dias_passados'])).strftime('%Y-%m-%dT%H:%M:%SZ') }}"

params = [
    {
        "s3_folder_name": "task_instances",
        "api_endpoint": "api/v1/dags/~/dagRuns/~/taskInstances",
        "response_contents_key": "task_instances",
        "schema": SCHEMA_AIRFLOW_TASK_INSTANCES,
        "end_date_gte": min_end_date,
    },
    {
        "s3_folder_name": "dag_runs",
        "api_endpoint": "api/v1/dags/~/dagRuns",
        "response_contents_key": "dag_runs",
        "schema": SCHEMA_AIRFLOW_DAG_RUNS,
        "end_date_gte": min_end_date,
    },
    {
        "s3_folder_name": "dags",
        "api_endpoint": "api/v1/dags",
        "response_contents_key": "dags",
        "schema": SCHEMA_AIRFLOW_DAGS,
    },
]

with DAG(
    "dag_extracao_metadados_airflow",
    default_args=default_args,
    schedule_interval="0 0 * * 2-6",
    catchup=False,
    render_template_as_native_obj=True,
    params={"n_dias_passados": 15},  # configura retroatividade de obtenção de
    # task instances e dag runs
) as dag:

    for param in params:
        s3_table_key = f"{folder_prefix}/{param['s3_folder_name']}"
        s3_partitions_key = f"ano_particao={ano_particao}/mes_particao={mes_particao}"
        s3_filename = f"{param['s3_folder_name']}_{data_interval_end}"
        s3_object_key = f"{s3_table_key}/{s3_partitions_key}/{s3_filename}"

        task_escrita_landing = PythonOperator(
            task_id=f"escrita_landing_{param['s3_folder_name']}",
            python_callable=extracao_metadados_generico,
            op_kwargs={
                "s3_obj_key": f"{s3_object_key}.json",
                "endpoint": param["api_endpoint"],
                "end_date_gte": param.get("end_date_gte"),
            },
        )

        task_qualidade_landing = PythonOperator(
            task_id=f"qualidade_landing_{param['s3_folder_name']}",
            python_callable=verifica_qualidade_landing,
            op_kwargs={
                "s3_bucket": s3_bucket_landing,
                "s3_table_key": s3_table_key,
                "s3_partitions_key": s3_partitions_key,
                "s3_filename": f"{s3_filename}.json",
                "schema": param["schema"],
                "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
                "funcao_leitura_landing": leitura_rest_api_airflow,
                "kwargs_funcao_leitura_landing": {
                    "s3_bucket": s3_bucket_landing,
                    "s3_key": f"{s3_object_key}.json",
                    "airflow_obj": param["response_contents_key"],
                },
            },
            on_failure_callback=qualidade_task_failed,
        )

        task_escrita_bronze = PythonOperator(
            task_id=f"escrita_bronze_{param['s3_folder_name']}",
            python_callable=transferencia_landing_para_bronze,
            op_kwargs={
                "s3_bucket_bronze": s3_bucket_bronze,
                "s3_table_key": s3_table_key,
                "s3_partitions_key": s3_partitions_key,
                "schema": param["schema"],
                "data_interval_end": data_interval_end,
                "funcao_leitura_landing": leitura_rest_api_airflow,
                "kwargs_funcao_leitura_landing": {
                    "s3_bucket": s3_bucket_landing,
                    "s3_key": f"{s3_object_key}.json",
                    "airflow_obj": param["response_contents_key"],
                },
            },
        )

        task_escrita_gold = PythonOperator(
            task_id=f"escrita_gold_{param['s3_folder_name']}",
            python_callable=escrita_gold,
            op_kwargs={
                "ts_dagrun": data_interval_end,
                "schema": param["schema"],
                "path_bronze": f"s3://{s3_bucket_bronze}/{s3_table_key}",
                "path_gold": f"s3://{s3_bucket_gold}/airflow/{param['s3_folder_name']}",
            },
        )

        (
            task_escrita_landing
            >> task_qualidade_landing
            >> task_escrita_bronze
            >> task_escrita_gold
        )

    s3_folder_name = "event_logs"
    s3_table_key = f"airflow/rest_api/{s3_folder_name}"
    s3_partitions_key = f"ano_particao={ano_particao}/mes_particao={mes_particao}"
    s3_filename = f"{s3_folder_name}_{data_interval_end}"
    s3_object_key = f"{s3_table_key}/{s3_partitions_key}/{s3_filename}"

    task_extracao = PythonOperator(
        task_id=f"extracao_metadados_{s3_folder_name}",
        python_callable=extracao_metadados_event_logs,
        op_kwargs={
            "s3_obj_key": f"{s3_object_key}.json",
            "limit": 10000,
            "order_by": "-event_log_id",
        },
    )

    task_qualidade_landing = PythonOperator(
        task_id=f"qualidade_landing_{s3_folder_name}",
        python_callable=verifica_qualidade_landing,
        op_kwargs={
            "s3_bucket": s3_bucket_landing,
            "s3_table_key": s3_table_key,
            "s3_partitions_key": s3_partitions_key,
            "s3_filename": f"{s3_filename}.json",
            "schema": SCHEMA_AIRFLOW_EVENT_LOGS,
            "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
            "funcao_leitura_landing": leitura_rest_api_airflow,
            "kwargs_funcao_leitura_landing": {
                "s3_bucket": s3_bucket_landing,
                "s3_key": f"{s3_object_key}.json",
                "airflow_obj": "event_logs",
                "is_event_log": True,
            },
        },
        on_failure_callback=qualidade_task_failed,
    )

    task_escrita_bronze = PythonOperator(
        task_id=f"escrita_bronze_{s3_folder_name}",
        python_callable=transferencia_landing_para_bronze,
        op_kwargs={
            "s3_bucket_bronze": s3_bucket_bronze,
            "s3_table_key": s3_table_key,
            "s3_partitions_key": s3_partitions_key,
            "schema": SCHEMA_AIRFLOW_EVENT_LOGS,
            "data_interval_end": data_interval_end,
            "funcao_leitura_landing": leitura_rest_api_airflow,
            "kwargs_funcao_leitura_landing": {
                "s3_bucket": s3_bucket_landing,
                "s3_key": f"{s3_object_key}.json",
                "airflow_obj": "event_logs",
                "is_event_log": True,
            },
        },
    )

    task_extracao >> task_qualidade_landing >> task_escrita_bronze

    s3_folder_name = "task_executions"
    s3_table_key = f"airflow/s3_logs/{s3_folder_name}"
    s3_partitions_key = f"ano_particao={ano_particao}/mes_particao={mes_particao}"
    s3_filename = f"{s3_folder_name}_{data_interval_end}"
    s3_object_key = f"{s3_table_key}/{s3_partitions_key}/{s3_filename}"

    task_extracao = PythonOperator(
        task_id=f"extracao_metadados_{s3_folder_name}",
        python_callable=parse_task_execution_logs,
        op_kwargs={
            "s3_obj_key": f"{s3_object_key}.parquet",
        },
    )

    task_qualidade_landing = PythonOperator(
        task_id=f"qualidade_landing_{s3_folder_name}",
        python_callable=verifica_qualidade_landing,
        op_kwargs={
            "s3_bucket": s3_bucket_landing,
            "s3_table_key": s3_table_key,
            "s3_partitions_key": s3_partitions_key,
            "s3_filename": f"{s3_filename}.parquet",
            "schema": SCHEMA_AIRFLOW_TASK_EXECUTIONS,
            "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
            "funcao_leitura_landing": pd.read_parquet,
            "kwargs_funcao_leitura_landing": {
                "path": f"s3://{s3_bucket_landing}/{s3_object_key}.parquet",
            },
        },
        on_failure_callback=qualidade_task_failed,
    )

    task_escrita_bronze = PythonOperator(
        task_id=f"escrita_bronze_{s3_folder_name}",
        python_callable=transferencia_landing_para_bronze,
        op_kwargs={
            "s3_bucket_bronze": s3_bucket_bronze,
            "s3_table_key": s3_table_key,
            "s3_partitions_key": s3_partitions_key,
            "schema": SCHEMA_AIRFLOW_TASK_EXECUTIONS,
            "data_interval_end": data_interval_end,
            "funcao_leitura_landing": pd.read_parquet,
            "kwargs_funcao_leitura_landing": {
                "path": f"s3://{s3_bucket_landing}/{s3_object_key}.parquet",
            },
        },
    )

    task_escrita_gold = PythonOperator(
        task_id=f"escrita_gold_{s3_folder_name}",
        python_callable=escrita_gold,
        op_kwargs={
            "ts_dagrun": data_interval_end,
            "schema": SCHEMA_AIRFLOW_TASK_EXECUTIONS,
            "path_bronze": f"s3://{s3_bucket_bronze}/{s3_table_key}",
            "path_gold": f"s3://{s3_bucket_gold}/airflow/{s3_folder_name}",
        },
    )

    task_extracao >> task_qualidade_landing >> task_escrita_bronze >> task_escrita_gold
