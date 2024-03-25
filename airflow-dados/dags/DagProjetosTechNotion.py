from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from utils.DagPipeTechNotion.functions import (
    download_base_projetos_tech_notion,
    leitura_json_to_dataframe,
    pipe_projetos_notion,
)

from utils.DagPipeTechNotion.schemas import SCHEMA_NOTION_PROJETOS_TECH
from utils.msteams import msteams_task_failed
from utils.bucket_names import get_bucket_name
from utils.datasets import (
    PROJETOS_TECH_NOTION_API_LANDING,
    PROJETOS_TECH_NOTION_API_BRONZE,
    PROJETOS_TECH_NOTION_API_GOLD,
)

from utils.functions import (
    verifica_qualidade_landing,
    transferencia_landing_para_bronze,
    parse_ts_dag_run,
)

schema = SCHEMA_NOTION_PROJETOS_TECH
datasets_list_landing = [PROJETOS_TECH_NOTION_API_LANDING]
datasets_list_bronze = [PROJETOS_TECH_NOTION_API_BRONZE]
datasets_list_gold = [PROJETOS_TECH_NOTION_API_GOLD]


def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


default_args = {
    "owner": "Eduardo Queiroz",
    "email": ["eduardo.queiroz@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2023, 10, 25),
    "retries": 0,
}

ano_particao = "{{ data_interval_end.year }}"
mes_particao = "{{ data_interval_end.month }}"
timestamp_dagrun = "{{ parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger) }}"

s3_table_name = "projetos"

extension = ".json"

landing_obj_table_key = f"notion/api_notion/tecnologia/{s3_table_name}"

landing_obj_filename = f"{s3_table_name}_{timestamp_dagrun}{extension}"

landing_obj_partition_key = f"ano_particao={ano_particao}/mes_particao={mes_particao}"

landing_obj_key = (
    f"{landing_obj_table_key}/{landing_obj_partition_key}/{landing_obj_filename}"
)

bucket_lake_bronze = get_bucket_name("lake-bronze")


description = """
Dag para coleta e faz o processamento da base de Projetos de Tech no Notion.
"""

with DAG(
    dag_id="dag_projetos_tech_notion",
    default_args=default_args,
    schedule_interval="55 13 * * 1-5",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    render_template_as_native_obj=True,
    description=description,
    user_defined_macros={"parse_ts_dag_run": parse_ts_dag_run},
) as dag:

    task_download_landing = PythonOperator(
        task_id=f"download_landing_{s3_table_name}",
        python_callable=download_base_projetos_tech_notion,
        op_kwargs={
            "bucket": get_bucket_name("lake-landing"),
            "download_object_key": landing_obj_key,
        },
        outlets=datasets_list_landing,
    )
    task_qualidade_landing = PythonOperator(
        task_id=f"qualidade_landing_{s3_table_name}",
        python_callable=verifica_qualidade_landing,
        op_kwargs={
            "s3_bucket": get_bucket_name("lake-landing"),
            "s3_table_key": landing_obj_table_key,
            "s3_partitions_key": landing_obj_partition_key,
            "s3_filename": landing_obj_filename,
            "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
            "schema": schema,
            "funcao_leitura_landing": leitura_json_to_dataframe,
            "kwargs_funcao_leitura_landing": {
                "s3_bucket_landing": get_bucket_name("lake-landing"),
                "s3_object_key": landing_obj_key,
            },
        },
        on_failure_callback=qualidade_task_failed,
    )

    task_escrita_bronze = PythonOperator(
        task_id=f"task_escrita_bronze_{s3_table_name}",
        python_callable=transferencia_landing_para_bronze,
        op_kwargs={
            "s3_bucket_bronze": get_bucket_name("lake-bronze"),
            "s3_table_key": landing_obj_table_key,
            "s3_partitions_key": landing_obj_partition_key,
            "schema": schema,
            "data_interval_end": timestamp_dagrun,
            "funcao_leitura_landing": leitura_json_to_dataframe,
            "kwargs_funcao_leitura_landing": {
                "s3_bucket_landing": get_bucket_name("lake-landing"),
                "s3_object_key": landing_obj_key,
            },
        },
        outlets=datasets_list_bronze,
    )

    task_escrita_gold = PythonOperator(
        task_id="task_escrita_gold",
        python_callable=pipe_projetos_notion,
        op_kwargs={"data_interval_end": timestamp_dagrun},
        outlets=[PROJETOS_TECH_NOTION_API_GOLD],
    )

    (
        task_download_landing
        >> task_qualidade_landing
        >> task_escrita_bronze
        >> task_escrita_gold
    )
