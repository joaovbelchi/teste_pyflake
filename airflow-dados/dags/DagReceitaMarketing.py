from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
from datetime import datetime

from utils.functions import parse_ts_dag_run
from utils.msteams import msteams_task_failed

from utils.bucket_names import get_bucket_name
from utils.functions import (
    verifica_qualidade_landing,
    transferencia_landing_para_bronze,
)
from utils.DagReceitaMarketing.functions import (
    transferencia_rds_para_s3_disparos_wpp,
    transferencia_airtable_para_s3_templates_receita,
    transferencia_bronze_2_silver_disparos_wpp,
    transferencia_bronze_2_silver_templates_receita,
)
from utils.DagReceitaMarketing.schemas import (
    SCHEMA_DISPAROS_WPP,
    SCHEMA_DISPAROS_WPP_SILVER,
    SCHEMA_TEMPLATES_RECEITA,
    SCHEMA_TEMPLATES_RECEITA_SILVER,
)

table_params = [
    {
        "s3_table_key": "one/rds/disparos_wpp",
        "s3_filename": "disparos_wpp",
        "schema_landing": SCHEMA_DISPAROS_WPP,
        "schema_silver": SCHEMA_DISPAROS_WPP_SILVER,
        "funcao_leitura_landing": pd.read_parquet,
        "funcao_extracao": transferencia_rds_para_s3_disparos_wpp,
        "funcao_processamento_silver": transferencia_bronze_2_silver_disparos_wpp,
    },
    {
        "s3_table_key": "one/rds/templates_receita",
        "s3_filename": "templates_receita",
        "schema_landing": SCHEMA_TEMPLATES_RECEITA,
        "schema_silver": SCHEMA_TEMPLATES_RECEITA_SILVER,
        "funcao_leitura_landing": pd.read_parquet,
        "funcao_extracao": transferencia_airtable_para_s3_templates_receita,
        "funcao_processamento_silver": transferencia_bronze_2_silver_templates_receita,
    },
]

timestamp_dagrun = "{{ parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger) }}"
date_interval_end = "{{ macros.dateutil.parser.isoparse(parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger)) }}"
ano_particao = "{{ macros.dateutil.parser.isoparse(parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger)).year }}"
mes_particao = "{{ macros.dateutil.parser.isoparse(parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger)).month }}"

description = """
Esta DAG é responsável por fazer o cruzamento dos dados de 
receita com os dados de disparos de campanhas de marketing 
via whatsapp. O processamento executado por esta DAG é incremental
e extrai sempre os dados de disparos do dia anterior, sendo assim
esta DAG deverá ser executada em todos os dias. Caso em algum 
dia haja uma falha nesta DAG o processamento dquele dia
deverá ser feito novamente.
"""

default_args = {
    "owner": "Eduardo Queiroz",
    "email": ["eduardo.queiroz@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2024, 3, 18),
    "retries": 0,
}

with DAG(
    dag_id="dag_receita_wpp_marketing__teste__final__",
    default_args=default_args,
    schedule_interval="0 17 * * *",
    catchup=True,
    max_active_runs=1,
    max_active_tasks=2,
    render_template_as_native_obj=True,
    description=description,
    user_defined_macros={
        "parse_ts_dag_run": parse_ts_dag_run,
    },
) as dag:

    up_stream = []
    for params in table_params:
        s3_partitions_key = f"ano_particao={ano_particao}/mes_particao={mes_particao}"
        s3_landing_file_name = f"{params['s3_filename']}_{timestamp_dagrun}.parquet"
        s3_landing_file_path = f"s3://{get_bucket_name('lake-landing')}/{params['s3_table_key']}/{s3_partitions_key}/{s3_landing_file_name}"
        s3_bronze_file_path = (
            f"s3://{get_bucket_name('lake-bronze')}/{params['s3_table_key']}/"
        )
        s3_silver_file_path = (
            f"s3://{get_bucket_name('lake-silver')}/{params['s3_table_key']}/"
        )

        task_extracao = PythonOperator(
            task_id=f"extracao_{params['s3_filename']}",
            python_callable=params["funcao_extracao"],
            op_kwargs={
                "schema": params["schema_landing"],
                "data_interval_end": date_interval_end,
                "s3_landing_file_path": s3_landing_file_path,
            },
        )

        task_verifica_qualidade_landing = PythonOperator(
            task_id=f"verifica_qualidade_{params['s3_filename']}",
            python_callable=verifica_qualidade_landing,
            op_kwargs={
                "s3_bucket": get_bucket_name("lake-landing"),
                "s3_table_key": params["s3_table_key"],
                "s3_partitions_key": s3_partitions_key,
                "s3_filename": s3_landing_file_name,
                "schema": params["schema_landing"],
                "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
                "funcao_leitura_landing": params["funcao_leitura_landing"],
                "kwargs_funcao_leitura_landing": {"path": s3_landing_file_path},
            },
        )

        task_transferencia_landing_para_bronze = PythonOperator(
            task_id=f"transferencia_landing_para_bronze_{params['s3_filename']}",
            python_callable=transferencia_landing_para_bronze,
            op_kwargs={
                "s3_bucket_bronze": get_bucket_name("lake-bronze"),
                "s3_table_key": params["s3_table_key"],
                "s3_partitions_key": s3_partitions_key,
                "schema": params["schema_landing"],
                "data_interval_end": date_interval_end,
                "funcao_leitura_landing": params["funcao_leitura_landing"],
                "kwargs_funcao_leitura_landing": {"path": s3_landing_file_path},
            },
        )

        task_transferencia_bronze_para_silver = PythonOperator(
            task_id=f"transferencia_bronze_para_silver_{params['s3_filename']}",
            python_callable=params["funcao_processamento_silver"],
            op_kwargs={
                "date_interval_end": date_interval_end,
                "path_bronze": s3_bronze_file_path,
                "path_silver": s3_silver_file_path,
                "schema": params["schema_silver"],
            },
        )

        (
            task_extracao
            >> task_verifica_qualidade_landing
            >> task_transferencia_landing_para_bronze
            >> task_transferencia_bronze_para_silver
        )
