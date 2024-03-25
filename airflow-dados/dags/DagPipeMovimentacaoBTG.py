from utils.msteams import msteams_task_failed
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from utils.functions import (
    verifica_qualidade_landing,
    transferencia_landing_para_bronze,
    trigger_webhook_btg,
    verifica_objeto_s3,
    download_arquivo_webhook_btg,
    leitura_csv,
)
from utils.bucket_names import get_bucket_name
from utils.DagPipeMovimentacaoBTG.schemas import SCHEMA_BTG_RELATORIO_MOVIMENTACAO
from utils.functions import parse_ts_dag_run, s3_to_onedrive
from utils.DagPipeMovimentacaoBTG.functions import atualiza_movimentacao_silver
from utils.datasets import (
    TB_MOVIMENTACAO_BTG_WEBHOOK_LANDING,
    TB_MOVIMENTACAO_BTG_WEBHOOK_BRONZE,
    TB_MOVIMENTACAO_BTG_SILVER,
)
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import ShortCircuitOperator


def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


def verifica_se_run_manual(dag_run):
    return dag_run.external_trigger


default_args = {
    "owner": "Felipe Nogueira",
    "email": ["felipe.nogueira@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2023, 10, 25),
    "retries": 0,
    "timeout": 600,
    "mode": "poke",
    "poke_interval": 20,
}

timestamp_dagrun = "{{ parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger) }}"

ano_particao = "{{ parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger, return_datetime= True).year }}"
mes_particao = "{{ parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger, return_datetime= True).month }}"


s3_table_name = "movimentacao"
extension = ".csv"
webhook_endpoint = (
    "https://api.btgpactual.com/api-rm-reports/api/v1/rm-reports/movement"
)
schema = SCHEMA_BTG_RELATORIO_MOVIMENTACAO
func_leitura_landing = leitura_csv
datasets_list_landing = [TB_MOVIMENTACAO_BTG_WEBHOOK_LANDING]
datasets_list_bronze = [TB_MOVIMENTACAO_BTG_WEBHOOK_BRONZE]
datasets_list_silver = [TB_MOVIMENTACAO_BTG_SILVER]

response_obj_key = f"btg/webhooks/responses/{s3_table_name}.json"
landing_obj_table_key = f"btg/webhooks/downloads/{s3_table_name}"
landing_obj_partition_key = f"ano_particao={ano_particao}/mes_particao={mes_particao}"
landing_obj_filename = f"{s3_table_name}_{timestamp_dagrun}{extension}"
landing_obj_key = (
    f"{landing_obj_table_key}/{landing_obj_partition_key}/{landing_obj_filename}"
)

with DAG(
    "dag_movimentacao_btg",
    default_args=default_args,
    schedule_interval="5 11,18,23 * * 1-5",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    user_defined_macros={"parse_ts_dag_run": parse_ts_dag_run},
) as dag:
    task_trigger_webhook = PythonOperator(
        task_id=f"trigger_webhook_{s3_table_name}",
        python_callable=trigger_webhook_btg,
        op_kwargs={"endpoint": webhook_endpoint},
    )

    sensor_aguarda_response = PythonSensor(
        task_id=f"aguarda_response_{s3_table_name}",
        python_callable=verifica_objeto_s3,
        op_kwargs={
            "bucket": get_bucket_name("lake-landing"),
            "object_key": response_obj_key,
            "min_last_modified": "{{ parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger, return_datetime= True)}}",
        },
        on_failure_callback=qualidade_task_failed,
    )

    task_download_landing = PythonOperator(
        task_id=f"download_landing_{s3_table_name}",
        python_callable=download_arquivo_webhook_btg,
        op_kwargs={
            "response_object_key": response_obj_key,
            "bucket": get_bucket_name("lake-landing"),
            "download_object_key": landing_obj_key,
        },
        outlets=datasets_list_landing,
    )

    task_verifica_qualidade_landing = PythonOperator(
        task_id=f"qualidade_landing_{s3_table_name}",
        python_callable=verifica_qualidade_landing,
        op_kwargs={
            "s3_bucket": get_bucket_name("lake-landing"),
            "s3_table_key": landing_obj_table_key,
            "s3_partitions_key": landing_obj_partition_key,
            "s3_filename": landing_obj_filename,
            "schema": schema,
            "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
            "funcao_leitura_landing": func_leitura_landing,
            "kwargs_funcao_leitura_landing": {
                "s3_bucket_landing": get_bucket_name("lake-landing"),
                "s3_object_key": landing_obj_key,
            },
        },
        on_failure_callback=qualidade_task_failed,
    )

    task_escrita_bronze = PythonOperator(
        task_id=f"escrita_bronze_{s3_table_name}",
        python_callable=transferencia_landing_para_bronze,
        op_kwargs={
            "s3_bucket_bronze": get_bucket_name("lake-bronze"),
            "s3_table_key": landing_obj_table_key,
            "s3_partitions_key": landing_obj_partition_key,
            "schema": schema,
            "data_interval_end": timestamp_dagrun,
            "funcao_leitura_landing": func_leitura_landing,
            "kwargs_funcao_leitura_landing": {
                "s3_bucket_landing": get_bucket_name("lake-landing"),
                "s3_object_key": landing_obj_key,
            },
        },
        outlets=datasets_list_bronze,
    )

    condicao_verifica_se_run_manual = ShortCircuitOperator(
        task_id="verifica_se_run_manual",
        python_callable=verifica_se_run_manual,
        ignore_downstream_trigger_rules=False,
    )

    sensor_dag_contas = ExternalTaskSensor(
        task_id="sensor_dag_contas",
        external_dag_id="dag_contas_btg",
    )

    task_escrita_silver = PythonOperator(
        task_id=f"escrita_silver_{s3_table_name}",
        python_callable=atualiza_movimentacao_silver,
        op_kwargs={"data_interval_end": timestamp_dagrun},
        outlets=datasets_list_silver,
        trigger_rule="none_failed_min_one_success",
    )

    (
        task_trigger_webhook
        >> sensor_aguarda_response
        >> task_download_landing
        >> task_verifica_qualidade_landing
        >> task_escrita_bronze
        >> task_escrita_silver
    )

    condicao_verifica_se_run_manual >> sensor_dag_contas >> task_escrita_silver
