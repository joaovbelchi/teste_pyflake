from datetime import datetime
from utils.msteams import msteams_task_failed
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from utils.schemas import (
    SCHEMA_BTG_ACCES_RELATORIO_MOVIMENTACAO,
    SCHEMA_BTG_ACCES_RELATORIO_NET_NEW_MONEY,
)
from utils.datasets import (
    TB_MOVIMENTACAO_HISTORICO_BTG_ACCESS_LANDING,
    TB_NNM_HISTORICO_BTG_ACCESS_LANDING,
    TB_MOVIMENTACAO_HISTORICO_BTG_ACCESS_BRONZE,
    TB_NNM_HISTORICO_BTG_ACCESS_BRONZE,
)
from utils.DagIngestaoNNMHistorico.functions import (
    verifica_objeto_modificado_s3,
    move_objeto_s3,
)
from utils.functions import (
    leitura_excel_s3,
    verifica_qualidade_landing,
    transferencia_landing_para_bronze,
    parse_ts_dag_run,
)
from utils.bucket_names import get_bucket_name
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


dag_name = "dag_ingestao_nnm_historico_btg"

default_args = {
    "owner": "Felipe Nogueira",
    "email": ["felipe.nogueira@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2023, 11, 2),
    "retries": 0,
    "timeout": 360,
    "mode": "reschedule",
    "poke_interval": 60,
}

table_params = [
    {
        "s3_table_name": "net_new_money_historico",
        "extension": ".xlsx",
        "schema": SCHEMA_BTG_ACCES_RELATORIO_NET_NEW_MONEY,
        "func_leitura_landing": leitura_excel_s3,
        "kwargs_func_leitura_landing": {"skiprows": 2},
        "datasets_list_landing": [TB_NNM_HISTORICO_BTG_ACCESS_LANDING],
        "datasets_list_bronze": [TB_NNM_HISTORICO_BTG_ACCESS_BRONZE],
    },
    {
        "s3_table_name": "movimentacao_historico",
        "extension": ".xlsx",
        "schema": SCHEMA_BTG_ACCES_RELATORIO_MOVIMENTACAO,
        "func_leitura_landing": leitura_excel_s3,
        "kwargs_func_leitura_landing": {"skiprows": 2},
        "datasets_list_landing": [TB_MOVIMENTACAO_HISTORICO_BTG_ACCESS_LANDING],
        "datasets_list_bronze": [TB_MOVIMENTACAO_HISTORICO_BTG_ACCESS_BRONZE],
    },
]

ano_particao = "{{ parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger, return_datetime=True).year }}"
mes_particao = "{{ parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger, return_datetime=True).month }}"
ts_dagrun = "{{ parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger) }}"

tasks_finais_bronze = []
with DAG(
    dag_name,
    default_args=default_args,
    schedule_interval="0 20 * * 5",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    user_defined_macros={"parse_ts_dag_run": parse_ts_dag_run},
) as dag:
    for params in table_params:
        landing_obj_table_key = f"btg/access/relatorio/{params['s3_table_name']}"
        landing_obj_partition_key = (
            f"ano_particao={ano_particao}/mes_particao={mes_particao}"
        )
        landing_obj_filename = (
            f"{params['s3_table_name']}_{ts_dagrun}{params['extension']}"
        )
        landing_obj_key = f"{landing_obj_table_key}/{landing_obj_partition_key}/{landing_obj_filename}"
        upload_obj_key = f"btg/access/relatorio/upload_manual/{params['s3_table_name']}{params['extension']}"

        sensor_upload_s3 = PythonSensor(
            task_id=f"sensor_{params['s3_table_name']}_upload_s3",
            python_callable=verifica_objeto_modificado_s3,
            op_kwargs={
                "s3_bucket_name": get_bucket_name("lake-landing"),
                "s3_object_key": upload_obj_key,
            },
            on_failure_callback=qualidade_task_failed,
        )

        task_move_upload_para_landing = PythonOperator(
            task_id=f"transfer_{params['s3_table_name']}_landing",
            python_callable=move_objeto_s3,
            op_kwargs={
                "s3_bucket_name": get_bucket_name("lake-landing"),
                "s3_object_key_src": upload_obj_key,
                "s3_object_key_dst": landing_obj_key,
            },
        )

        task_verifica_qualidade_landing = PythonOperator(
            task_id=f"qualidade_landing_{params['s3_table_name']}",
            python_callable=verifica_qualidade_landing,
            op_kwargs={
                "s3_bucket": get_bucket_name("lake-landing"),
                "s3_table_key": landing_obj_table_key,
                "s3_partitions_key": landing_obj_partition_key,
                "s3_filename": landing_obj_filename,
                "schema": params["schema"],
                "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
                "funcao_leitura_landing": params["func_leitura_landing"],
                "kwargs_funcao_leitura_landing": {
                    "s3_bucket_landing": get_bucket_name("lake-landing"),
                    "s3_object_key": landing_obj_key,
                    **params["kwargs_func_leitura_landing"],
                },
            },
            on_failure_callback=qualidade_task_failed,
        )

        task_move_para_bronze = PythonOperator(
            task_id=f"transfer_{params['s3_table_name']}_bronze",
            python_callable=transferencia_landing_para_bronze,
            op_kwargs={
                "s3_bucket_bronze": get_bucket_name("lake-bronze"),
                "s3_table_key": landing_obj_table_key,
                "s3_partitions_key": landing_obj_partition_key,
                "schema": params["schema"],
                "data_interval_end": ts_dagrun,
                "funcao_leitura_landing": params["func_leitura_landing"],
                "kwargs_funcao_leitura_landing": {
                    "s3_bucket_landing": get_bucket_name("lake-landing"),
                    "s3_object_key": landing_obj_key,
                    **params["kwargs_func_leitura_landing"],
                },
            },
            outlets=params["datasets_list_bronze"],
        )

        (
            sensor_upload_s3
            >> task_move_upload_para_landing
            >> task_verifica_qualidade_landing
            >> task_move_para_bronze
        )

        tasks_finais_bronze.append(task_move_para_bronze)

    trigger_dag_nnm = TriggerDagRunOperator(
        task_id="trigger_dag_nnm",
        trigger_dag_id="dag_atualiza_nnm",
        execution_date=ts_dagrun,
        conf={
            "tipo_atualizacao": "historica",
            "upstream_dag": dag_name,
            "upstream_external_trigger": "{{ dag_run.external_trigger }}",
        },
        retries=2,
        retry_delay=120,
        retry_exponential_backoff=True,
    )

    tasks_finais_bronze >> trigger_dag_nnm
