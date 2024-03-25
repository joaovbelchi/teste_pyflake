from utils.msteams import msteams_task_failed
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime
from utils.DagPipeFundosBTG.schemas import (
    SCHEMA_BTG_WEBHOOK_FUNDOS,
    SCHEMA_BTG_WEBHOOK_FUNDOS_INFO,
)
from utils.datasets import (
    TB_FUNDOS_BTG_WEBHOOK_LANDING,
    TB_FUNDOS_BTG_WEBHOOK_BRONZE,
    TB_FUNDOS_INFO_BTG_WEBHOOK_LANDING,
    TB_FUNDOS_INFO_BTG_WEBHOOK_BRONZE,
)
from utils.functions import (
    verifica_qualidade_landing,
    transferencia_landing_para_bronze,
    trigger_webhook_btg,
    verifica_objeto_s3,
    download_arquivo_webhook_btg,
    leitura_csv,
)
from utils.bucket_names import get_bucket_name


def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


default_args = {
    "owner": "Felipe Nogueira",
    "email": ["felipe.nogueira@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2023, 10, 25),
    "retries": 0,
    "timeout": 360,
    "mode": "poke",
    "poke_interval": 20,
}

table_params = [
    {
        "s3_table_name": "fundos",
        "extension": ".csv",
        "webhook_endpoint": "https://api.btgpactual.com/api-rm-reports/api/v1/rm-reports/funds",
        "schema": SCHEMA_BTG_WEBHOOK_FUNDOS,
        "func_leitura_landing": leitura_csv,
        "datasets_list_landing": [TB_FUNDOS_BTG_WEBHOOK_LANDING],
        "datasets_list_bronze": [TB_FUNDOS_BTG_WEBHOOK_BRONZE],
    },
    {
        "s3_table_name": "fundos_informacao",
        "extension": ".csv",
        "webhook_endpoint": "https://api.btgpactual.com/api-rm-reports/api/v1/rm-reports/funds-information",
        "schema": SCHEMA_BTG_WEBHOOK_FUNDOS_INFO,
        "func_leitura_landing": leitura_csv,
        "datasets_list_landing": [TB_FUNDOS_INFO_BTG_WEBHOOK_LANDING],
        "datasets_list_bronze": [TB_FUNDOS_INFO_BTG_WEBHOOK_BRONZE],
    },
]

ano_particao = "{{ data_interval_end.year }}"
mes_particao = "{{ data_interval_end.month }}"
data_interval_end = "{{ data_interval_end.strftime('%Y%m%dT%H%M%S') }}"

with DAG(
    "dag_fundos_btg",
    default_args=default_args,
    schedule_interval="5 11,18,23 * * 1-5",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:
    silver_upstream = []
    for params in table_params:
        response_obj_key = f"btg/webhooks/responses/{params['s3_table_name']}.json"
        landing_obj_table_key = f"btg/webhooks/downloads/{params['s3_table_name']}"
        landing_obj_partition_key = (
            f"ano_particao={ano_particao}/mes_particao={mes_particao}"
        )
        landing_obj_filename = (
            f"{params['s3_table_name']}_{data_interval_end}{params['extension']}"
        )
        landing_obj_key = f"{landing_obj_table_key}/{landing_obj_partition_key}/{landing_obj_filename}"

        task_trigger_webhook = PythonOperator(
            task_id=f"trigger_webhook_{params['s3_table_name']}",
            python_callable=trigger_webhook_btg,
            op_kwargs={"endpoint": params["webhook_endpoint"]},
        )

        sensor_aguarda_response = PythonSensor(
            task_id=f"aguarda_response_{params['s3_table_name']}",
            python_callable=verifica_objeto_s3,
            op_kwargs={
                "bucket": get_bucket_name("lake-landing"),
                "object_key": response_obj_key,
                "min_last_modified": "{{ data_interval_end }}",
            },
            on_failure_callback=qualidade_task_failed,
        )

        task_download_landing = PythonOperator(
            task_id=f"download_landing_{params['s3_table_name']}",
            python_callable=download_arquivo_webhook_btg,
            op_kwargs={
                "response_object_key": response_obj_key,
                "bucket": get_bucket_name("lake-landing"),
                "download_object_key": landing_obj_key,
            },
            outlets=params["datasets_list_landing"],
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
                },
            },
            on_failure_callback=qualidade_task_failed,
        )

        task_escrita_bronze = PythonOperator(
            task_id=f"escrita_bronze_{params['s3_table_name']}",
            python_callable=transferencia_landing_para_bronze,
            op_kwargs={
                "s3_bucket_bronze": get_bucket_name("lake-bronze"),
                "s3_table_key": landing_obj_table_key,
                "s3_partitions_key": landing_obj_partition_key,
                "schema": params["schema"],
                "data_interval_end": data_interval_end,
                "funcao_leitura_landing": params["func_leitura_landing"],
                "kwargs_funcao_leitura_landing": {
                    "s3_bucket_landing": get_bucket_name("lake-landing"),
                    "s3_object_key": landing_obj_key,
                },
            },
            outlets=params["datasets_list_bronze"],
        )

        (
            task_trigger_webhook
            >> sensor_aguarda_response
            >> task_download_landing
            >> task_verifica_qualidade_landing
            >> task_escrita_bronze
        )
