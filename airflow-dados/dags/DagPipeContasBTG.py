from utils.msteams import msteams_task_failed
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from utils.DagPipeContasBTG.functions import (
    escrita_gold_contas,
    escrita_silver_contas,
    leitura_zip,
    atualiza_contas_in_out,
    atualiza_contas_in_out_offshore,
    verifica_qualidade_contas_in_out,
    atualiza_status_contas_crm,
    SCHEMA_BTG_WEBHOOK_CONTAS_ASSESSOR,
    SCHEMA_BTG_WEBHOOK_DADOS_CADASTRAIS,
    SCHEMA_BTG_WEBHOOK_DADOS_ONBOARDING,
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
from utils.datasets import (
    TB_CONTAS_ASSESSOR_BTG_WEBHOOK_LANDING,
    TB_DADOS_CADASTRAIS_BTG_WEBHOOK_LANDING,
    TB_DADOS_ONBOARDING_BTG_WEBHOOK_LANDING,
    TB_DADOS_CADASTRAIS_BTG_WEBHOOK_BRONZE,
    TB_DADOS_ONBOARDING_BTG_WEBHOOK_BRONZE,
    TB_CONTAS_ASSESSOR_BTG_WEBHOOK_BRONZE,
    TB_CONTAS_BTG_SILVER_NOVA,
    TB_CONTAS_BTG_GOLD_NOVA,
    ACC_IN_OUT_SILVER,
    ACC_IN_OUT_GOLD,
    ACC_IN_OUT_OFFSHORE,
)


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
        "s3_table_name": "contas_por_assessor",
        "extension": ".zip",
        "webhook_endpoint": "https://api.btgpactual.com/iaas-account-advisor/api/v1/advisor/link/account",
        "schema": SCHEMA_BTG_WEBHOOK_CONTAS_ASSESSOR,
        "func_leitura_landing": leitura_zip,
        "datasets_list_landing": [TB_CONTAS_ASSESSOR_BTG_WEBHOOK_LANDING],
        "datasets_list_bronze": [TB_CONTAS_ASSESSOR_BTG_WEBHOOK_BRONZE],
    },
    {
        "s3_table_name": "dados_cadastrais",
        "extension": ".csv",
        "webhook_endpoint": "https://api.btgpactual.com/api-rm-reports/api/v1/rm-reports/registration-data",
        "schema": SCHEMA_BTG_WEBHOOK_DADOS_CADASTRAIS,
        "func_leitura_landing": leitura_csv,
        "datasets_list_landing": [TB_DADOS_CADASTRAIS_BTG_WEBHOOK_LANDING],
        "datasets_list_bronze": [TB_DADOS_CADASTRAIS_BTG_WEBHOOK_BRONZE],
    },
    {
        "s3_table_name": "dados_onboarding",
        "extension": ".csv",
        "webhook_endpoint": "https://api.btgpactual.com/api-rm-reports/api/v1/rm-reports/onboarding-data",
        "schema": SCHEMA_BTG_WEBHOOK_DADOS_ONBOARDING,
        "func_leitura_landing": leitura_csv,
        "datasets_list_landing": [TB_DADOS_ONBOARDING_BTG_WEBHOOK_LANDING],
        "datasets_list_bronze": [TB_DADOS_ONBOARDING_BTG_WEBHOOK_BRONZE],
    },
]

ano_particao = "{{ data_interval_end.year }}"
mes_particao = "{{ data_interval_end.month }}"
data_interval_end = "{{ data_interval_end.strftime('%Y%m%dT%H%M%S') }}"

with DAG(
    "dag_contas_btg",
    default_args=default_args,
    schedule_interval="5 11,18,23 * * *",
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

        silver_upstream.append(task_escrita_bronze)

    task_escrita_silver_contas = PythonOperator(
        task_id="escrita_silver_contas",
        python_callable=escrita_silver_contas,
        op_kwargs={"data_escrita": "{{ data_interval_end.date() }}"},
        outlets=[TB_CONTAS_BTG_SILVER_NOVA],
    )

    task_escrita_gold_contas = PythonOperator(
        task_id="escrita_gold_contas",
        python_callable=escrita_gold_contas,
        op_kwargs={"data_escrita": "{{ data_interval_end.date() }}"},
        outlets=[TB_CONTAS_BTG_GOLD_NOVA],
    )

    task_contas_in_out = PythonOperator(
        task_id="atualiza_contas_in_out",
        python_callable=atualiza_contas_in_out,
        op_kwargs={"date_interval_end": "{{ data_interval_end.date() }}"},
        outlets=[ACC_IN_OUT_SILVER],
    )

    task_contas_in_out_offshore = PythonOperator(
        task_id="atualiza_contas_in_out_offshore",
        python_callable=atualiza_contas_in_out_offshore,
        op_kwargs={
            "date_interval_end": "{{ data_interval_end.date() }}",
            "drive_id": "{{ var.value.get('guilherme_drive_id') }}",
        },
        outlets=[ACC_IN_OUT_OFFSHORE],
    )

    silver_obj_table_key = "btg/api/accounts_in_out"
    silver_obj_filename = "accounts_in_out.parquet"

    task_verifica_qualidade_contas_in_out = PythonOperator(
        task_id="qualidade_contas_in_out",
        python_callable=verifica_qualidade_contas_in_out,
        op_kwargs={
            "date_interval_end": "{{ data_interval_end.date() }}",
            "s3_bucket": get_bucket_name("lake-silver"),
            "s3_table_key": silver_obj_table_key,
            "s3_filename": silver_obj_filename,
            "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
        },
    )

    task_atualizacao_status_conta_crm = PythonOperator(
        task_id="atualizacao_status_conta_crm",
        python_callable=atualiza_status_contas_crm,
        op_kwargs={
            "timestamp_dagrun": data_interval_end,
        },
    )

    bucket_name_silver = get_bucket_name("lake-silver")
    bucket_name_gold = get_bucket_name("lake-gold")
    task_copiar_arquivo = S3CopyObjectOperator(
        task_id="copia_accounts_in_out_para_gold",
        source_bucket_name=bucket_name_silver,
        dest_bucket_name=bucket_name_gold,
        source_bucket_key="btg/api/accounts_in_out/accounts_in_out.parquet",
        dest_bucket_key="btg/accounts_in_out/accounts_in_out.parquet",
        outlets=[ACC_IN_OUT_GOLD],
    )

    (silver_upstream >> task_escrita_silver_contas >> task_escrita_gold_contas)

    (
        task_escrita_silver_contas
        >> task_contas_in_out
        >> task_verifica_qualidade_contas_in_out
        >> task_copiar_arquivo
    )
    task_copiar_arquivo >> task_atualizacao_status_conta_crm
    task_contas_in_out_offshore
