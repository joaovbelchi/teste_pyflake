from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from datetime import datetime

from utils.msteams import msteams_task_failed
from utils.bucket_names import get_bucket_name
from utils.functions import (
    transferencia_onedrive_para_s3,
    verifica_qualidade_landing,
    transferencia_landing_para_bronze,
    trigger_glue,
    parse_ts_dag_run,
)

from utils.DagReceita.functions import (
    leitura_parquet_landing,
    leitura_excel_landing,
    processa_bases_incrementais,
    processa_bases_historicas,
    bronze_2_silver,
    processa_consolidado_historico_silver,
    calcula_receita_consolidada_aplicativo,
    qualidade_silver,
)
from utils.DagReceita.functions_aai import carrega_e_limpa_base_aai
from utils.DagReceita.functions_icatu import carrega_e_limpa_base_icatu
from utils.DagReceita.functions_ouribank import carrega_e_limpa_base_ouribank
from utils.DagReceita.functions_offshore_us import carrega_e_limpa_base_offshore_us
from utils.DagReceita.functions_extras import carrega_e_limpa_base_extras
from utils.DagReceita.functions_onewm import carrega_e_limpa_base_onewm
from utils.DagReceita.functions_avenue import carrega_e_limpa_avenue
from utils.DagReceita.functions_prev_btg import (
    prev_btg_bronze_2_silver,
    carrega_e_limpa_previdencia_btg,
)
from utils.DagReceita.functions_semanal import (
    carrega_e_limpa_base_semanal,
    semanal_vazia,
)
from utils.DagReceita.functions_corban import (
    corban_bronze_2_silver,
    carrega_e_limpa_base_corban,
    leitura_corban_landing,
    envio_email_btg_empresas,
)
from utils.DagReceita.functions_offshore_cy import (
    carrega_e_limpa_base_offshore_cy,
    leitura_cayman_landing,
)

from utils.DagReceita.functions_prudential import (
    transferencia_prudential_para_s3,
    carrega_e_limpa_base_prudential,
)

from utils.datasets import (
    AAI_LANDING,
    AAI_BRONZE,
    AAI_SILVER_PRE_PROCESSADA,
    AAI_SILVER_PROCESSADA,
    AVENUE_LANDING,
    AVENUE_BRONZE,
    AVENUE_SILVER_PRE_PROCESSADA,
    AVENUE_SILVER_PROCESSADA,
    CORBAN_LANDING,
    CORBAN_BRONZE,
    CORBAN_SILVER_PRE_PROCESSADA,
    CORBAN_SILVER_PROCESSADA,
    EXTRAS_LANDING,
    EXTRAS_BRONZE,
    EXTRAS_SILVER_PROCESSADA,
    ICATU_LANDING,
    ICATU_BRONZE,
    ICATU_SILVER_PRE_PROCESSADA,
    ICATU_SILVER_PROCESSADA,
    OFFSHORE_US_LANDING,
    OFFSHORE_US_BRONZE,
    OFFSHORE_US_SILVER_PRE_PROCESSADA,
    OFFSHORE_US_SILVER_PROCESSADA,
    ONEWM_LANDING,
    ONEWM_BRONZE,
    ONEWM_SILVER_PROCESSADA,
    OURIBANK_LANDING,
    OURIBANK_BRONZE,
    OURIBANK_SILVER_PRE_PROCESSADA,
    OURIBANK_SILVER_PROCESSADA,
    PREV_BTG_LANDING,
    PREV_BTG_BRONZE,
    PREV_BTG_SILVER_PRE_PROCESSADA,
    PREV_BTG_SILVER_PROCESSADA,
    SEMANAL_LANDING,
    SEMANAL_BRONZE,
    SEMANAL_SILVER_PROCESSADA,
    OFFSHORE_CY_LANDING,
    OFFSHORE_CY_BRONZE,
    OFFSHORE_CY_SILVER_PRE_PROCESSADA,
    OFFSHORE_CY_SILVER_PROCESSADA,
    PRUDENTIAL_LANDING,
    PRUDENTIAL_BRONZE,
    PRUDENTIAL_SILVER_PROCESSADA,
)
from utils.DagReceita.schemas import (
    SCHEMA_AAI,
    SCHEMA_AVENUE,
    SCHEMA_CORBAN,
    SCHEMA_EXTRAS,
    SCHEMA_ICATU,
    SCHEMA_OFFSHORE_US,
    SCHEMA_ONEWM,
    SCHEMA_OURIBANK,
    SCHEMA_PREV_BTG,
    SCHEMA_SEMANAL,
    SCHEMA_PRUDENTIAL,
    SCHEMA_OFFSHORE_CY,
    SCHEMA_SILVER_PROCESSADA,
)


def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


def confere_base(params, file_name):
    if file_name not in params:
        raise AirflowSkipException


s3_bucket_landing = get_bucket_name("lake-landing")
s3_bucket_bronze = get_bucket_name("lake-bronze")
s3_bucket_silver = get_bucket_name("lake-silver")


data_interval_end = "{{ parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger) }}"
ano_particao = "{{ macros.dateutil.parser.isoparse(parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger)).year }}"
mes_particao = "{{ macros.dateutil.parser.isoparse(parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger)).month }}"
file_year = "{{ (macros.dateutil.parser.isoparse(parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger)).replace(day=1)-macros.timedelta(days=1)).year }}"
file_month = "{{ (macros.dateutil.parser.isoparse(parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger)).replace(day=1)-macros.timedelta(days=1)).month }}"


default_args = {
    "owner": "Eduardo Queiroz",
    "email": ["eduardo.queiroz@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2023, 10, 25),
    "params": {
        "bases_processadas": {
            # "aai": "incremental",
            # "extras": None,
            # "icatu": "incremental",
            # "offshore_us": "incremental",
            # "offshore_cy": "incremental",
            "onewm": None,
            # "ouribank": "incremental",
            # "corban": "incremental",
            # "prevbtg": "incremental",
            # "avenue": "incremental",
            # "semanal": None,
            # "prudential": None,
        },
    },
    "retries": 0,
}

bases_historicas = ["extras", "onewm", "semanal", "prudential"]

table_params = [
    # Base AAI
    {
        "onedrive_file_name": f"aai_{file_year}_{file_month}",
        "onedrive_file_extension": ".parquet",
        "s3_folder_name": "aai",
        "s3_folder_prefix": "btg/onedrive/receita",
        "datasets_list_landing": [AAI_LANDING],
        "datasets_list_bronze": [AAI_BRONZE],
        "datasets_list_silver_step_1": [AAI_SILVER_PRE_PROCESSADA],
        "datasets_list_silver_step_2": [AAI_SILVER_PROCESSADA],
        "schema": SCHEMA_AAI,
        "onedrive_path_prefix": [
            "One Analytics",
            "Banco de dados",
            "Histórico",
            "Receita",
            "Bases Intermediárias",
            "AAI",
        ],
        "funcao_leitura_landing": leitura_parquet_landing,
        "funcao_transf_silver": bronze_2_silver,
        "funcao_processa_silver": carrega_e_limpa_base_aai,
        "wrapper_processa_silver": processa_bases_incrementais,
        "s3_bronze_path": f"s3://{s3_bucket_bronze}/btg/onedrive/receita/aai/",
        "s3_silver_step_1_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/aai_pre_processada/",
        "s3_silver_step_2_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/aai_processada/",
        "schema_silver": SCHEMA_SILVER_PROCESSADA,
        "atualizacao": "{{ params.bases_processadas['aai'] }}",
    },
    # Base AVENUE
    {
        "onedrive_file_name": f"avenue_{file_year}_{file_month}",
        "onedrive_file_extension": ".parquet",
        "s3_folder_name": "avenue",
        "s3_folder_prefix": "btg/onedrive/receita",
        "datasets_list_landing": [AVENUE_LANDING],
        "datasets_list_bronze": [AVENUE_BRONZE],
        "datasets_list_silver_step_1": [AVENUE_SILVER_PRE_PROCESSADA],
        "datasets_list_silver_step_2": [AVENUE_SILVER_PROCESSADA],
        "schema": SCHEMA_AVENUE,
        "onedrive_path_prefix": [
            "One Analytics",
            "Banco de dados",
            "Histórico",
            "Receita",
            "Bases Intermediárias",
            "AVENUE",
        ],
        "funcao_leitura_landing": leitura_parquet_landing,
        "funcao_transf_silver": bronze_2_silver,
        "funcao_processa_silver": carrega_e_limpa_avenue,
        "wrapper_processa_silver": processa_bases_incrementais,
        "s3_bronze_path": f"s3://{s3_bucket_bronze}/btg/onedrive/receita/avenue/",
        "s3_silver_step_1_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/avenue_pre_processada/",
        "s3_silver_step_2_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/avenue_processada/",
        "schema_silver": SCHEMA_SILVER_PROCESSADA,
        "atualizacao": "{{ params.bases_processadas['avenue'] }}",
    },
    # Base CORBAN
    {
        "onedrive_file_name": f"corban_{file_year}_{file_month}",
        "onedrive_file_extension": ".xlsx",
        "s3_folder_name": "corban",
        "s3_folder_prefix": "btg/onedrive/receita",
        "datasets_list_landing": [CORBAN_LANDING],
        "datasets_list_bronze": [CORBAN_BRONZE],
        "datasets_list_silver_step_1": [CORBAN_SILVER_PRE_PROCESSADA],
        "datasets_list_silver_step_2": [CORBAN_SILVER_PROCESSADA],
        "schema": SCHEMA_CORBAN,
        "onedrive_path_prefix": [
            "One Analytics",
            "Banco de dados",
            "Histórico",
            "Receita",
            "Bases Intermediárias",
            "CORBAN",
        ],
        "funcao_leitura_landing": leitura_corban_landing,
        "funcao_transf_silver": corban_bronze_2_silver,
        "funcao_processa_silver": carrega_e_limpa_base_corban,
        "wrapper_processa_silver": processa_bases_incrementais,
        "s3_bronze_path": f"s3://{s3_bucket_bronze}/btg/onedrive/receita/corban/",
        "s3_silver_step_1_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/corban_pre_processada/",
        "s3_silver_step_2_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/corban_processada/",
        "schema_silver": SCHEMA_SILVER_PROCESSADA,
        "atualizacao": "{{ params.bases_processadas['corban'] }}",
    },
    # Base EXTRAS
    {
        "onedrive_file_name": f"extras",
        "onedrive_file_extension": ".xlsx",
        "s3_folder_name": "extras",
        "s3_folder_prefix": "btg/onedrive/receita",
        "datasets_list_landing": [EXTRAS_LANDING],
        "datasets_list_bronze": [EXTRAS_BRONZE],
        "datasets_list_silver_step_2": [EXTRAS_SILVER_PROCESSADA],
        "schema": SCHEMA_EXTRAS,
        "onedrive_path_prefix": [
            "One Analytics",
            "Banco de dados",
            "Histórico",
            "Receita",
            "Bases Intermediárias",
            "EXTRAS",
        ],
        "funcao_leitura_landing": leitura_excel_landing,
        "funcao_processa_silver": carrega_e_limpa_base_extras,
        "wrapper_processa_silver": processa_bases_historicas,
        "s3_bronze_path": f"s3://{s3_bucket_bronze}/btg/onedrive/receita/extras/",
        "s3_silver_step_2_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/extras_processada/extras_processada.parquet",
        "schema_silver": SCHEMA_SILVER_PROCESSADA,
    },
    # Base ICATU
    {
        "onedrive_file_name": f"icatu_{file_year}_{file_month}",
        "onedrive_file_extension": ".xlsx",
        "s3_folder_name": "icatu",
        "s3_folder_prefix": "btg/onedrive/receita",
        "datasets_list_landing": [ICATU_LANDING],
        "datasets_list_bronze": [ICATU_BRONZE],
        "datasets_list_silver_step_1": [ICATU_SILVER_PRE_PROCESSADA],
        "datasets_list_silver_step_2": [ICATU_SILVER_PROCESSADA],
        "schema": SCHEMA_ICATU,
        "onedrive_path_prefix": [
            "One Analytics",
            "Banco de dados",
            "Histórico",
            "Receita",
            "Bases Intermediárias",
            "ICATU",
        ],
        "funcao_leitura_landing": leitura_excel_landing,
        "funcao_transf_silver": bronze_2_silver,
        "funcao_processa_silver": carrega_e_limpa_base_icatu,
        "wrapper_processa_silver": processa_bases_incrementais,
        "s3_bronze_path": f"s3://{s3_bucket_bronze}/btg/onedrive/receita/icatu/",
        "s3_silver_step_1_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/icatu_pre_processada/",
        "s3_silver_step_2_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/icatu_processada/",
        "schema_silver": SCHEMA_SILVER_PROCESSADA,
        "atualizacao": "{{ params.bases_processadas['icatu'] }}",
    },
    # Base OFFSHORE_US
    {
        "onedrive_file_name": f"offshore_us_{file_year}_{file_month}",
        "onedrive_file_extension": ".parquet",
        "s3_folder_name": "offshore_us",
        "s3_folder_prefix": "btg/onedrive/receita",
        "datasets_list_landing": [OFFSHORE_US_LANDING],
        "datasets_list_bronze": [OFFSHORE_US_BRONZE],
        "datasets_list_silver_step_1": [OFFSHORE_US_SILVER_PRE_PROCESSADA],
        "datasets_list_silver_step_2": [OFFSHORE_US_SILVER_PROCESSADA],
        "schema": SCHEMA_OFFSHORE_US,
        "onedrive_path_prefix": [
            "One Analytics",
            "Banco de dados",
            "Histórico",
            "Receita",
            "Bases Intermediárias",
            "OFFSHORE_US",
        ],
        "funcao_leitura_landing": leitura_parquet_landing,
        "funcao_transf_silver": bronze_2_silver,
        "funcao_processa_silver": carrega_e_limpa_base_offshore_us,
        "wrapper_processa_silver": processa_bases_incrementais,
        "s3_bronze_path": f"s3://{s3_bucket_bronze}/btg/onedrive/receita/offshore_us/",
        "s3_silver_step_1_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/offshore_us_pre_processada/",
        "s3_silver_step_2_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/offshore_us_processada/",
        "schema_silver": SCHEMA_SILVER_PROCESSADA,
        "atualizacao": "{{ params.bases_processadas['offshore_us'] }}",
    },
    # Base OFFSHORE_CY
    {
        "onedrive_file_name": f"offshore_cy_{file_year}_{file_month}",
        "onedrive_file_extension": ".xlsx",
        "s3_folder_name": "offshore_cy",
        "s3_folder_prefix": "btg/onedrive/receita",
        "datasets_list_landing": [OFFSHORE_CY_LANDING],
        "datasets_list_bronze": [OFFSHORE_CY_BRONZE],
        "datasets_list_silver_step_1": [OFFSHORE_CY_SILVER_PRE_PROCESSADA],
        "datasets_list_silver_step_2": [OFFSHORE_CY_SILVER_PROCESSADA],
        "schema": SCHEMA_OFFSHORE_CY,
        "onedrive_path_prefix": [
            "One Analytics",
            "Banco de dados",
            "Histórico",
            "Receita",
            "Bases Intermediárias",
            "OFFSHORE_CY",
        ],
        "funcao_leitura_landing": leitura_cayman_landing,
        "funcao_transf_silver": bronze_2_silver,
        "funcao_processa_silver": carrega_e_limpa_base_offshore_cy,
        "wrapper_processa_silver": processa_bases_incrementais,
        "s3_bronze_path": f"s3://{s3_bucket_bronze}/btg/onedrive/receita/offshore_cy/",
        "s3_silver_step_1_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/offshore_cy_pre_processada/",
        "s3_silver_step_2_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/offshore_cy_processada/",
        "schema_silver": SCHEMA_SILVER_PROCESSADA,
        "atualizacao": "{{ params.bases_processadas['offshore_cy'] }}",
    },
    # Base ONEWM
    {
        "onedrive_file_name": "onewm",
        "onedrive_file_extension": ".xlsx",
        "s3_folder_name": "onewm",
        "s3_folder_prefix": "btg/onedrive/receita",
        "datasets_list_landing": [ONEWM_LANDING],
        "datasets_list_bronze": [ONEWM_BRONZE],
        "datasets_list_silver_step_2": [ONEWM_SILVER_PROCESSADA],
        "schema": SCHEMA_ONEWM,
        "onedrive_path_prefix": [
            "One Analytics",
            "Banco de dados",
            "Histórico",
            "Receita",
            "Bases Intermediárias",
            "ONEWM",
        ],
        "funcao_leitura_landing": leitura_excel_landing,
        "funcao_processa_silver": carrega_e_limpa_base_onewm,
        "wrapper_processa_silver": processa_bases_historicas,
        "s3_bronze_path": f"s3://{s3_bucket_bronze}/btg/onedrive/receita/onewm/",
        "s3_silver_step_2_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/onewm_processada/onewm_processada.parquet",
        "schema_silver": SCHEMA_SILVER_PROCESSADA,
    },
    # Base OURIBANK
    {
        "onedrive_file_name": f"ouribank_{file_year}_{file_month}",
        "onedrive_file_extension": ".parquet",
        "s3_folder_name": "ouribank",
        "s3_folder_prefix": "btg/onedrive/receita",
        "datasets_list_landing": [OURIBANK_LANDING],
        "datasets_list_bronze": [OURIBANK_BRONZE],
        "datasets_list_silver_step_1": [OURIBANK_SILVER_PRE_PROCESSADA],
        "datasets_list_silver_step_2": [OURIBANK_SILVER_PROCESSADA],
        "schema": SCHEMA_OURIBANK,
        "onedrive_path_prefix": [
            "One Analytics",
            "Banco de dados",
            "Histórico",
            "Receita",
            "Bases Intermediárias",
            "OURIBANK",
        ],
        "funcao_leitura_landing": leitura_parquet_landing,
        "funcao_transf_silver": bronze_2_silver,
        "funcao_processa_silver": carrega_e_limpa_base_ouribank,
        "wrapper_processa_silver": processa_bases_incrementais,
        "s3_bronze_path": f"s3://{s3_bucket_bronze}/btg/onedrive/receita/ouribank/",
        "s3_silver_step_1_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/ouribank_pre_processada/",
        "s3_silver_step_2_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/ouribank_processada/",
        "schema_silver": SCHEMA_SILVER_PROCESSADA,
        "atualizacao": "{{ params.bases_processadas['ouribank'] }}",
    },
    # Base PREV BTG
    {
        "onedrive_file_name": f"prev_btg_{file_year}_{file_month}",
        "onedrive_file_extension": ".xlsx",
        "s3_folder_name": "prevbtg",
        "s3_folder_prefix": "btg/onedrive/receita",
        "datasets_list_landing": [PREV_BTG_LANDING],
        "datasets_list_bronze": [PREV_BTG_BRONZE],
        "datasets_list_silver_step_1": [PREV_BTG_SILVER_PRE_PROCESSADA],
        "datasets_list_silver_step_2": [PREV_BTG_SILVER_PROCESSADA],
        "schema": SCHEMA_PREV_BTG,
        "onedrive_path_prefix": [
            "One Analytics",
            "Banco de dados",
            "Histórico",
            "Receita",
            "Bases Intermediárias",
            "PREV BTG",
        ],
        "funcao_leitura_landing": leitura_excel_landing,
        "funcao_transf_silver": prev_btg_bronze_2_silver,
        "funcao_processa_silver": carrega_e_limpa_previdencia_btg,
        "wrapper_processa_silver": processa_bases_incrementais,
        "s3_bronze_path": f"s3://{s3_bucket_bronze}/btg/onedrive/receita/prevbtg/",
        "s3_silver_step_1_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/prevbtg_pre_processada/",
        "s3_silver_step_2_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/prevbtg_processada/",
        "schema_silver": SCHEMA_SILVER_PROCESSADA,
        "atualizacao": "{{ params.bases_processadas['prevbtg'] }}",
    },
    # Base SEMANAL
    {
        "onedrive_file_name": "semanal",
        "onedrive_file_extension": ".parquet",
        "s3_folder_name": "semanal",
        "s3_folder_prefix": "btg/onedrive/receita",
        "datasets_list_landing": [SEMANAL_LANDING],
        "datasets_list_bronze": [SEMANAL_BRONZE],
        "datasets_list_silver_step_2": [SEMANAL_SILVER_PROCESSADA],
        "schema": SCHEMA_SEMANAL,
        "onedrive_path_prefix": [
            "One Analytics",
            "Banco de dados",
            "Histórico",
            "Receita",
            "Bases Intermediárias",
            "SEMANAL",
        ],
        "funcao_leitura_landing": leitura_parquet_landing,
        "funcao_processa_silver": carrega_e_limpa_base_semanal,
        "wrapper_processa_silver": processa_bases_historicas,
        "s3_bronze_path": f"s3://{s3_bucket_bronze}/btg/onedrive/receita/semanal/",
        "s3_silver_step_2_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/semanal_processada/semanal_processada.parquet",
        "schema_silver": SCHEMA_SILVER_PROCESSADA,
    },
    # Base PRUDENTIAL
    {
        "s3_folder_name": "prudential",
        "file_extension": ".parquet",
        "s3_folder_name": "prudential",
        "s3_folder_prefix": "btg/onedrive/receita",
        "datasets_list_landing": [PRUDENTIAL_LANDING],
        "datasets_list_bronze": [PRUDENTIAL_BRONZE],
        "datasets_list_silver_step_2": [PRUDENTIAL_SILVER_PROCESSADA],
        "schema": SCHEMA_PRUDENTIAL,
        "funcao_leitura_landing": leitura_parquet_landing,
        "s3_landing_path": f"s3://{s3_bucket_landing}/btg/onedrive/receita/prudential/ano_particao={ano_particao}/mes_particao={mes_particao}/prudential_{data_interval_end}.parquet",
        "funcao_processa_silver": carrega_e_limpa_base_prudential,
        "wrapper_processa_silver": processa_bases_historicas,
        "s3_bronze_path": f"s3://{s3_bucket_bronze}/btg/onedrive/receita/prudential/",
        "s3_silver_step_2_path": f"s3://{s3_bucket_silver}/btg/onedrive/receita/prudential_processada/prudential_processada.parquet",
        "schema_silver": SCHEMA_SILVER_PROCESSADA,
    },
]

description = """
Dag para processamento da base de receita e suas dependências.
Esta Dag é disparada manualmente através de aplicativo específico
pelo time de produtos.
"""


# Definição do pipeline
with DAG(
    dag_id="Dag_Receita",
    default_args=default_args,
    schedule_interval="30 9 * * 1-5",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    description=description,
    max_active_tasks=4,
    user_defined_macros={"parse_ts_dag_run": parse_ts_dag_run},
) as dag:
    upstream = []
    for params in table_params:
        # Paths para os arquivos
        # Para a base prudential que está sendo extraida de um RDS a
        # definição de paths é ligeiramente diferente
        s3_table_key = f"{params['s3_folder_prefix']}/{params['s3_folder_name']}"
        s3_partitions_key = f"ano_particao={ano_particao}/mes_particao={mes_particao}"

        if params["s3_folder_name"] == "prudential":
            s3_filename = f"{params['s3_folder_name']}_{data_interval_end}{params['file_extension']}"
        else:
            s3_filename = f"{params['s3_folder_name']}_{data_interval_end}{params['onedrive_file_extension']}"
            onedrive_filename = (
                f"{params['onedrive_file_name']}{params['onedrive_file_extension']}"
            )

        s3_object_key = f"{s3_table_key}/{s3_partitions_key}/{s3_filename}"
        # Task para verificar se a base semanal está vazia
        # esta task só é criada quando o nome do arquivo for
        # semanal
        if params["s3_folder_name"] == "semanal":
            verifica_semanal = BranchPythonOperator(
                task_id=f"verifica_se_{params['onedrive_file_name']}_vazia",
                python_callable=semanal_vazia,
                op_kwargs={
                    "drive_id": "{{ var.value.get('guilherme_drive_id') }}",
                    "onedrive_path": params["onedrive_path_prefix"]
                    + [onedrive_filename],
                },
            )

            skip_semanal = EmptyOperator(task_id=f"skip_semanal")

        # Task que confere quais bases de dados devem ser processadas
        confere_trigger_config = PythonOperator(
            task_id=f"confere_trigger_condition_{params['s3_folder_name']}",
            python_callable=confere_base,
            op_kwargs={
                "params": "{{ params.bases_processadas }}",
                "file_name": params["s3_folder_name"],
            },
        )

        if params["s3_folder_name"] == "prudential":
            # Tasks extração onedrive to bronze
            transfer_landing = PythonOperator(
                task_id=f"transfer_{params['s3_folder_name']}_landing",
                python_callable=transferencia_prudential_para_s3,
                op_kwargs={
                    "s3_landing_path": params["s3_landing_path"],
                },
                outlets=params["datasets_list_landing"],
            )
        else:
            # Tasks extração onedrive to bronze
            transfer_landing = PythonOperator(
                task_id=f"transfer_{params['s3_folder_name']}_landing",
                python_callable=transferencia_onedrive_para_s3,
                op_kwargs={
                    "drive_id": "{{ var.value.get('guilherme_drive_id') }}",
                    "onedrive_path": params["onedrive_path_prefix"]
                    + [onedrive_filename],
                    "s3_bucket": s3_bucket_landing,
                    "s3_object_key": s3_object_key,
                },
                outlets=params["datasets_list_landing"],
            )

        # Task que verifica qualidade da landing
        qualidade_landing = PythonOperator(
            task_id=f"qualidade_{params['s3_folder_name']}_landing",
            python_callable=verifica_qualidade_landing,
            op_kwargs={
                "s3_bucket": s3_bucket_landing,
                "s3_table_key": s3_table_key,
                "s3_partitions_key": s3_partitions_key,
                "s3_filename": s3_filename,
                "schema": params["schema"],
                "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
                "funcao_leitura_landing": params["funcao_leitura_landing"],
                "kwargs_funcao_leitura_landing": {
                    "path": f"s3://{s3_bucket_landing}/{s3_object_key}"
                },
            },
            on_failure_callback=qualidade_task_failed,
        )

        # Task que transfere a base de dados para a bronze
        move_para_bronze = PythonOperator(
            task_id=f"transfer_{params['s3_folder_name']}_bronze",
            python_callable=transferencia_landing_para_bronze,
            op_kwargs={
                "s3_bucket_bronze": s3_bucket_bronze,
                "s3_table_key": s3_table_key,
                "s3_partitions_key": s3_partitions_key,
                "schema": params["schema"],
                "data_interval_end": data_interval_end,
                "funcao_leitura_landing": params["funcao_leitura_landing"],
                "kwargs_funcao_leitura_landing": {
                    "path": f"s3://{s3_bucket_landing}/{s3_object_key}"
                },
            },
            outlets=params["datasets_list_bronze"],
        )

        # Task que transfere a base de dados para a camada silver
        # para bases históricas só é salvo um único arquivo
        # parquet na camada silver
        if params["s3_folder_name"] not in bases_historicas:
            move_para_silver = PythonOperator(
                task_id=f"transfer_{params['s3_folder_name']}_silver",
                python_callable=params["funcao_transf_silver"],
                op_kwargs={
                    "ano_particao": ano_particao,
                    "mes_particao": mes_particao,
                    "s3_bronze_path": params["s3_bronze_path"],
                    "s3_silver_step_1_path": params["s3_silver_step_1_path"],
                    "schema": params["schema"],
                },
                outlets=params["datasets_list_silver_step_2"],
            )

            # Task para processamento da base de dados
            processa_silver = PythonOperator(
                task_id=f"processa_{params['s3_folder_name']}_silver",
                python_callable=params["wrapper_processa_silver"],
                op_kwargs={
                    "atualizacao": "histórica",
                    "ano_particao": ano_particao,
                    "mes_particao": mes_particao,
                    "s3_silver_step_1_path": params["s3_silver_step_1_path"],
                    "s3_silver_step_2_path": params["s3_silver_step_2_path"],
                    "funcao_processamento": params["funcao_processa_silver"],
                    "schema": params["schema_silver"],
                },
                outlets=params["datasets_list_silver_step_2"],
            )
        else:
            # Task para processamento da base de dados
            processa_silver = PythonOperator(
                task_id=f"processa_{params['s3_folder_name']}_silver",
                python_callable=params["wrapper_processa_silver"],
                op_kwargs={
                    "atualizacao": "histórica",
                    "ano_particao": ano_particao,
                    "mes_particao": mes_particao,
                    "s3_bronze_path": params["s3_bronze_path"],
                    "s3_silver_step_2_path": params["s3_silver_step_2_path"],
                    "funcao_processamento": params["funcao_processa_silver"],
                },
                outlets=params["datasets_list_silver_step_2"],
            )

        if params["s3_folder_name"] == "corban":
            email_btg_empresas = PythonOperator(
                task_id=f"envio_email_novas_contas_btg_empresas",
                python_callable=envio_email_btg_empresas,
                op_kwargs={
                    "ano_particao": ano_particao,
                    "mes_particao": mes_particao,
                    "atualizacao": params["atualizacao"],
                    "s3_silver_step_1_path": params["s3_silver_step_1_path"],
                },
            )

        if params["s3_folder_name"] == "semanal":
            (
                confere_trigger_config
                >> verifica_semanal
                >> transfer_landing
                >> qualidade_landing
                >> move_para_bronze
                >> processa_silver
            )
            upstream.append(processa_silver)
            verifica_semanal >> skip_semanal
            upstream.append(skip_semanal)

        elif params["s3_folder_name"] in bases_historicas:
            (
                confere_trigger_config
                >> transfer_landing
                >> qualidade_landing
                >> move_para_bronze
                >> processa_silver
            )
            upstream.append(processa_silver)

        elif params["s3_folder_name"] == "corban":
            (
                confere_trigger_config
                >> transfer_landing
                >> qualidade_landing
                >> move_para_bronze
                >> move_para_silver
                >> processa_silver
            )
            move_para_silver >> processa_silver
            move_para_silver >> email_btg_empresas
            upstream.append(processa_silver)
        else:
            (
                confere_trigger_config
                >> transfer_landing
                >> qualidade_landing
                >> move_para_bronze
                >> move_para_silver
                >> processa_silver
            )
            upstream.append(processa_silver)

    task_consolidado_silver = PythonOperator(
        task_id=f"processa_consolidado_historico_silver",
        python_callable=processa_consolidado_historico_silver,
        op_kwargs={
            "ano_particao": ano_particao,
            "mes_particao": mes_particao,
            "bases_processadas": "{{ params.bases_processadas }}",
            "bases_historicas": bases_historicas,
        },
        trigger_rule="none_failed_min_one_success",
        outlets=params["datasets_list_silver_step_2"],
    )

    task_qualidade_consolidado_silver = PythonOperator(
        task_id=f"verifica_qualidade_consolidado_historico_silver",
        python_callable=qualidade_silver,
        op_kwargs={
            "ano_particao": ano_particao,
            "mes_particao": mes_particao,
        },
    )

    task_trigger_job_glue = PythonOperator(
        task_id=f"triger_glue_job_receita_gold",
        python_callable=trigger_glue,
        op_kwargs={
            "glue_job": "receita",
            "arguments": {
                "--ano_particao": f"ano-{ano_particao}",
                "--mes_particao": f"mes-{mes_particao}",
            },
        },
    )

    sensor_job_glue = GlueJobSensor(
        task_id="sensor_receita_gold_glue",
        job_name="receita",
        run_id="{{ ti.xcom_pull(key='receita_run_id', task_ids='triger_glue_job_receita_gold') }}",
        aws_conn_id="my_aws",
    )

    cria_base_aux_app = PythonOperator(
        task_id=f"processa_base_aux_aplicativo",
        python_callable=calcula_receita_consolidada_aplicativo,
        op_kwargs={
            "ano_particao": ano_particao,
            "mes_particao": mes_particao,
        },
    )

    (
        upstream
        >> task_consolidado_silver
        >> task_qualidade_consolidado_silver
        >> task_trigger_job_glue
        >> sensor_job_glue
        >> cria_base_aux_app
    )
