from airflow import DAG
from airflow.models import Variable
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain

from utils.bucket_names import get_bucket_name
from utils.DagExtracaoOnedrive.functions import (
    verifica_arquivo_modificado_onedrive,
    transferencia_onedrive_para_s3,
    leitura_relatorios_run,
    enviar_email_base_desatualizada,
)

from utils.DagExtracaoOnedrive.schemas import (
    SCHEMA_SUGESTAO_FUNDOS,
    SCHEMA_SUGESTAO_CP,
    SCHEMA_TERMOS_NEGATIVOS,
    SCHEMA_OFERTAS_PUBLICAS,
    SCHEMA_ASSET_ALLOCATION,
    SCHEMA_DEBENTURES_ISENTAS,
    SCHEMA_DEBENTURES_NAO_ISENTAS,
    SCHEMA_CRI_CRA,
    SCHEMA_CREDITO_BANCARIO,
    SCHEMA_TITULO_PUBLICO,
)

from utils.functions import (
    verifica_qualidade_landing,
    leitura_excel_s3,
    transferencia_landing_para_bronze,
)

from utils.datasets import (
    SUGESTAO_FUNDOS_LANDING,
    SUGESTAO_CREDITO_CORPORATIVO,
    SUGESTAO_FUNDOS_BRONZE,
    OFERTAS_PUBLICAS,
    TERMOS_NEGATIVOS,
    ASSET_ALLOCATION_REFERENCIA_LANDING,
    ASSET_ALLOCATION_REFERENCIA_BRONZE,
    RUN_CREDITO_CORPORATIVO,
    RUN_DEBENTURES_ISENTAS,
    RUN_DEBENTURES_NAO_ISENTAS,
    RUN_CRI_CRA,
    RUN_CREDITO_BANCARIO_LANDING,
    RUN_CREDITO_BANCARIO,
    RUN_TITULO_PUBLICO,
)
from datetime import datetime
import unicodedata
from utils.msteams import msteams_task_failed


def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


default_args = {
    "owner": "Nathalia Montandon",
    "email": ["nathalia.montandon@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2023, 10, 25),
    "retries": 0,
    "timeout": 3600,
    "mode": "reschedule",
}

s3_bucket_landing = get_bucket_name("lake-landing")
s3_bucket_bronze = get_bucket_name("lake-bronze")

# As chaves dos dicionários dataset_list_bronze e schema devem ser iguais para uma mesma base ou aba.
# Caso seja referente a aba de uma planilha a chave deverá ser o nome desta aba.
table_params = [
    {
        "onedrive_file_name": "Sugestão Fundos.xlsx",
        "onedrive_file_extension": ".xlsx",
        "s3_folder_name": "sugestoes_de_ativos",
        "s3_folder_prefix": "one/sugestao_de_realocacao",
        "datasets_list_landing": [SUGESTAO_FUNDOS_LANDING],
        "datasets_list_bronze": {
            "Fundos": SUGESTAO_FUNDOS_BRONZE,
            "Sugestão CPs": SUGESTAO_CREDITO_CORPORATIVO,
            "Termos Negativos": TERMOS_NEGATIVOS,
            "Ofertas Públicas": OFERTAS_PUBLICAS,
        },
        "schema": {
            "Fundos": SCHEMA_SUGESTAO_FUNDOS,
            "Sugestão CPs": SCHEMA_SUGESTAO_CP,
            "Termos Negativos": SCHEMA_TERMOS_NEGATIVOS,
            "Ofertas Públicas": SCHEMA_OFERTAS_PUBLICAS,
        },
        "onedrive_path_prefix": [
            "One Analytics",
            "Renda Fixa",
            "13 - Planilhas de Atualização",
        ],
        "funcao_leitura_landing": leitura_excel_s3,
        "days_dif": 15,
        "destinatarios": [
            "vitor.oliveira@investimentos.one",
            "luis.martins@investimentos.one",
            "nathalia.montandon@investimentos.one",
        ],
        "timeout": 60,
    },
    {
        "onedrive_file_name": "Asset_Allocation.xlsx",
        "onedrive_file_extension": ".xlsx",
        "s3_folder_name": "referencia_asset_allocation",
        "s3_folder_prefix": "one/sugestao_de_realocacao",
        "datasets_list_landing": [ASSET_ALLOCATION_REFERENCIA_LANDING],
        "datasets_list_bronze": {
            "Asset Allocation": ASSET_ALLOCATION_REFERENCIA_BRONZE
        },
        "schema": {"Asset Allocation": SCHEMA_ASSET_ALLOCATION},
        "onedrive_path_prefix": [
            "One Analytics",
            "Renda Fixa",
            "13 - Planilhas de Atualização",
        ],
        "funcao_leitura_landing": leitura_excel_s3,
        "days_dif": 15,
        "destinatarios": [
            "vitor.oliveira@investimentos.one",
            "luis.martins@investimentos.one",
            "nathalia.montandon@investimentos.one",
        ],
        "timeout": 60,
    },
    {
        "onedrive_file_name": "RUN Crédito Corporativo.xlsm",
        "onedrive_file_extension": ".xlsm",
        "s3_folder_name": "credito_corporativo",
        "s3_folder_prefix": "one/run",
        "datasets_list_landing": [RUN_CREDITO_CORPORATIVO],
        "datasets_list_bronze": {
            "Debentures Isentas": RUN_DEBENTURES_ISENTAS,
            "Debentures Não Isentas": RUN_DEBENTURES_NAO_ISENTAS,
            "CRICRA": RUN_CRI_CRA,
        },
        "schema": {
            "Debentures Isentas": SCHEMA_DEBENTURES_ISENTAS,
            "Debentures Não Isentas": SCHEMA_DEBENTURES_NAO_ISENTAS,
            "CRICRA": SCHEMA_CRI_CRA,
        },
        "onedrive_path_prefix": [
            "One Analytics",
            "Banco de dados - Investimentos",
            "Novos",
        ],
        "funcao_leitura_landing": leitura_relatorios_run,
        "days_dif": 0,
    },
    {
        "onedrive_file_name": "RUN Bancário e Tít. Públicos.xlsm",
        "onedrive_file_extension": ".xlsm",
        "s3_folder_name": "credito_bancario",
        "s3_folder_prefix": "one/run",
        "datasets_list_landing": [RUN_CREDITO_BANCARIO_LANDING],
        "datasets_list_bronze": {
            "Crédito Bancário": RUN_CREDITO_BANCARIO,
            "Título Público": RUN_TITULO_PUBLICO,
        },
        "schema": {
            "Crédito Bancário": SCHEMA_CREDITO_BANCARIO,
            "Título Público": SCHEMA_TITULO_PUBLICO,
        },
        "onedrive_path_prefix": [
            "One Analytics",
            "Banco de dados - Investimentos",
            "Novos",
        ],
        "funcao_leitura_landing": leitura_relatorios_run,
        "days_dif": 0,
    },
]

ano_particao = "{{ data_interval_end.year }}"
mes_particao = "{{ data_interval_end.month }}"
data_interval_end = "{{ data_interval_end.strftime('%Y%m%dT%H%M%S') }}"

with DAG(
    "Dag_Extracao_Onedrive",
    default_args=default_args,
    schedule_interval="15 15 * * 1-5",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:
    for params in table_params:
        s3_table_key = f"{params['s3_folder_prefix']}/{params['s3_folder_name']}"
        s3_partitions_key = f"ano_particao={ano_particao}/mes_particao={mes_particao}"
        s3_filename = f"{params['s3_folder_name']}_{data_interval_end}{params['onedrive_file_extension']}"
        s3_object_key = f"{s3_table_key}/{s3_partitions_key}/{s3_filename}"

        sensor_onedrive = PythonSensor(
            task_id=f"sensor_{params['s3_folder_name']}_onedrive",
            python_callable=verifica_arquivo_modificado_onedrive,
            timeout=(
                params["timeout"]
                if "timeout" in params.keys()
                else default_args["timeout"]
            ),
            op_kwargs={
                "drive_id": "{{ var.value.get('guilherme_drive_id') }}",
                "onedrive_path": [
                    *params["onedrive_path_prefix"],
                    params["onedrive_file_name"],
                ],
                "days_dif": params["days_dif"],
            },
            on_failure_callback=qualidade_task_failed,
        )

        # Enviar o e-mail caso a primeira task tenha falhado (trigger_rule="all_failed")
        operator_send_email = PythonOperator(
            task_id=f"operator_{params['s3_folder_name']}_send_email",
            trigger_rule="all_failed",
            python_callable=enviar_email_base_desatualizada,
            op_kwargs={
                "drive_id": "{{ var.value.get('guilherme_drive_id') }}",
                "item_id": "d64bcd7f-c47a-4343-b0b4-73f4a8e927a5",  # tech@investimentos.one
                "planilha": params["onedrive_file_name"],
                "days_dif": params["days_dif"],
                "destinatarios": (
                    params["destinatarios"] if "destinatarios" in params.keys() else []
                ),
            },
        )

        transfer_landing = PythonOperator(
            task_id=f"transfer_{params['s3_folder_name']}_landing",
            python_callable=transferencia_onedrive_para_s3,
            op_kwargs={
                "drive_id": "{{ var.value.get('guilherme_drive_id') }}",
                "onedrive_path": [
                    *params["onedrive_path_prefix"],
                    params["onedrive_file_name"],
                ],
                "s3_bucket": s3_bucket_landing,
                "s3_object_key": f"{s3_object_key}",
            },
            outlets=params["datasets_list_landing"],
        )

        qualidade_landing = []
        move_para_bronze = []
        for sheet_name, dataset in params["datasets_list_bronze"].items():
            dataset_name = sheet_name.replace(" ", "_")
            dataset_name = dataset_name.lower()
            dataset_name = (
                unicodedata.normalize("NFKD", dataset_name)
                .encode("ASCII", "ignore")
                .decode("utf-8")
            )
            schema = params["schema"][sheet_name]

            # Caso a lista de datasets da bronze tenha apenas 1 dataset,
            # isso indica que não é necessário especificar a aba que deverá ser salva na bronze
            if len(params["datasets_list_bronze"]) == 1:
                sheet_name = 0
                s3_table_key_path = s3_table_key

            # Caso tenha mais de 1 aba, é necessário especificar o nome da aba,
            # para que não substitua a aba anterior
            else:
                s3_table_key_path = f"{s3_table_key}/{dataset_name}"

            locals()["qualidade_landing_" + str(dataset_name)] = PythonOperator(
                task_id=f"qualidade_{dataset_name}_landing",
                python_callable=verifica_qualidade_landing,
                op_kwargs={
                    "s3_bucket": s3_bucket_landing,
                    "s3_table_key": s3_table_key,
                    "s3_partitions_key": s3_partitions_key,
                    "s3_filename": f"{s3_filename}",
                    "schema": schema,
                    "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
                    "funcao_leitura_landing": params["funcao_leitura_landing"],
                    "kwargs_funcao_leitura_landing": {
                        "s3_bucket_landing": s3_bucket_landing,
                        "s3_object_key": f"{s3_object_key}",
                        "sheet_name": sheet_name,
                        "skiprows": params.get("skiprows"),
                    },
                },
                on_failure_callback=qualidade_task_failed,
            )

            qualidade_landing += [locals()["qualidade_landing_" + str(dataset_name)]]

            locals()["move_para_bronze_" + str(dataset_name)] = PythonOperator(
                task_id=f"transfer_{dataset_name}_bronze",
                python_callable=transferencia_landing_para_bronze,
                op_kwargs={
                    "s3_bucket_bronze": s3_bucket_bronze,
                    "s3_table_key": s3_table_key_path,
                    "s3_partitions_key": s3_partitions_key,
                    "schema": schema,
                    "data_interval_end": data_interval_end,
                    "funcao_leitura_landing": params["funcao_leitura_landing"],
                    "kwargs_funcao_leitura_landing": {
                        "s3_bucket_landing": s3_bucket_landing,
                        "s3_object_key": f"{s3_object_key}",
                        "sheet_name": sheet_name,
                        "skiprows": params.get("skiprows"),
                    },
                },
                outlets=dataset,
            )

            move_para_bronze += [locals()["move_para_bronze_" + str(dataset_name)]]

        # Estou usando chain, pois a DAG possui duas listas de tasks sequenciais
        # https://docs.astronomer.io/learn/managing-dependencies
        sensor_onedrive >> transfer_landing

        chain(
            transfer_landing,
            qualidade_landing,
            move_para_bronze,
        )

        sensor_onedrive >> operator_send_email
