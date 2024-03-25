from utils.msteams import msteams_task_failed
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from utils.DagPipePosicaoBTG.functions import (
    download_posicao_escritorio,
    leitura_posicao_escritorio,
    download_posicoes_por_conta,
    consolida_posicao_silver,
    tratamento_posicao_silver,
    PL_silver,
    PL_gold,
    validacoes_qualidade_landing_posicao,
    posicao_gold,
    posicao_dia_a_dia_gold,
    define_modo_execucao,
    posicao_auxiliar_receita,
    auc_dia_dia_completo,
    atualiza_auc_contas_crm,
)
from utils.bucket_names import get_bucket_name
from utils.functions import (
    verifica_qualidade_landing,
    transferencia_landing_para_bronze,
    s3_to_onedrive,
)
from utils.DagPipePosicaoBTG.schemas import SCHEMA_BTG_POSICAO_ESCRITORIO


def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


default_args = {
    "owner": "Felipe Nogueira",
    "email": ["felipe.nogueira@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2023, 12, 26),
    "retries": 0,
}

ano_particao = "{{ data_interval_end.year }}"
mes_particao = "{{ data_interval_end.month }}"
data_interval_end = "{{ data_interval_end.strftime('%Y%m%dT%H%M%S') }}"
modo_execucao = "{{ ti.xcom_pull(task_ids='define_modo_execucao') }}"
data_atualizacao = "{{ ti.xcom_pull(task_ids='consolida_posicao_silver') }}"

landing_obj_partition_key = f"ano_particao={ano_particao}/mes_particao={mes_particao}"
extension = ".zip"

params_dict = {
    "posicao_escritorio": {"func_download": download_posicao_escritorio},
    "posicao_por_conta": {"func_download": download_posicoes_por_conta},
}
tasks_dict = {key: {} for key in params_dict.keys()}

with DAG(
    "dag_posicao_btg",
    default_args=default_args,
    schedule_interval="30 8,15,20 * * 1-5",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    render_template_as_native_obj=True,
) as dag:
    for table_name, params in params_dict.items():
        landing_obj_table_key = f"btg/api/{table_name}"
        landing_obj_filename = f"{table_name}_{data_interval_end}{extension}"
        landing_obj_key = f"{landing_obj_table_key}/{landing_obj_partition_key}/{landing_obj_filename}"
        landing_obj_filename_no_extension = ".".join(
            landing_obj_filename.split(".")[:-1]
        )

        if table_name == "posicao_escritorio":
            op_kwargs = {"s3_obj_key": landing_obj_key}
        elif table_name == "posicao_por_conta":
            op_kwargs = {
                "s3_obj_key": landing_obj_key,
                "timestamp_dagrun": data_interval_end,
                "modo_execucao": modo_execucao,
            }

        tasks_dict[table_name]["download_landing"] = PythonOperator(
            task_id=f"download_{table_name}",
            python_callable=params["func_download"],
            op_kwargs=op_kwargs,
            trigger_rule=(
                "none_failed_min_one_success"
                if table_name == "posicao_por_conta"
                else "all_success"
            ),
        )

        tasks_dict[table_name]["qualidade_schema_landing"] = PythonOperator(
            task_id=f"qualidade_schema_landing_{table_name}",
            python_callable=verifica_qualidade_landing,
            op_kwargs={
                "s3_bucket": get_bucket_name("lake-landing"),
                "s3_table_key": landing_obj_table_key,
                "s3_partitions_key": landing_obj_partition_key,
                "s3_filename": landing_obj_filename,
                "schema": SCHEMA_BTG_POSICAO_ESCRITORIO,
                "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
                "funcao_leitura_landing": leitura_posicao_escritorio,
                "kwargs_funcao_leitura_landing": {
                    "s3_bucket_landing": get_bucket_name("lake-landing"),
                    "s3_object_key": landing_obj_key,
                },
            },
            on_failure_callback=qualidade_task_failed,
        )

        tasks_dict[table_name]["qualidade_landing"] = PythonOperator(
            task_id=f"qualidade_landing_{table_name}",
            python_callable=validacoes_qualidade_landing_posicao,
            op_kwargs={
                "s3_bucket": get_bucket_name("lake-landing"),
                "s3_table_key": landing_obj_table_key,
                "s3_partitions_key": landing_obj_partition_key,
                "s3_filename": landing_obj_filename_no_extension,
                "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
            },
            on_failure_callback=qualidade_task_failed,
        )

        tasks_dict[table_name]["escrita_bronze"] = PythonOperator(
            task_id=f"escrita_bronze_{table_name}",
            python_callable=transferencia_landing_para_bronze,
            op_kwargs={
                "s3_bucket_bronze": get_bucket_name("lake-bronze"),
                "s3_table_key": landing_obj_table_key,
                "s3_partitions_key": landing_obj_partition_key,
                "schema": SCHEMA_BTG_POSICAO_ESCRITORIO,
                "data_interval_end": data_interval_end,
                "funcao_leitura_landing": leitura_posicao_escritorio,
                "kwargs_funcao_leitura_landing": {
                    "s3_bucket_landing": get_bucket_name("lake-landing"),
                    "s3_object_key": landing_obj_key,
                },
            },
        )

        (
            tasks_dict[table_name]["download_landing"]
            >> tasks_dict[table_name]["qualidade_schema_landing"]
            >> tasks_dict[table_name]["qualidade_landing"]
            >> tasks_dict[table_name]["escrita_bronze"]
        )

    branch_define_modo_execucao = BranchPythonOperator(
        task_id="define_modo_execucao",
        python_callable=define_modo_execucao,
        op_kwargs={"dt_interval_end": data_interval_end},
    )

    empty_execucao_normal = EmptyOperator(task_id="execucao_normal")

    empty_fechamento_mes_anterior = EmptyOperator(task_id="fechamento_mes_anterior")

    task_posicao_consolidada_silver = PythonOperator(
        task_id="consolida_posicao_silver",
        python_callable=consolida_posicao_silver,
        op_kwargs={
            "timestamp_dagrun": data_interval_end,
            "modo_execucao": modo_execucao,
        },
    )

    task_posicao_tratada_silver = PythonOperator(
        task_id="tratamento_posicao_silver",
        python_callable=tratamento_posicao_silver,
        op_kwargs={
            "timestamp_dagrun": data_interval_end,
            "data_atualizacao": data_atualizacao,
        },
    )

    task_posicao_gold = PythonOperator(
        task_id="posicao_gold",
        python_callable=posicao_gold,
        op_kwargs={"timestamp_dagrun": data_interval_end},
    )

    task_posicao_dia_a_dia_gold = PythonOperator(
        task_id="posicao_dia_a_dia_gold",
        python_callable=posicao_dia_a_dia_gold,
        op_kwargs={"timestamp_dagrun": data_interval_end},
    )

    task_auc_dia_dia = PythonOperator(
        task_id="atualizacao_auc_dia_dia",
        python_callable=auc_dia_dia_completo,
        op_kwargs={"timestamp_dagrun": data_interval_end},
    )

    task_PL_silver = PythonOperator(
        task_id="PL_silver",
        python_callable=PL_silver,
        op_kwargs={
            "timestamp_dagrun": data_interval_end,
            "drive_id": "{{ var.value.get('guilherme_drive_id') }}",
            "data_atualizacao": data_atualizacao,
        },
    )

    task_PL_gold = PythonOperator(
        task_id="PL_gold",
        python_callable=PL_gold,
    )
    task_atualizacao_auc_crm = PythonOperator(
        task_id="atualizacao_auc_crm",
        python_callable=atualiza_auc_contas_crm,
        op_kwargs={
            "timestamp_dagrun": data_interval_end,
        },
    )
    task_s3_to_onedrive = PythonOperator(
        task_id=f"copia_PL_s3_para_onedrive",
        python_callable=s3_to_onedrive,
        op_kwargs={
            "s3_bucket": get_bucket_name("lake-gold"),
            "s3_obj_key": "btg/api/PL/PL.parquet",
            "onedrive_drive_id": "{{ var.value.get('guilherme_drive_id') }}",
            "onedrive_path": [
                "One Analytics",
                "Banco de dados",
                "Histórico",
                "csv_pbi",
                "PL.csv",
            ],
        },
    )

    task_base_posicao_aux_receita = PythonOperator(
        task_id=f"base_posicao_aux_receita",
        python_callable=posicao_auxiliar_receita,
    )

    branch_define_modo_execucao >> [
        empty_execucao_normal,
        empty_fechamento_mes_anterior,
    ]

    empty_execucao_normal >> tasks_dict["posicao_escritorio"]["download_landing"]

    [
        tasks_dict["posicao_escritorio"]["escrita_bronze"],
        empty_fechamento_mes_anterior,
    ] >> tasks_dict["posicao_por_conta"]["download_landing"]

    (
        tasks_dict["posicao_por_conta"]["escrita_bronze"]
        >> task_posicao_consolidada_silver
        >> task_posicao_tratada_silver
        >> task_PL_silver
        >> task_PL_gold
        >> task_s3_to_onedrive
    )

    task_posicao_tratada_silver >> task_posicao_gold >> task_posicao_dia_a_dia_gold
    task_posicao_gold >> task_base_posicao_aux_receita
    task_PL_gold >> task_auc_dia_dia >> task_atualizacao_auc_crm
