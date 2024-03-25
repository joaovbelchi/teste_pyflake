from airflow import DAG
from utils.msteams import msteams_task_failed
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)
from utils.DagAtualizaNNM.functions import (
    atualiza_nnm_silver,
    atualiza_nnm_silver_com_offshore,
    atualiza_nnm_gold,
    verifica_qualidade_silver,
    atualiza_nnm_b2c,
)
from utils.datasets import (
    TB_NNM_BTG_GOLD,
    TB_NNM_HISTORICO_BTG_SILVER,
    TB_NNM_HISTORICO_BTG_SILVER_OFFSHORE,
    TB_NNM_B2C_BTG_BRONZE,
    TB_ACC_IN_OUT_BTG_BRONZE,
)
from utils.functions import parse_ts_dag_run, s3_to_onedrive
from utils.bucket_names import get_bucket_name
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable


class MyExternalTaskSensor(ExternalTaskSensor):
    template_fields = ["execution_delta", *ExternalTaskSensor.template_fields]


ex_delta = (
    "{{ macros.timedelta(seconds=ti.xcom_pull(task_ids='calcula_execution_delta')) }}"
)
ex_delta_posicao = "{{ macros.timedelta(seconds=ti.xcom_pull(task_ids='calcula_execution_delta_posicao')) }}"
guilherme_drive_id = "{{ var.value.get('guilherme_drive_id') }}"
timestamp_dagrun = "{{ parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger) }}"

default_args = {
    "owner": "Felipe Nogueira",
    "email": ["felipe.nogueira@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2023, 10, 29),
    "retries": 0,
    "timeout": 1800,
    "mode": "poke",
    "poke_interval": 20,
}


def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


def calcula_execution_delta_posicao(dag_run):
    if dag_run.external_trigger:
        return timedelta(hours=11, minutes=30).seconds
    return timedelta(hours=2, minutes=35).seconds


def calcula_execution_delta(dag_run):
    if dag_run.external_trigger:
        return timedelta(hours=8, minutes=55).seconds
    return timedelta(seconds=0).seconds


def determina_se_run_historica(params):
    if params.get("tipo_atualizacao") == "historica":
        return "sensor_dag_nnm_historico_btg"
    return "run_incremental"


def determina_se_run_planejada(dag_run, params):
    if (
        (params.get("upstream_dag") == "dag_ingestao_nnm_historico_btg")
        and not params.get("upstream_external_trigger")
    ) or not dag_run.external_trigger:
        return "run_planejada"
    return "run_manual"


with DAG(
    "dag_atualiza_nnm",
    default_args=default_args,
    schedule_interval="5 11,18,23 * * 1-5",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    params={"tipo_atualizacao": "incremental"},
    user_defined_macros={"parse_ts_dag_run": parse_ts_dag_run},
) as dag:
    branch_determina_se_run_planejada = BranchPythonOperator(
        task_id="determina_se_run_planejada", python_callable=determina_se_run_planejada
    )

    task_calcula_execution_delta = PythonOperator(
        task_id="calcula_execution_delta",
        python_callable=calcula_execution_delta,
    )

    task_calcula_execution_delta_posicao = PythonOperator(
        task_id="calcula_execution_delta_posicao",
        python_callable=calcula_execution_delta_posicao,
    )

    run_planejada = EmptyOperator(task_id="run_planejada")

    conector_run_planejada = EmptyOperator(
        task_id="conector_run_planejada", trigger_rule="none_failed_min_one_success"
    )

    run_manual = EmptyOperator(task_id="run_manual")

    sensor_task_movimentacao_bronze = MyExternalTaskSensor(
        task_id="sensor_task_movimentacao_bronze",
        external_dag_id="dag_movimentacao_btg",
        external_task_id="escrita_bronze_movimentacao",
        execution_delta=ex_delta,
        on_failure_callback=qualidade_task_failed,
    )

    sensor_task_posicao_silver = MyExternalTaskSensor(
        task_id="sensor_task_posicao_silver",
        external_dag_id="dag_posicao_btg",
        external_task_id="tratamento_posicao_silver",
        execution_delta=ex_delta_posicao,
        on_failure_callback=qualidade_task_failed,
        timeout=3600
    )

    sensor_dag_contas_incremental = MyExternalTaskSensor(
        task_id="sensor_task_contas_gold",
        external_dag_id="dag_contas_btg",
        external_task_id="escrita_gold_contas",
        execution_delta=ex_delta,
        on_failure_callback=qualidade_task_failed,
    )

    sensor_dag_nnm_historico_btg = MyExternalTaskSensor(
        task_id="sensor_dag_nnm_historico_btg",
        external_dag_id="dag_ingestao_nnm_historico_btg",
        execution_delta="{{ macros.timedelta(days=7) if not params.get('upstream_external_trigger') else macros.timedelta(days=0) }}",
        on_failure_callback=qualidade_task_failed,
    )

    branch_determina_se_run_historica = BranchPythonOperator(
        task_id="determina_se_run_historica",
        python_callable=determina_se_run_historica,
    )

    run_incremental = EmptyOperator(task_id="run_incremental")

    task_atualiza_nnm_b2c = PythonOperator(
        task_id="atualiza_nnm_b2c",
        python_callable=atualiza_nnm_b2c,
        op_kwargs={"timestamp_dagrun": timestamp_dagrun},
        outlets=[TB_NNM_B2C_BTG_BRONZE, TB_ACC_IN_OUT_BTG_BRONZE],
        trigger_rule="none_failed_min_one_success",
    )

    task_atualiza_silver = PythonOperator(
        task_id="atualiza_nnm_silver",
        python_callable=atualiza_nnm_silver,
        op_kwargs={
            "tipo_atualizacao": "{{ params.tipo_atualizacao }}",
            "data_interval_end": timestamp_dagrun,
            "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
        },
        outlets=[TB_NNM_HISTORICO_BTG_SILVER],
        trigger_rule="none_failed",
    )

    task_atualiza_silver_com_offshore = PythonOperator(
        task_id="atualiza_nnm_silver_com_offshore",
        op_kwargs={"drive_id": guilherme_drive_id},
        python_callable=atualiza_nnm_silver_com_offshore,
        outlets=[TB_NNM_HISTORICO_BTG_SILVER_OFFSHORE],
    )

    task_qualidade_silver = PythonOperator(
        task_id="qualidade_nnm_silver",
        python_callable=verifica_qualidade_silver,
        op_kwargs={
            "date_interval_end": timestamp_dagrun,
            "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
        },
    )

    task_atualiza_gold = PythonOperator(
        task_id="atualiza_nnm_gold",
        op_kwargs={"drive_id": guilherme_drive_id},
        python_callable=atualiza_nnm_gold,
        outlets=[TB_NNM_BTG_GOLD],
    )

    tasks_s3_to_onedrive = dict()
    for i in [["Histórico", "csv_pbi"], ["gestao_performance"]]:
        tasks_s3_to_onedrive[i[-1]] = PythonOperator(
            task_id=f"copia_nnm_gold_s3_para_onedrive_{i[-1]}",
            python_callable=s3_to_onedrive,
            op_kwargs={
                "s3_bucket": get_bucket_name("lake-gold"),
                "s3_obj_key": "api/btg/s3/nnm/NNM.parquet",
                "onedrive_drive_id": guilherme_drive_id,
                "onedrive_path": [
                    "One Analytics",
                    "Banco de dados",
                    *i,
                    "Net New Money Detalhado.csv",
                ],
            },
        )

    (
        [task_calcula_execution_delta, task_calcula_execution_delta_posicao]
        >> branch_determina_se_run_planejada
        >> [
            run_planejada,
            run_manual,
        ]
    )

    (
        run_planejada
        >> [
            sensor_task_posicao_silver,
            sensor_task_movimentacao_bronze,
            sensor_dag_contas_incremental,
        ]
        >> branch_determina_se_run_historica
        >> [sensor_dag_nnm_historico_btg, run_incremental]
        >> conector_run_planejada
    )

    (
        [conector_run_planejada, run_manual]
        >> task_atualiza_nnm_b2c
        >> task_atualiza_silver
        >> task_atualiza_silver_com_offshore
        >> task_qualidade_silver
        >> task_atualiza_gold
        >> list(tasks_s3_to_onedrive.values())
    )
