from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from utils.DagMonitoramentoAplicacoesRf.functions import (
    gera_base_movimentacoes_relevantes_rf_drive,
    gera_base_movimentacoes_relevantes_rf_lake,
    envia_emails_aplicacoes_relevantes_advisors,
)
from datetime import datetime
from utils.msteams import msteams_task_failed
from datetime import timedelta

from utils.functions import parse_ts_dag_run

default_args = {
    "owner": "Gustavo Sousa",
    "start_date": datetime(2023, 10, 25),
    "on_failure_callback": msteams_task_failed,
    "email": ["gustavo.sousa@investimentos.one"],
    "retries": 0,
}


def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


def tratar_data_interval_end(data_interval_end):
    return datetime.strptime(data_interval_end[:8], "%Y%m%d")


with DAG(
    dag_id="Dag_Monitoramento_Aplicacaoes_Rf",
    default_args=default_args,
    schedule_interval="15 23 * * 1-5",
    tags=["rf", "monitoramento", "advisors"],
    catchup=False,
    description="Pipeline para atualizar monitorar aplicações relevantes de renda fixa e aletar advisors por e-mail",
    user_defined_macros={
        "parse_ts_dag_run": parse_ts_dag_run,
        "tratar_data_interval_end": tratar_data_interval_end,
    },
    render_template_as_native_obj=True,
    max_active_runs=1,
    max_active_tasks=3,
) as dag:
    dag_data_interval_end = "{{ tratar_data_interval_end(parse_ts_dag_run(dag_run_id=run_id, data_interval_end=data_interval_end, external_trigger=dag_run.external_trigger)) }}"

    verifica_task_atualiza_movimentacao_gold = ExternalTaskSensor(
        task_id="verifica_task_atualiza_movimentacao_gold",
        external_dag_id="dag_movimentacao_btg",
        external_task_id="escrita_silver_movimentacao",
        execution_delta=timedelta(minutes=10),
        on_failure_callback=qualidade_task_failed,
        timeout=1800,
        mode="poke",
        poke_interval=60,
    )
    verifica_task_atualizacao_posicao_tratada_silver = ExternalTaskSensor(
        task_id="verifica_task_atualizacao_posicao_tratada_silver",
        external_dag_id="dag_posicao_btg",
        external_task_id="tratamento_posicao_silver",
        execution_delta=timedelta(hours=2, minutes=45),
        on_failure_callback=qualidade_task_failed,
        timeout=1800,
        mode="poke",
        poke_interval=60,
    )
    verifica_task_atualizacao_clientes_assessores_gold = ExternalTaskSensor(
        task_id="verifica_task_atualizacao_clientes_assessores_gold",
        external_dag_id="Dag_ClientesAssessores",
        external_task_id="dag_clientes_assessores",
        execution_delta=timedelta(hours=14, minutes=30),
        on_failure_callback=qualidade_task_failed,
        timeout=1800,
        mode="poke",
        poke_interval=60,
    )
    gerar_base_emails_aplicacacoes_relevantes_rf_lake = PythonOperator(
        task_id="gerar_base_emails_aplicacacoes_relevantes_rf_lake",
        python_callable=gera_base_movimentacoes_relevantes_rf_lake,
        op_kwargs={"dag_data_interval_end": dag_data_interval_end},
    )
    gerar_base_emails_aplicacacoes_relevantes_rf_drive = PythonOperator(
        task_id="gerar_base_emails_aplicacacoes_relevantes_rf_drive",
        python_callable=gera_base_movimentacoes_relevantes_rf_drive,
    )
    enviar_emails_aplicacoes_relevantes_advisors = PythonOperator(
        task_id="envia_emails_aplicacoes_relevantes_advisors",
        python_callable=envia_emails_aplicacoes_relevantes_advisors,
        op_kwargs={"dag_data_interval_end": dag_data_interval_end},
    )

(
    [
        verifica_task_atualiza_movimentacao_gold,
        verifica_task_atualizacao_posicao_tratada_silver,
        verifica_task_atualizacao_clientes_assessores_gold,
    ]
    >> gerar_base_emails_aplicacacoes_relevantes_rf_lake
    >> [
        gerar_base_emails_aplicacacoes_relevantes_rf_drive,
        enviar_emails_aplicacoes_relevantes_advisors,
    ]
)
