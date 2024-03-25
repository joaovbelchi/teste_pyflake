from airflow import DAG
from datetime import datetime
from utils.msteams import msteams_task_failed
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from utils.DagSaldoEmConta.functions import (
    notifica_saldo_em_conta_func,
    verifica_atualizacao_arquivo_saldos,
    atualiza_saldo_em_conta_historico,
)


default_args = {
    "owner": "Gustavo Sousa",
    "start_date": datetime(2023, 12, 6),
    "email": ["gustavo.sousa@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "retries": 0,
}

with DAG(
    dag_id="Dag_Saldo_em_Conta",
    default_args=default_args,
    schedule_interval="0 13,18 * * 1-5",
    tags=[
        "pythonoperator",
        "dev",
        "dados",
        "one",
        "btg",
        "email",
        "microsoft",
        "contas",
        "saldo_em_conta",
        "comerciais",
        "advisors",
        "backoffice",
    ],
    catchup=False,
    description="Pipeline para envio do e-mail de saldo em conta",
    max_active_runs=1,
) as dag:
    sensor_planilha_saldos = PythonSensor(
        task_id="sensor_planilha_saldos",
        python_callable=verifica_atualizacao_arquivo_saldos,
        on_failure_callback=msteams_task_failed,
        mode="poke",
        poke_interval=600,
    )
    atualiza_saldo_historico = PythonOperator(
        task_id="atualiza_saldo_historico",
        python_callable=atualiza_saldo_em_conta_historico,
        on_failure_callback=msteams_task_failed,
    )
    saldo_em_conta = PythonOperator(
        task_id="saldo_em_conta",
        python_callable=notifica_saldo_em_conta_func,
    )

sensor_planilha_saldos >> atualiza_saldo_historico >> saldo_em_conta
