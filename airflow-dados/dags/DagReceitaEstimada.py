from airflow import DAG
from airflow.models import Variable
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.operators.empty import EmptyOperator

from datetime import datetime

from utils.datasets import RECEITA_ESTIMADA

from utils.msteams import msteams_task_failed
from utils.DagExtracaoOnedrive.functions import verifica_arquivo_modificado_onedrive


# from utils.DagReceitaEstimada.RendaFixa.main import renda_fixa
from utils.DagReceitaEstimada.Cambio.cambio import cambio
from utils.DagReceitaEstimada.Estruturadas.estruturadas import estruturadas
from utils.DagReceitaEstimada.Fundos.fundos import fundos
from utils.DagReceitaEstimada.OfertaPublica.oferta_publica import oferta_publica
from utils.DagReceitaEstimada.Previdencia.previdencia import previdencia
from utils.DagReceitaEstimada.RendaFixa.renda_fixa import renda_fixa
from utils.DagReceitaEstimada.RendaVariavel.renda_variavel import renda_variavel
from utils.DagReceitaEstimada.TradesInternosRF.trades_internos_rf import (
    trades_internos_rf,
)
from utils.DagReceitaEstimada.TradesInternosRV.trades_internos_rv import (
    trades_internos_rv,
)
from utils.DagReceitaEstimada.Validacao.validacao import validacao
from utils.functions import trigger_glue


def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


drive_id = "{{ var.value.get('guilherme_drive_id') }}"

default_args = {
    "owner": "Nathalia Montandon",
    "email": ["nathalia.montandon@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2023, 10, 25),
    "retries": 0,
    "timeout": 3600,
    "mode": "reschedule",
}

with DAG(
    "Dag_Receita_Estimada",
    default_args=default_args,
    schedule_interval="30 13 * * 1-5",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    render_template_as_native_obj=True,
) as dag:
    sensor_onedrive = PythonSensor(
        task_id=f"sensor_movimentacao_onedrive",
        python_callable=verifica_arquivo_modificado_onedrive,
        op_kwargs={
            "drive_id": drive_id,
            "onedrive_path": [
                "One Analytics",
                "Banco de dados",
                "Histórico",
                "movimentacao.csv",
            ],
        },
        on_failure_callback=qualidade_task_failed,
    )

    # Câmbio
    operator_cambio = PythonOperator(
        task_id="operator_cambio",
        python_callable=cambio,
        op_kwargs={"drive_id": drive_id},
    )

    # Estruturadas
    operator_estruturadas = PythonOperator(
        task_id="operator_estruturadas",
        python_callable=estruturadas,
        op_kwargs={"drive_id": drive_id},
    )

    # Fundos
    operator_fundos = PythonOperator(
        task_id="operator_fundos",
        python_callable=fundos,
        op_kwargs={"drive_id": drive_id},
    )

    # Oferta Pública
    operator_oferta_publica = PythonOperator(
        task_id="operator_oferta_publica",
        python_callable=oferta_publica,
        op_kwargs={"drive_id": drive_id},
    )

    # Previdência
    operator_previdencia = PythonOperator(
        task_id="operator_previdencia",
        python_callable=previdencia,
        op_kwargs={"drive_id": drive_id},
    )

    # Renda Fixa
    operator_renda_fixa = PythonOperator(
        task_id="operator_renda_fixa",
        python_callable=renda_fixa,
        op_kwargs={"drive_id": drive_id},
    )

    # Renda Variável
    operator_renda_variavel = PythonOperator(
        task_id="operator_renda_variavel",
        python_callable=renda_variavel,
        op_kwargs={"drive_id": drive_id},
    )

    # Trades Internos RF
    operator_trades_internos_rf = PythonOperator(
        task_id="operator_trades_internos_rf",
        python_callable=trades_internos_rf,
        op_kwargs={"drive_id": drive_id},
    )

    # Trades Internos RV
    operator_trades_internos_rv = PythonOperator(
        task_id="operator_trades_internos_rv",
        python_callable=trades_internos_rv,
        op_kwargs={"drive_id": drive_id},
    )

    # Receita Estimada
    operator_receita_estimada = PythonOperator(
        task_id="operator_receita_estimada",
        python_callable=trigger_glue,
        op_kwargs={
            "arguments": {"--drive_id": drive_id},
            "glue_job": "receita_estimada_python",
        },
    )

    # As variáveis abaixo devem ser igual ao nome do Job no Glue
    # glue_job = job_name = key{_run_id}
    sensor_receita_estimada_glue = GlueJobSensor(
        task_id="sensor_receita_estimada_glue",
        job_name="receita_estimada_python",
        run_id="{{ ti.xcom_pull(key='receita_estimada_python_run_id', task_ids='operator_receita_estimada') }}",
        aws_conn_id="my_aws",
    )

    # Validação
    operator_validacao = PythonOperator(
        task_id="operator_validacao",
        python_callable=validacao,
        op_kwargs={"drive_id": drive_id},
    )

    # Dataset
    dataset_receita_estimada = EmptyOperator(
        task_id="dataset_receita_estimada", outlets=[RECEITA_ESTIMADA]
    )

    (
        sensor_onedrive
        >> [
            operator_cambio,
            operator_estruturadas,
            operator_fundos,
            operator_oferta_publica,
            operator_previdencia,
            operator_renda_fixa,
            operator_renda_variavel,
            operator_trades_internos_rf,
            operator_trades_internos_rv,
        ]
        >> operator_receita_estimada
        >> sensor_receita_estimada_glue
        >> operator_validacao
        >> dataset_receita_estimada
    )
