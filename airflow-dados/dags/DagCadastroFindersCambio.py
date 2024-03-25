from airflow.models import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from utils.DagCadastroFindersCambio.functions import (
    recebe_resposta_lambda_hubspot,
    incrementa_base_cadastro_finder,
    incrementa_base_cadastro_indicacao_finder,
)
from utils.msteams import msteams_task_failed
from datetime import datetime


default_args = {
    "owner": "Gustavo Sousa",
    "email": ["gustavo.sousa@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2024, 2, 21),
    "params": {
        "evento_lambda": None,
        "datetime_arquivo": None,
        "id_arquivo": None,
    },
    "retries": 0,
}


with DAG(
    dag_id="Dag_Cadastro_Finders_Cambio",
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    render_template_as_native_obj=False,
    max_active_runs=1,
    max_active_tasks=2,
    description="Pipeline que recebe dados de um webhook do hubspot e aciona um pipeline de tratamento desses dados, gerando as bases necessárias para monitorar os finders de câmbio.",
) as dag:

    recebe_resposta_lambda = BranchPythonOperator(
        task_id="recebe_resposta_lambda",
        python_callable=recebe_resposta_lambda_hubspot,
        op_kwargs={"evento_lambda": "{{ params['evento_lambda']}}"},
    )
    fluxo_finder = PythonOperator(
        task_id="fluxo_finder",
        python_callable=incrementa_base_cadastro_finder,
        op_kwargs={
            "datetime_arquivo": "{{ params['datetime_arquivo'] }}",
            "id_arquivo": "{{ params['id_arquivo']}}",
        },
    )
    fluxo_indicacao = PythonOperator(
        task_id="fluxo_indicacao",
        python_callable=incrementa_base_cadastro_indicacao_finder,
        op_kwargs={
            "datetime_arquivo": "{{ params['datetime_arquivo'] }}",
            "id_arquivo": "{{ params['id_arquivo']}}",
        },
    )

recebe_resposta_lambda >> [fluxo_finder, fluxo_indicacao]
