from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from datetime import datetime
from utils.msteams import msteams_task_failed
from utils.bucket_names import get_bucket_name
from utils.DagClusterizacaoClientesTreinamento.functions import (
    train_movimentacoes,
    train_nnm,
    train_aa,
    train_posicao,
)


# Define variáveis gerais
ano_particao = "{{ data_interval_end.year }}"
mes_particao = "{{ data_interval_end.month }}"
data_interval_end = "{{ data_interval_end.strftime('%Y%m%dT%H%M%S') }}"

# Define argumentos padrões
default_args = {
    "owner": "Eduardo Queiroz",
    "email": ["eduardo.queiroz@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2023, 11, 20),
    "retries": 0,
}


with DAG(
    "dag_clusterizacao_clientes_treinamento",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:
    task_train_nnm_model = PythonOperator(
        task_id="task_train_nnm_model",
        python_callable=train_nnm,
        op_kwargs={
            "date_interval_end": data_interval_end,
        },
    )

    task_train_mov_model = PythonOperator(
        task_id="task_train_mov_model",
        python_callable=train_movimentacoes,
        op_kwargs={
            "date_interval_end": data_interval_end,
        },
    )

    task_train_aa_model = PythonOperator(
        task_id="task_train_aa_model",
        python_callable=train_aa,
        op_kwargs={
            "date_interval_end": data_interval_end,
        },
    )

    task_train_posicao_model = PythonOperator(
        task_id="task_train_posicao_model",
        python_callable=train_posicao,
        op_kwargs={
            "date_interval_end": data_interval_end,
        },
    )
