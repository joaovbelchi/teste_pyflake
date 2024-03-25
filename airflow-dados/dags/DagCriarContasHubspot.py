from airflow.models import DAG
from utils.msteams import msteams_task_failed
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from utils.DagCriarContasHubspot.functions import criar_contas_hubspot_func


default_args = {
    "owner": "Gustavo Sousa",
    "start_date": datetime(2023, 12, 6),
    "email": ["gustavo.sousa@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "retries": 0,
}

with DAG(
    dag_id="Dag_Criar_Contas_Hubspot",
    default_args=default_args,
    schedule_interval="55 18 * * 1-5",
    tags=[
        "pythonoperator",
        "dev",
        "dados",
        "one",
        "btg",
        "microsoft",
        "onedrive",
        "hubspot",
        "contas",
        "offshore",
    ],
    catchup=False,
    description="Processo que cria/modifica contas automaticamente no Hubspot",
) as dag:
    criar_contas_hubspot = PythonOperator(
        task_id="criar_contas_hub",
        python_callable=criar_contas_hubspot_func,
        provide_context=True,
        op_kwargs={"GUILHERME_DRIVE_ID": Variable.get("guilherme_drive_id")},
    )

criar_contas_hubspot
