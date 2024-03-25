from airflow import DAG
from datetime import datetime
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from utils.msteams import msteams_task_failed

from utils.datasets import (
    COMPANIES,
    CONTACTS_AND_COMPANIES,
    INDICACAO,
    FREQ_CONTATO,
    CORPORATE_SOW,
    BASE_EMAIL_MARKETING,
    CLIENTES_ASSESSORES,
    CLIENTES_ASSESSORES_NOVOS,
    CONTAS_HISTORICO_CP,
    ADVISORS_NO_DUP_CP,
    CONTAS_HISTORICO,
)
from airflow.operators.empty import EmptyOperator


def failure_callback_func(context):
    msteams_task_failed(context)


default_args = {
    "owner": "Eduardo Queiroz",
    "start_date": datetime(2023, 10, 25),
    "email": ["eduardo.queiroz@investimentos.one"],
    "on_failure_callback": failure_callback_func,
    "retries": 0,
    "kubernetes_conn_id": "con_eks_dados",
    "namespace": "processing",
}

with DAG(
    dag_id="Dag_ClientesAssessores",
    default_args=default_args,
    schedule_interval="45 8 * * 1-5",
    tags=[
        "pythonoperator",
        "dev",
        "dados",
        "one",
        "hubspot",
        "email",
        "microsoft",
        "contas",
        "socios",
        "silver",
        "gold",
        "ClientesAssessores",
    ],
    catchup=False,
    description="Pipeline para atualizar ClientesAssessores",
    max_active_runs=1,
) as dag:
    # Task para extrair objetos e associações do Hubspot
    objetos_e_associacoes = SparkKubernetesOperator(
        task_id="dag_objetos_e_associacoes",
        params=dict(
            app_name="one-hubspot-objetos-e-associacoes",
            mainApplicationFile="local:///app/one-hubspot-objetos-e-associacoes.py",
        ),
        application_file="ConfigSpark6G.yaml",
        do_xcom_push=True,
    )

    monitor_objetos_e_associacoes = (
        SparkKubernetesSensor(
            task_id="monitor_objetos_e_associacoes",
            attach_log=True,
            application_name="{{ task_instance.xcom_pull(task_ids='dag_objetos_e_associacoes')['metadata']['name'] }}",
        ),
    )

    dataset_objetos_e_associacoes = EmptyOperator(
        task_id="dataset_objetos_e_associacoes",
        outlets=[COMPANIES, CONTACTS_AND_COMPANIES],
    )

    # Task para atualizar indicações e frequência de contato
    indicacoes_e_frequencia = SparkKubernetesOperator(
        task_id="dag_indicacoes_e_frequencia",
        params=dict(
            app_name="one-hubspot-indicacao-frequencia",
            mainApplicationFile="local:///app/one-hubspot-indicacao-frequencia.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_indicacoes_e_frequencia = (
        SparkKubernetesSensor(
            task_id="monitor_indicacoes_e_frequencia",
            attach_log=True,
            application_name="{{ task_instance.xcom_pull(task_ids='dag_indicacoes_e_frequencia')['metadata']['name'] }}",
        ),
    )

    dataset_indicacoes_e_frequencia = EmptyOperator(
        task_id="dataset_indicacoes_e_frequencia", outlets=[INDICACAO, FREQ_CONTATO]
    )

    # Task para atualizar clientes corporate (corporate_sow)
    corporate = SparkKubernetesOperator(
        task_id="dag_corporate",
        params=dict(
            app_name="one-hubspot-corporate",
            mainApplicationFile="local:///app/one-hubspot-corporate-sow.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_corporate = (
        SparkKubernetesSensor(
            task_id="monitor_corporate",
            attach_log=True,
            application_name="{{ task_instance.xcom_pull(task_ids='dag_corporate')['metadata']['name'] }}",
        ),
    )

    dataset_corporate = EmptyOperator(
        task_id="dataset_corporate", outlets=[CORPORATE_SOW]
    )

    # Task para atualizar clientes E-mail Marketing (corporate_sow)
    email_marketing = SparkKubernetesOperator(
        task_id="dag_email_marketing",
        params=dict(
            app_name="one-hubspot-email-marketing",
            mainApplicationFile="local:///app/one-hubspot-base-email-marketing.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_email_marketing = (
        SparkKubernetesSensor(
            task_id="monitor_email_marketing",
            attach_log=True,
            application_name="{{ task_instance.xcom_pull(task_ids='dag_email_marketing')['metadata']['name'] }}",
        ),
    )

    dataset_email_marketing = EmptyOperator(
        task_id="dataset_email_marketing", outlets=[BASE_EMAIL_MARKETING]
    )

    # Task para atualizar as bases ClientesAssessores e enviar os e-mails de alterações
    clientes_assessores = SparkKubernetesOperator(
        task_id="dag_clientes_assessores",
        params=dict(
            app_name="one-hubspot-clientes-assessores",
            mainApplicationFile="local:///app/one-hubspot-clientes-assessores.py",
        ),
        application_file="ConfigSpark4G.yaml",
        do_xcom_push=True,
    )

    monitor_clientes_assessores = (
        SparkKubernetesSensor(
            task_id="monitor_clientes_assessores",
            attach_log=True,
            application_name="{{ task_instance.xcom_pull(task_ids='dag_clientes_assessores')['metadata']['name'] }}",
        ),
    )

    dataset_clientes_assessores = EmptyOperator(
        task_id="dataset_clientes_assessores",
        outlets=[
            CLIENTES_ASSESSORES,
            CLIENTES_ASSESSORES_NOVOS,
            CONTAS_HISTORICO_CP,
            ADVISORS_NO_DUP_CP,
            CONTAS_HISTORICO,
        ],
    )

    # Task de upload onedrive das bases ClientesAssessores
    clientes_assessores_onedrive = SparkKubernetesOperator(
        task_id="dag_clientes_assessores_onedrive",
        params=dict(
            app_name="one-hubspot-clientes-assessores-onedrive",
            mainApplicationFile="local:///app/one-hubspot-clientes-assessores-onedrive.py",
        ),
        application_file="ConfigSpark6G.yaml",
        do_xcom_push=True,
    )

    monitor_clientes_assessores_onedrive = (
        SparkKubernetesSensor(
            task_id="monitor_clientes_assessores_onedrive",
            attach_log=True,
            application_name="{{ task_instance.xcom_pull(task_ids='dag_clientes_assessores_onedrive')['metadata']['name'] }}",
        ),
    )


objetos_e_associacoes >> monitor_objetos_e_associacoes >> dataset_objetos_e_associacoes
(
    dataset_objetos_e_associacoes
    >> indicacoes_e_frequencia
    >> monitor_indicacoes_e_frequencia
    >> dataset_indicacoes_e_frequencia
)
dataset_objetos_e_associacoes >> corporate >> monitor_corporate >> dataset_corporate
(
    dataset_objetos_e_associacoes
    >> email_marketing
    >> monitor_email_marketing
    >> dataset_email_marketing
)
(
    dataset_objetos_e_associacoes
    >> clientes_assessores
    >> monitor_clientes_assessores
    >> dataset_clientes_assessores
)

(
    [
        dataset_clientes_assessores,
        dataset_corporate,
        dataset_email_marketing,
        dataset_indicacoes_e_frequencia,
    ]
    >> clientes_assessores_onedrive
    >> monitor_clientes_assessores_onedrive
)
