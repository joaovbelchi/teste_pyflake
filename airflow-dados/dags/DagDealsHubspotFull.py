from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from datetime import datetime, time
import pytz
from utils.msteams import msteams_task_failed
from utils.DagDealsHubspotFull.functions import processa_tabela_id_receita_hubspot
from utils.datasets import (
    TB_CONTAS_LANDING,
    TB_CONTAS_BRONZE,
    TB_CONTATOS_LANDING,
    TB_CONTATOS_BRONZE,
    TB_CONTATOS_SILVER,
    TB_EMPRESAS_LANDING,
    TB_EMPRESAS_BRONZE,
    TB_GRUPO_FAMILIAR_LANDING,
    TB_DEALS_STAGING_LANDING,
    TB_DEALS_BRONZE,
    TB_DEALS_LANDING,
    MEETINGS_LANDING,
    NOTES_LANDING,
    CALLS_LANDING,
    EMAILS_LANDING,
    COMUNICATIONS_LANDING,
    REUNIOES_CRM,
    CRM_INTERACTIONS,
    INTERACTIONS_CRM,
    DF_AUX_MARKETING,
    PIPELINE_MARKETING_SILVER,
    PIPELINE_MARKETING_GOLD,
    MARKETING_ASSESSORES_SDR,
    BTG,
    PROSPECCAO_COMERCIAL_SILVER,
    PROSPECCAO_COMERCIAL_GOLD,
    PIPELINE_NNM_SOW_SILVER,
    PIPELINE_NNM_SOW_GOLD,
    FUNIL_CORPORATE_SILVER,
    FUNIL_CORPORATE_GOLD,
    INDICACOES_CORPORATE_SILVER,
    INDICACOES_CORPORATE_GOLD,
    FUNIL_BANKERS_SILVER,
    FUNIL_BANKERS_GOLD,
    TROCA_FUNIL,
    FUNIL_OPERACIONAL,
    CAMBIO_PROSPECCAO_SILVER,
    CAMBIO_PROSPECCAO_GOLD,
    CAMBIO_COMERCIAL_SILVER,
    CAMBIO_COMERCIAL_GOLD,
    SEGUROS_SILVER,
    SEGUROS_GOLD,
    FUNIL_TECH_SILVER,
    FUNIL_TECH_GOLD,
    RETROSPECTIVA_SPRINTS,
)


def failure_callback_func(context):
    msteams_task_failed(context)


def verifica_horario():
    """Verifica se o horário na time zone de São Paulo
    está entre 10:00 e 11:00. Se sim, retorna verdadeiro,
    se não, retorna falso

    Returns:
        bool: Verdadeiro se horário está entre 10:00 e 11:00.
        Falso, caso contrário
    """
    tz = pytz.timezone("America/Sao_Paulo")
    current_time = datetime.now(tz).time()
    return time(10, 0) <= current_time <= time(11, 0)


default_args = {
    "owner": "Fred Lamas",
    "start_date": datetime(2023, 8, 16, 14, 30),
    "email": ["frederick.lamas@investimentos.one"],
    "on_failure_callback": failure_callback_func,
    "retries": 0,
    "poke_interval": 10,
    "kubernetes_conn_id": "con_eks_dados",
    "namespace": "processing",
}

with DAG(
    dag_id="Dag_Deals_Hubspot_Full",
    default_args=default_args,
    schedule_interval="0 4,13,20 * * 1-5",
    tags=["pythonoperator", "prd", "dados", "one", "hubspot"],
    max_active_tasks=3,
    catchup=False,
    description="Pipeline para coleta de dados de deals e engagementes e armazenamento no Data Lake",
) as dag:
    # ---------- TASK DOWNLOADS ----------#
    deals_downloads = SparkKubernetesOperator(
        task_id="dag_deals_hubspot_downloads",
        params=dict(
            app_name="one-deals-hubspot-download",
            mainApplicationFile="local:///app/one-hubspot-deals-downloads.py",
        ),
        application_file="ConfigSpark6G.yaml",
        do_xcom_push=True,
    )

    monitor_deals_downloads = SparkKubernetesSensor(
        task_id="monitor_download_hubspot",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_deals_hubspot_downloads')['metadata']['name'] }}",
    )

    dataset_deals_downloads = EmptyOperator(
        task_id="dataset_deals_downloads",
        outlets=[
            TB_CONTAS_LANDING,
            TB_CONTAS_BRONZE,
            TB_CONTATOS_LANDING,
            TB_CONTATOS_BRONZE,
            TB_CONTATOS_SILVER,
            TB_EMPRESAS_LANDING,
            TB_EMPRESAS_BRONZE,
            TB_GRUPO_FAMILIAR_LANDING,
            TB_DEALS_STAGING_LANDING,
            TB_DEALS_BRONZE,
            TB_DEALS_LANDING,
        ],
    )

    # ---------- TASK ENGAGEMENTS ----------#
    engagements = SparkKubernetesOperator(
        task_id="dag_deals_hubspot_engagements",
        params=dict(
            app_name="one-deals-hubspot-engagements",
            mainApplicationFile="local:///app/one-hubspot-deals-engagements.py",
        ),
        application_file="ConfigSpark4G.yaml",
        do_xcom_push=True,
    )

    monitor_engagements = SparkKubernetesSensor(
        task_id="monitor_engagements_hubspot",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_deals_hubspot_engagements')['metadata']['name'] }}",
    )

    dataset_engagements = EmptyOperator(
        task_id="dataset_engagements",
        outlets=[
            MEETINGS_LANDING,
            NOTES_LANDING,
            CALLS_LANDING,
            EMAILS_LANDING,
            COMUNICATIONS_LANDING,
            REUNIOES_CRM,
            CRM_INTERACTIONS,
            INTERACTIONS_CRM,
        ],
    )

    # ---------- TASK MARKETING ASSESSORES ----------#
    marketing_assessores = SparkKubernetesOperator(
        task_id="dag_deals_hubspot_marketing",
        params=dict(
            app_name="one-deals-hubspot-marketing",
            mainApplicationFile="local:///app/one-hubspot-deals-marketingassessores.py",
        ),
        application_file="ConfigSpark10G.yaml",
        do_xcom_push=True,
    )

    monitor_marketing_assessores = SparkKubernetesSensor(
        task_id="monitor_marketing_hubspot",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_deals_hubspot_marketing')['metadata']['name'] }}",
    )

    dataset_marketing_assessores = EmptyOperator(
        task_id="dataset_marketing_assessores",
        outlets=[
            DF_AUX_MARKETING,
            PIPELINE_MARKETING_SILVER,
            PIPELINE_MARKETING_GOLD,
            MARKETING_ASSESSORES_SDR,
        ],
    )

    # ---------- TASK PROSPECÇÃO COMERCIAL ----------#
    prospeccao_comercial = SparkKubernetesOperator(
        task_id="dag_deals_hubspot_prospeccao",
        params=dict(
            app_name="one-deals-hubspot-prospeccao",
            mainApplicationFile="local:///app/one-hubspot-deals-prospeccaocomercial.py",
        ),
        application_file="ConfigSpark4G.yaml",
        do_xcom_push=True,
    )

    monitor_prospeccao_comercial = SparkKubernetesSensor(
        task_id="monitor_prospeccao_hubspot",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_deals_hubspot_prospeccao')['metadata']['name'] }}",
    )

    dataset_prospeccao_comercial = EmptyOperator(
        task_id="dataset_prospeccao_comercial",
        outlets=[BTG, PROSPECCAO_COMERCIAL_SILVER, PROSPECCAO_COMERCIAL_GOLD],
    )

    # ---------- TASK CAPTAÇÃO - SOW ----------#
    captacao_sow = SparkKubernetesOperator(
        task_id="dag_deals_hubspot_sow",
        params=dict(
            app_name="one-deals-hubspot-sow",
            mainApplicationFile="local:///app/one-hubspot-deals-captacaosow.py",
        ),
        application_file="ConfigSpark4G.yaml",
        do_xcom_push=True,
    )

    monitor_captacao_sow = SparkKubernetesSensor(
        task_id="monitor_sow_hubspot",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_deals_hubspot_sow')['metadata']['name'] }}",
    )

    dataset_captacao_sow = EmptyOperator(
        task_id="dataset_captacao_sow",
        outlets=[PIPELINE_NNM_SOW_SILVER, PIPELINE_NNM_SOW_GOLD],
    )

    # ---------- TASK CORPORATE ----------#
    corporate = SparkKubernetesOperator(
        task_id="dag_deals_hubspot_corporate",
        params=dict(
            app_name="one-deals-hubspot-corporate",
            mainApplicationFile="local:///app/one-hubspot-deals-corporate.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_corporate = SparkKubernetesSensor(
        task_id="monitor_corporate_hubspot",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_deals_hubspot_corporate')['metadata']['name'] }}",
    )

    dataset_corporate = EmptyOperator(
        task_id="dataset_corporate",
        outlets=[
            FUNIL_CORPORATE_SILVER,
            FUNIL_CORPORATE_GOLD,
            INDICACOES_CORPORATE_SILVER,
            INDICACOES_CORPORATE_GOLD,
        ],
    )

    # ---------- TASK BANKERS ----------#
    bankers = SparkKubernetesOperator(
        task_id="dag_deals_hubspot_bankers",
        params=dict(
            app_name="one-deals-hubspot-bankers",
            mainApplicationFile="local:///app/one-hubspot-deals-bankers.py",
        ),
        application_file="ConfigSpark4G.yaml",
        do_xcom_push=True,
    )

    monitor_bankers = SparkKubernetesSensor(
        task_id="monitor_bankers_hubspot",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_deals_hubspot_bankers')['metadata']['name'] }}",
    )

    dataset_bankers = EmptyOperator(
        task_id="dataset_bankers_hubspot",
        outlets=[FUNIL_BANKERS_SILVER, FUNIL_BANKERS_GOLD, TROCA_FUNIL],
    )

    # ---------- TASK OPERACIONAL ----------#
    operacional = SparkKubernetesOperator(
        task_id="dag_deals_hubspot_operacional",
        params=dict(
            app_name="one-deals-hubspot-operacional",
            mainApplicationFile="local:///app/one-hubspot-deals-operacional.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_operacional = SparkKubernetesSensor(
        task_id="monitor_operacional_hubspot",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_deals_hubspot_operacional')['metadata']['name'] }}",
    )

    dataset_operacional = EmptyOperator(
        task_id="dataset_operacional", outlets=[FUNIL_OPERACIONAL]
    )

    # #---------- TASK CÂMBIO ----------#
    cambio = SparkKubernetesOperator(
        task_id="dag_deals_hubspot_cambio",
        params=dict(
            app_name="one-deals-hubspot-cambio",
            mainApplicationFile="local:///app/one-hubspot-deals-cambio.py",
        ),
        application_file="ConfigSpark4G.yaml",
        do_xcom_push=True,
    )

    monitor_cambio = SparkKubernetesSensor(
        task_id="monitor_deals_hubspot_cambio",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_deals_hubspot_cambio')['metadata']['name'] }}",
    )

    dataset_cambio = EmptyOperator(
        task_id="dataset_cambio",
        outlets=[
            CAMBIO_PROSPECCAO_SILVER,
            CAMBIO_PROSPECCAO_GOLD,
            CAMBIO_COMERCIAL_SILVER,
            CAMBIO_COMERCIAL_GOLD,
        ],
    )

    # #---------- TASK SEGUROS ----------#
    seguros = SparkKubernetesOperator(
        task_id="dag_deals_hubspot_seguros",
        params=dict(
            app_name="one-hubspot-deals-seguros",
            mainApplicationFile="local:///app/one-hubspot-deals-seguros.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_seguros = SparkKubernetesSensor(
        task_id="monitor_deals_hubspot_seguros",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_deals_hubspot_seguros')['metadata']['name'] }}",
    )

    dataset_seguros = EmptyOperator(
        task_id="dataset_seguros", outlets=[SEGUROS_SILVER, SEGUROS_GOLD]
    )

    # #---------- TASK TECH ----------#
    tech = SparkKubernetesOperator(
        task_id="dag_deals_hubspot_tech",
        params=dict(
            app_name="one-hubspot-deals-tech",
            mainApplicationFile="local:///app/one-hubspot-deals-tech.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_tech = SparkKubernetesSensor(
        task_id="monitor_deals_hubspot_tech",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_deals_hubspot_tech')['metadata']['name'] }}",
    )

    dataset_tech = EmptyOperator(
        task_id="dataset_deals_hubspot_tech",
        outlets=[FUNIL_TECH_SILVER, FUNIL_TECH_GOLD, RETROSPECTIVA_SPRINTS],
    )

    # #---------- TASK TABELA ID RECEITA ----------#

    task_tabela_id_receita_hubspot = PythonOperator(
        task_id="processa_tabela_id_receita_hubspot",
        python_callable=processa_tabela_id_receita_hubspot,
    )

    # #---------- TASK UPLOAD ONEDRIVE ----------#
    upload_onedrive = SparkKubernetesOperator(
        task_id="dag_deals_hubspot_upload_onedrive",
        params=dict(
            app_name="one-hubspot-deals-upload-onedrive",
            mainApplicationFile="local:///app/one-hubspot-deals-upload-onedrive.py",
        ),
        application_file="ConfigSpark10G.yaml",
        do_xcom_push=True,
    )

    monitor_upload_onedrive = SparkKubernetesSensor(
        task_id="monitor_deals_hubspot_upload_onedrive",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='dag_deals_hubspot_upload_onedrive')['metadata']['name'] }}",
    )

    # #---------- AJUSTE TEMPORÁRIO ----------#

    task_agrupa_pipe_comercial = EmptyOperator(
        task_id="agrupa_pipes_comerciais",
    )


(deals_downloads >> monitor_deals_downloads >> dataset_deals_downloads)


(
    dataset_deals_downloads
    >> marketing_assessores
    >> monitor_marketing_assessores
    >> dataset_marketing_assessores
    >> task_agrupa_pipe_comercial
)
(
    dataset_deals_downloads
    >> prospeccao_comercial
    >> monitor_prospeccao_comercial
    >> dataset_prospeccao_comercial
    >> task_agrupa_pipe_comercial
)
(
    dataset_deals_downloads
    >> captacao_sow
    >> monitor_captacao_sow
    >> dataset_captacao_sow
    >> task_agrupa_pipe_comercial
)


(
    task_agrupa_pipe_comercial
    >> engagements
    >> monitor_engagements
    >> dataset_engagements
)


task_agrupa_pipe_comercial >> corporate >> monitor_corporate >> dataset_corporate
task_agrupa_pipe_comercial >> bankers >> monitor_bankers >> dataset_bankers
task_agrupa_pipe_comercial >> operacional >> monitor_operacional >> dataset_operacional
task_agrupa_pipe_comercial >> cambio >> monitor_cambio >> dataset_cambio
task_agrupa_pipe_comercial >> seguros >> monitor_seguros >> dataset_seguros
task_agrupa_pipe_comercial >> tech >> monitor_tech >> dataset_tech
task_agrupa_pipe_comercial >> task_tabela_id_receita_hubspot

(
    [
        dataset_engagements,
        dataset_corporate,
        dataset_bankers,
        dataset_operacional,
        dataset_cambio,
        dataset_seguros,
        dataset_tech,
    ]
    >> upload_onedrive
    >> monitor_upload_onedrive
)
