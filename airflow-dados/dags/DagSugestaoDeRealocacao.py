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
    PONTUACAO_ATIVOS_RUN,
    SUGESTAO_DE_REALOCACAO,
    SUGESTAO_DE_REALOCACAO_ASSET_ALLOCATION,
    SUGESTAO_DE_REALOCACAO_SAIDAS,
    SUGESTAO_DE_REALOCACAO_ENTRADA_FUNDOS,
    SUGESTAO_DE_REALOCACAO_ENTRADA_RF,
)
from airflow.operators.empty import EmptyOperator


def failure_callback_func(context):
    msteams_task_failed(context)


default_args = {
    "owner": "Nathalia Montandon",
    "start_date": datetime(2023, 10, 10, 15, 45),
    "on_failure_callback": failure_callback_func,
    "email": ["nathalia.montandon@investimentos.one"],
    "retries": 0,
    "kubernetes_conn_id": "con_eks_dados",
    "namespace": "processing",
}

with DAG(
    dag_id="Dag_Sugestao_De_Realocacao",
    default_args=default_args,
    schedule=[PONTUACAO_ATIVOS_RUN],
    tags=[
        "dados",
        "one",
        "sugestao_de_realocacao",
        "receita_estimada",
        "microsoft",
        "gold",
    ],
    catchup=False,
    description="Pipeline para calcular a estimativa de receita a partir das sugestoes de realocacao",
    max_active_runs=1,
) as dag:
    # Task para gerar a base para os cálculos
    asset_allocation = SparkKubernetesOperator(
        task_id="submit_asset_allocation",
        params=dict(
            app_name="sugestao-asset-allocation",
            mainApplicationFile="local:///app/one-rf-sugestao-de-realocacao-asset-allocation.py",
        ),
        application_file="ConfigSpark.yaml",
        do_xcom_push=True,
    )

    monitor_asset_allocation = SparkKubernetesSensor(
        task_id="monitor_asset_allocation",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='submit_asset_allocation')['metadata']['name'] }}",
    )

    dataset_asset_allocation = EmptyOperator(
        task_id="dataset_asset_allocation",
        outlets=[SUGESTAO_DE_REALOCACAO_ASSET_ALLOCATION],
    )

    # Task para gerar as sugestões de saída
    sugestoes_de_saida = SparkKubernetesOperator(
        task_id="submit_sugestoes_de_saida",
        params=dict(
            app_name="sugestao-saidas",
            mainApplicationFile="local:///app/one-rf-sugestao-de-realocacao-saidas.py",
        ),
        application_file="ConfigSpark10G.yaml",
        do_xcom_push=True,
    )

    monitor_sugestoes_de_saida = SparkKubernetesSensor(
        task_id="monitor_sugestoes_de_saida",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='submit_sugestoes_de_saida')['metadata']['name'] }}",
    )

    dataset_sugestoes_de_saida = EmptyOperator(
        task_id="dataset_sugestoes_de_saida", outlets=[SUGESTAO_DE_REALOCACAO_SAIDAS]
    )

    # Task para gerar as sugestões de entrada em Fundos
    sugestoes_de_entrada_fundos = SparkKubernetesOperator(
        task_id="submit_sugestoes_de_entrada_fundos",
        params=dict(
            app_name="sugestao-entradas-fundos",
            mainApplicationFile="local:///app/one-rf-sugestao-de-realocacao-entradas-fundos.py",
        ),
        application_file="ConfigSpark10G.yaml",
        do_xcom_push=True,
    )

    monitor_sugestoes_de_entrada_fundos = SparkKubernetesSensor(
        task_id="monitor_sugestoes_de_entrada_fundos",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='submit_sugestoes_de_entrada_fundos')['metadata']['name'] }}",
    )

    dataset_sugestoes_de_entrada_fundos = EmptyOperator(
        task_id="dataset_sugestoes_de_entrada_fundos",
        outlets=[SUGESTAO_DE_REALOCACAO_ENTRADA_FUNDOS],
    )

    # Task para gerar as sugestões de entrada RF
    sugestoes_de_entrada_rf = SparkKubernetesOperator(
        task_id="submit_sugestoes_de_entrada_rf",
        params=dict(
            app_name="sugestao-entradas-rf",
            mainApplicationFile="local:///app/one-rf-sugestao-de-realocacao-entradas-rf.py",
        ),
        application_file="ConfigSpark10G.yaml",
        do_xcom_push=True,
    )

    monitor_sugestoes_de_entrada_rf = SparkKubernetesSensor(
        task_id="monitor_sugestoes_de_entrada_rf",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='submit_sugestoes_de_entrada_rf')['metadata']['name'] }}",
    )

    dataset_sugestoes_de_entrada_rf = EmptyOperator(
        task_id="dataset_sugestoes_de_entrada_rf",
        outlets=[SUGESTAO_DE_REALOCACAO_ENTRADA_RF],
    )

    # Task para calcular a estimativa de receita gerada a partir das sugetões de realocação
    receita_estimada = SparkKubernetesOperator(
        task_id="submit_receita_estimada",
        params=dict(
            app_name="sugestao-receita-estimada",
            mainApplicationFile="local:///app/one-rf-sugestao-de-realocacao-receita-estimada.py",
        ),
        application_file="ConfigSpark10G.yaml",
        do_xcom_push=True,
    )

    monitor_receita_estimada = SparkKubernetesSensor(
        task_id="monitor_receita_estimada",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='submit_receita_estimada')['metadata']['name'] }}",
    )

    dataset_receita_estimada = EmptyOperator(
        task_id="dataset_receita_estimada", outlets=[SUGESTAO_DE_REALOCACAO]
    )

    # Task para fazer upload do arquivo contendo as sugestões no OneDrive
    upload = SparkKubernetesOperator(
        task_id="submit_upload",
        params=dict(
            app_name="sugestao-upload-onedrive",
            mainApplicationFile="local:///app/one-rf-sugestao-de-realocacao-upload-onedrive.py",
        ),
        application_file="ConfigSpark10G.yaml",
        do_xcom_push=True,
    )

    monitor_upload = SparkKubernetesSensor(
        task_id="monitor_upload",
        attach_log=True,
        application_name="{{ task_instance.xcom_pull(task_ids='submit_upload')['metadata']['name'] }}",
    )

    asset_allocation >> monitor_asset_allocation >> dataset_asset_allocation
    sugestoes_de_saida >> monitor_sugestoes_de_saida >> dataset_sugestoes_de_saida
    (
        sugestoes_de_entrada_fundos
        >> monitor_sugestoes_de_entrada_fundos
        >> dataset_sugestoes_de_entrada_fundos
    )
    (
        sugestoes_de_entrada_rf
        >> monitor_sugestoes_de_entrada_rf
        >> dataset_sugestoes_de_entrada_rf
    )
    receita_estimada >> monitor_receita_estimada >> dataset_receita_estimada
    upload >> monitor_upload

    dataset_asset_allocation >> sugestoes_de_saida
    dataset_sugestoes_de_saida >> [sugestoes_de_entrada_fundos, sugestoes_de_entrada_rf]
    [
        dataset_sugestoes_de_entrada_fundos,
        dataset_sugestoes_de_entrada_rf,
    ] >> receita_estimada
    dataset_receita_estimada >> upload
