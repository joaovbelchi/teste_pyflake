from airflow.datasets import Dataset


bucket_lake_landing = "prd-dados-lake-landing-oneinvest"
bucket_lake_gold = "prd-dados-lake-gold-oneinvest"
bucket_lake_silver = "prd-dados-lake-silver-oneinvest"
bucket_lake_bronze = "prd-dados-lake-bronze-oneinvest"


PLANILHA_SEGUROS = Dataset("onedrive/planilha_seguros")

# AlertaBloqueioJudicial
PRE_OPERACOES = Dataset(
    f"s3://{bucket_lake_landing}/webhook/bases/pre_operacoes/pre_operacoes.parquet"
)
OPERACOES_WEBHOOK = Dataset(
    f"s3://{bucket_lake_landing}/webhook/bases/pre_operacoes/operacoes_webhook.parquet"
)

# AssetAllocation
ASSET_ALLOCATION_HISTORICO = Dataset(
    f"s3a://{bucket_lake_gold}/api/one/asset_allocation/asset_allocation_historico.parquet"
)
ASSET_ALLOCATION_MOVIMENTACAO = Dataset(
    f"s3a://{bucket_lake_gold}/api/one/asset_allocation/asset_allocation_movimentacao.parquet"
)
ASSET_ALLOCATION_METRICA = Dataset(
    f"s3://{bucket_lake_gold}/api/one/asset_allocation/metrica_asset_allocation.parquet"
)


# ClientesAssessores
COMPANIES = Dataset(f"s3a://{bucket_lake_silver}/api/hubspot/associations/companies")
CONTACTS_AND_COMPANIES = Dataset(
    f"s3a://{bucket_lake_silver}/api/hubspot/associations/contacts_and_companies.parquet"
)

INDICACAO = Dataset(f"s3a://{bucket_lake_gold}/api/hubspot/indicacao/indicacao.parquet")
FREQ_CONTATO = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/freq_contato/freq_contato.parquet"
)

CORPORATE_SOW = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/corporate_sow/corporate_sow.parquet"
)

BASE_EMAIL_MARKETING = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/Base_Email_Marketing/Base_Email_Marketing.parquet"
)

CLIENTES_ASSESSORES = Dataset(
    f"s3a://{bucket_lake_silver}/api/hubspot/clientesassessores/ClientesAssessores.parquet"
)
CLIENTES_ASSESSORES_NOVOS = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/ClientesAssessores_novos/ClientesAssessores_novos.parquet"
)
CONTAS_HISTORICO_CP = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/ContasHistoricoCp/ContasHistoricoCp.parquet"
)
ADVISORS_NO_DUP_CP = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/AdvisorsNoDupCp/AdvisorsNoDupCp.parquet"
)
CONTAS_HISTORICO = Dataset(
    f"s3a://{bucket_lake_silver}/api/btg/crm/ContasHistorico/ContasHistorico.parquet"
)

# Criar contas
CONTAS_SEM_ASSOCIACAO = Dataset(
    f"s3://{bucket_lake_gold}/api/hubspot/contas_sem_associacao/contas_sem_associacao.parquet"
)

# OneSocios
SOCIOS_LANDING = Dataset(f"s3a://{bucket_lake_landing}/api/one/socios/")

SOCIO_SILVER = Dataset(f"s3a://{bucket_lake_silver}/api/one/socios/socios.parquet")

N_ASSESSORES = Dataset(
    f"s3a://{bucket_lake_silver}/api/one/n_assessores/n_assessores.parquet"
)
SOCIO_GOLD = Dataset(f"s3a://{bucket_lake_gold}/api/one/socios/socios.parquet")

# Deals downloads
TB_CONTAS_LANDING = Dataset(f"s3a://{bucket_lake_landing}/api/hubspot/tb_contas/")
TB_CONTAS_BRONZE = Dataset(f"s3a://{bucket_lake_bronze}/api/hubspot/tb_contas/")
TB_CONTATOS_LANDING = Dataset(f"s3a://{bucket_lake_landing}/api/hubspot/tb_contatos/")
TB_CONTATOS_BRONZE = Dataset(f"s3a://{bucket_lake_bronze}/api/hubspot/tb_contatos/")
TB_CONTATOS_SILVER = Dataset(f"s3a://{bucket_lake_silver}/api/hubspot/tb_contatos/")
TB_EMPRESAS_LANDING = Dataset(f"s3a://{bucket_lake_landing}/api/hubspot/tb_empresa/")
TB_EMPRESAS_BRONZE = Dataset(f"s3a://{bucket_lake_bronze}/api/hubspot/tb_empresa/")
TB_GRUPO_FAMILIAR_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/api/hubspot/tb_grupo_familiar/"
)
TB_DEALS_STAGING_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/api/hubspot/tb_deals_staging/"
)
TB_DEALS_BRONZE = Dataset(f"s3a://{bucket_lake_bronze}/api/hubspot/tb_deals/")
TB_DEALS_LANDING = Dataset(f"s3a://{bucket_lake_landing}/api/hubspot/tb_deals/")

MEETINGS_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/api/hubspot/engagements/meetings.parquet"
)
NOTES_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/api/hubspot/engagements/notes.parquet"
)
CALLS_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/api/hubspot/engagements/calls.parquet"
)
EMAILS_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/api/hubspot/engagements/emails.parquet"
)
COMUNICATIONS_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/api/hubspot/engagements/comunications.parquet"
)
REUNIOES_CRM = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/engagements/funis/reunioes_crm/reunioes_crm.parquet"
)
CRM_INTERACTIONS = Dataset(
    f"s3a://{bucket_lake_silver}/api/hubspot/engagements/crm_interactions/crm_interactions.parquet"
)
INTERACTIONS_CRM = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/engagements/interactions_crm/interactions_crm.parquet"
)

DF_AUX_MARKETING = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/funis/df_aux_marketing/df_aux_marketing.parquet"
)
PIPELINE_MARKETING_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/hubspot/funis/pipeline_marketing/pipeline_marketing.parquet"
)
PIPELINE_MARKETING_GOLD = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/funis/pipeline_marketing/pipeline_marketing.parquet"
)
MARKETING_ASSESSORES_SDR = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/funis/marketing_assessores_sdr/marketing_assessores_sdr.parquet"
)

BTG = Dataset(f"s3a://{bucket_lake_silver}/api/hubspot/funis/btg/btg.parquet")
PROSPECCAO_COMERCIAL_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/hubspot/funis/prospeccao_comercial/prospeccao_comercial.parquet"
)
PROSPECCAO_COMERCIAL_GOLD = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/funis/prospeccao_comercial/prospeccao_comercial.parquet"
)

PIPELINE_NNM_SOW_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/hubspot/funis/pipeline_nnm_sow/pipeline_nnm_sow.parquet"
)
PIPELINE_NNM_SOW_GOLD = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/funis/pipeline_nnm_sow/pipeline_nnm_sow.parquet"
)

FUNIL_CORPORATE_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/hubspot/funis/funil_corporate/funil_corporate.parquet"
)
FUNIL_CORPORATE_GOLD = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/funis/funil_corporate/funil_corporate.parquet"
)
INDICACOES_CORPORATE_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/hubspot/funis/indicacoes_corporate/indicacoes_corporate.parquet"
)
INDICACOES_CORPORATE_GOLD = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/funis/indicacoes_corporate/indicacoes_corporate.parquet"
)

FUNIL_BANKERS_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/hubspot/funis/funil_bankers/funil_bankers.parquet"
)
FUNIL_BANKERS_GOLD = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/funis/funil_bankers/funil_bankers.parquet"
)
TROCA_FUNIL = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/funis/troca_funil/troca_funil.parquet"
)

FUNIL_OPERACIONAL = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/funis/funil_operacional/funil_operacional.parquet"
)

CAMBIO_PROSPECCAO_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/hubspot/funis/cambio_prospeccao/cambio_prospeccao.parquet"
)
CAMBIO_PROSPECCAO_GOLD = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/funis/cambio_prospeccao/cambio_prospeccao.parquet"
)
CAMBIO_COMERCIAL_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/hubspot/funis/cambio_comercial/cambio_comercial.parquet"
)
CAMBIO_COMERCIAL_GOLD = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/funis/cambio_comercial/cambio_comercial.parquet"
)

SEGUROS_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/hubspot/funis/seguros/seguros.parquet"
)
SEGUROS_GOLD = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/funis/seguros/seguros.parquet"
)

FUNIL_TECH_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/hubspot/funis/funil_tech/funil_tech.parquet"
)
FUNIL_TECH_GOLD = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/funis/funil_tech/funil_tech.parquet"
)
RETROSPECTIVA_SPRINTS = Dataset(
    f"s3a://{bucket_lake_gold}/api/hubspot/funis/retrospectiva_sprints/retrospectiva_sprints.parquet"
)

# Fresh desk
AGENTES_SILVER = Dataset(
    f"s3://{bucket_lake_silver}/api/one/freshdesk/agentes/agentes.parquet"
)
TICKETS_LANDING = Dataset(
    f"s3://{bucket_lake_landing}/api/one/freshdesk/tickets.parquet"
)
TICKETS_SILVER = Dataset(
    f"s3://{bucket_lake_silver}/api/one/freshdesk/tickets/tickets.parquet"
)
TICKETS_GOLD = Dataset(
    f"s3://{bucket_lake_gold}/api/one/freshdesk/tickets/tickets.parquet"
)

# NPS
BASE_NPS_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/api/onedrive/nps/nps_landing/Base_NPS"
)
BASE_INDICACOES_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/api/onedrive/nps/nps_indicacoes/Base_NPS_indicacoes"
)
BASE_ABORDAGENS_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/api/onedrive/nps/nps_abordagens/Base_NPS_abordagens"
)

BASE_NPS_SILVER = Dataset(f"s3a://{bucket_lake_silver}/api/onedrive/nps/nps/Base_NPS")
BASE_INDICACOES_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/onedrive/nps/nps_indicacoes/Base_NPS_indicacoes"
)
BASE_ABORDAGENS_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/onedrive/nps/nps_abordagens/Base_NPS_abordagens"
)

BASE_NPS_GOLD = Dataset(f"s3a://{bucket_lake_gold}/api/onedrive/nps/nps/nps")
BASE_INDICACOES_GOLD = Dataset(
    f"s3a://{bucket_lake_gold}/api/onedrive/nps/nps_indicacoes/nps_indicacoes"
)
BASE_ABORDAGENS_GOLD = Dataset(
    f"s3a://{bucket_lake_gold}/api/onedrive/nps/respostas_nps/respostas_nps"
)

# RUN Credito Bancario
RUN_CREDITO_BANCARIO_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/one/run/credito_bancario/"
)
RUN_CREDITO_BANCARIO = Dataset(
    f"s3a://{bucket_lake_bronze}/one/run/credito_bancario/credito_bancario/"
)
RUN_TITULO_PUBLICO = Dataset(
    f"s3a://{bucket_lake_bronze}/one/run/credito_bancario/titulo_publico/"
)


# RUN Credito Corporativo
RUN_CREDITO_CORPORATIVO = Dataset(
    f"s3a://{bucket_lake_landing}/one/run/credito_corporativo/"
)
RUN_DEBENTURES_ISENTAS = Dataset(
    f"s3a://{bucket_lake_bronze}/one/run/credito_corporativo/debentures_isentas"
)
RUN_DEBENTURES_NAO_ISENTAS = Dataset(
    f"s3a://{bucket_lake_bronze}/one/run/credito_corporativo/debentures_nao_isentas"
)
RUN_CRI_CRA = Dataset(f"s3a://{bucket_lake_bronze}/one/run/credito_corporativo/cri_cra")

# PontuacaoAtivosRun
PONTUACAO_ATIVOS_RUN = Dataset(
    f"s3://{bucket_lake_gold}/api/one/sugestao-de-realocacao/pontuacao-ativos-run.parquet"
)

# SugestaoDeRealocacao
SUGESTAO_FUNDOS_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/one/sugestao_de_realocacao/sugestoes_de_ativos/"
)
SUGESTAO_FUNDOS_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/one/sugestao_de_realocacao/sugestoes_de_ativos/sugestao_fundos/"
)
SUGESTAO_CREDITO_CORPORATIVO = Dataset(
    f"s3a://{bucket_lake_bronze}/one/sugestao_de_realocacao/sugestoes_de_ativos/sugestao_credito_corporativo/"
)
TERMOS_NEGATIVOS = Dataset(
    f"s3a://{bucket_lake_bronze}/one/sugestao_de_realocacao/sugestoes_de_ativos/termos_negativos/"
)
OFERTAS_PUBLICAS = Dataset(
    f"s3a://{bucket_lake_bronze}/one/sugestao_de_realocacao/sugestoes_de_ativos/ofertas_publicas/"
)

ASSET_ALLOCATION_REFERENCIA_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/one/sugestao_de_realocacao/referencia_asset_allocation/"
)
ASSET_ALLOCATION_REFERENCIA_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/one/sugestao_de_realocacao/referencia_asset_allocation/"
)

SUGESTAO_DE_REALOCACAO_ASSET_ALLOCATION = Dataset(
    f"s3://{bucket_lake_silver}/one/sugestao_de_realocacao/asset_allocation.parquet"
)
SUGESTAO_DE_REALOCACAO_SAIDAS = Dataset(
    f"s3//{bucket_lake_silver}/one/sugestao_de_realocacao/sugestoes_de_saida.parquet"
)
SUGESTAO_DE_REALOCACAO_ENTRADA_FUNDOS = Dataset(
    f"s3//{bucket_lake_silver}/one/sugestao_de_realocacao/sugestoes_de_entrada_fundos.parquet"
)
SUGESTAO_DE_REALOCACAO_ENTRADA_RF = Dataset(
    f"s3//{bucket_lake_silver}/one/sugestao_de_realocacao/sugestoes_de_entrada_rf.parquet"
)
SUGESTAO_DE_REALOCACAO = Dataset(
    f"s3://{bucket_lake_gold}/api/one/sugestao-de-realocacao/sugestao_de_realocacao.parquet"
)

# Receita Estimada
RECEITA_ESTIMADA = Dataset(
    f"s3://{bucket_lake_gold}/one/receita/estimativa/receita_estimada.parquet"
)
BANCO_DE_DADOS_RV = Dataset(
    f"s3a://{bucket_lake_gold}/api/one/receita_estimada/banco_de_dados_rv.parquet"
)

# Reserva Ofertas Publicas
ACOMPANHAMENTO_RESERVAS = Dataset(
    f"s3a://{bucket_lake_gold}/api/one/Ofertas_Publicas/acompanhamento_reservas.parquet"
)

RANKING_RESERVAS = Dataset(
    f"s3a://{bucket_lake_gold}/api/one/Ofertas_Publicas/ranking_reservas.parquet"
)

# BTG full

TB_FUNDOS_BTG_LANDING = Dataset(f"s3a://{bucket_lake_landing}/api/btg/s3/tb_fundos/")
TB_FUNDOS_BTG_BRONZE = Dataset(f"s3a://{bucket_lake_bronze}/api/btg/s3/tb_fundos/")

TB_FUNDOS_INFORMACAO_BTG_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/api/btg/s3/tb_fundo_informacao/"
)
TB_FUNDOS_INFORMACAO_BTG_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/api/btg/s3/tb_fundo_informacao/"
)

TB_MOVIMENTACAO_BTG_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/api/btg/s3/tb_movimentacao/"
)
TB_MOVIMENTACAO_BTG_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/api/btg/s3/tb_movimentacao/"
)
TB_MOVIMENTACAO_BTG_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/btg/s3/movimentacao_historico/movimentacao_historico.parquet"
)

TB_POSICAO_BTG_LANDING = Dataset(f"s3a://{bucket_lake_landing}/api/btg/s3/tb_posicao/")
TB_POSICAO_BTG_BRONZE = Dataset(f"s3a://{bucket_lake_bronze}/api/btg/s3/tb_posicao/")
TB_POSICAO_BTG_SILVER = Dataset(f"s3a://{bucket_lake_silver}/api/btg/s3/Posicao/")

TB_PL_BTG_BRONZE = Dataset(f"s3a://{bucket_lake_silver}/api/btg/s3/Pl/")
TB_PL_TEMP_BTG_SILVER = Dataset(f"s3a://{bucket_lake_silver}/api/btg/s3/Pltemp/")
TB_PL_BTG_GOLD = Dataset(f"s3a://{bucket_lake_gold}/api/btg/s3/PL/PL.parquet")

TB_NNM_B2C_BTG_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/api/one/nnm_b2c/nnm_b2c.parquet"
)
TB_ACC_IN_OUT_BTG_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/api/one/accounts_in_out/accounts_in_out.parquet"
)
TB_NNM_HISTORICO_BTG_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/btg/s3/nnm/nnm_historico.parquet"
)
TB_NNM_BTG_GOLD = Dataset(f"s3a://{bucket_lake_gold}/api/btg/s3/nnm/NNM.parquet")

TB_NNM_HISTORICO_BTG_SILVER_OFFSHORE = Dataset(
    f"s3://{bucket_lake_silver}/api/btg/s3/nnm/nnm_historico_offshore.parquet"
)

# BTG Access
TB_MOVIMENTACAO_BTG_ACCESS_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/btg/access/export/movimentacao/"
)
TB_NNM_BTG_ACCESS_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/btg/access/export/net_new_money/"
)

TB_MOVIMENTACAO_HISTORICO_BTG_ACCESS_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/btg/access/export/movimentacao_historico/"
)
TB_NNM_HISTORICO_BTG_ACCESS_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/btg/access/export/net_new_money_historico/"
)
TB_MOVIMENTACAO_BTG_ACCESS_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/btg/access/export/movimentacao/"
)
TB_NNM_BTG_ACCESS_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/btg/access/export/net_new_money/"
)
TB_MOVIMENTACAO_HISTORICO_BTG_ACCESS_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/btg/access/export/movimentacao_historico/"
)
TB_NNM_HISTORICO_BTG_ACCESS_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/btg/access/export/net_new_money_historico/"
)

# Ranking Exclusive 1
RANKING_EXCLUSIVE1 = Dataset(
    f"s3a://{bucket_lake_gold}/api/one/ranking_exclusive1/ranking_exclusive1.parquet"
)

AGIO_DESAGIO_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/one/agio_desagio/agio_e_desagio.parquet"
)

AGIO_DESAGIO_GOLD = Dataset(
    f"s3a://{bucket_lake_gold}/api/one/agio_desagio/agio_e_desagio.parquet"
)

# BTG Webhook
TB_CONTAS_ASSESSOR_BTG_WEBHOOK_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/btg/webhooks/downloads/contas_por_assessor/"
)

TB_DADOS_CADASTRAIS_BTG_WEBHOOK_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/btg/webhooks/downloads/dados_cadastrais/"
)

TB_DADOS_ONBOARDING_BTG_WEBHOOK_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/btg/webhooks/downloads/dados_onboarding/"
)

TB_MOVIMENTACAO_BTG_WEBHOOK_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/btg/webhooks/downloads/movimentacao/"
)

TB_FUNDOS_BTG_WEBHOOK_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/btg/webhooks/downloads/fundos/"
)

TB_FUNDOS_INFO_BTG_WEBHOOK_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/btg/webhooks/downloads/fundos_informacao/"
)

TB_CONTAS_ASSESSOR_BTG_WEBHOOK_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/btg/webhooks/downloads/contas_por_assessor/"
)

TB_DADOS_CADASTRAIS_BTG_WEBHOOK_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/btg/webhooks/downloads/dados_cadastrais/"
)

TB_DADOS_ONBOARDING_BTG_WEBHOOK_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/btg/webhooks/downloads/dados_onboarding/"
)

TB_MOVIMENTACAO_BTG_WEBHOOK_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/btg/webhooks/downloads/movimentacao/"
)

TB_FUNDOS_BTG_WEBHOOK_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/btg/webhooks/downloads/fundos/"
)

TB_FUNDOS_INFO_BTG_WEBHOOK_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/btg/webhooks/downloads/fundos_informacao/"
)

# BTG Silver e Gold
TB_CONTAS_BTG_SILVER_NOVA = Dataset(f"s3a://{bucket_lake_silver}/btg/contas/")

TB_CONTAS_BTG_GOLD_NOVA = Dataset(f"s3a://{bucket_lake_gold}/btg/contas/")

# Airflow Rest API
TB_AIRFLOW_API_TASK_INSTANCES_LANDING = Dataset(
    f"s3://{bucket_lake_landing}/airflow/rest_api/task_instances/"
)
TB_AIRFLOW_API_TASK_INSTANCES_BRONZE = Dataset(
    f"s3://{bucket_lake_bronze}/airflow/rest_api/task_instances/"
)

TB_AIRFLOW_API_DAG_RUNS_LANDING = Dataset(
    f"s3://{bucket_lake_landing}/airflow/rest_api/dag_runs/"
)
TB_AIRFLOW_API_DAG_RUNS_BRONZE = Dataset(
    f"s3://{bucket_lake_bronze}/airflow/rest_api/dag_runs/"
)

TB_AIRFLOW_API_DAGS_LANDING = Dataset(
    f"s3://{bucket_lake_landing}/airflow/rest_api/dags/"
)
TB_AIRFLOW_API_DAGS_BRONZE = Dataset(
    f"s3://{bucket_lake_bronze}/airflow/rest_api/dags/"
)

TB_AIRFLOW_API_EVENT_LOGS_LANDING = Dataset(
    f"s3://{bucket_lake_landing}/airflow/rest_api/event_logs/"
)
TB_AIRFLOW_API_EVENT_LOGS_BRONZE = Dataset(
    f"s3://{bucket_lake_bronze}/airflow/rest_api/event_logs/"
)

# A3Data

TB_B3_FII_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/api/one/b3/fundos_listados/fii//"
)

TB_B3_FIP_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/api/one/b3/fundos_listados/fip/"
)

TB_B3_ETF_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/api/one/b3/fundos_listados/etf/"
)

TB_B3_FII_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/api/one/b3/fundos_listados/fii/"
)

TB_B3_FIP_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/api/one/b3/fundos_listados/fip/"
)

TB_B3_ETF_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/api/one/b3/fundos_listados/etf/"
)
TB_B3_FII_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/one/b3/fundos_listados/fii/"
)

TB_B3_FIP_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/one/b3/fundos_listados/fip/"
)

TB_B3_ETF_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/api/one/b3/fundos_listados/etf/"
)

ASSET_ALLOCATION_PROCESSADA = Dataset(
    f"s3://{bucket_lake_silver}/ml/clusterizacao_clientes/asset_allocation_processada/"
)

MOVIMENTACAO_PROCESSADA = Dataset(
    f"s3://{bucket_lake_silver}/ml/clusterizacao_clientes/movimentacao_processada/"
)

NNM_PROCESSADA = Dataset(
    f"s3://{bucket_lake_silver}/ml/clusterizacao_clientes/nnm_processada/"
)

POSICAO_PROCESSADA = Dataset(
    f"s3://{bucket_lake_silver}/ml/clusterizacao_clientes/posicao_processada/"
)


PREVISAO_ASSET_ALLOCATION = Dataset(
    f"s3://{bucket_lake_gold}/ml/clusterizacao_clientes/asset_allocation/"
)

PREVISAO_MOVIMENTACAO = Dataset(
    f"s3://{bucket_lake_gold}/ml/clusterizacao_clientes/movimentacao/"
)

PREVISAO_NNM = Dataset(f"s3://{bucket_lake_gold}/ml/clusterizacao_clientes/nnm/")

# ACC IN OUT

ACC_IN_OUT_SILVER = Dataset(f"s3a://{bucket_lake_silver}/btg/api/accounts_in_out/")

ACC_IN_OUT_OFFSHORE = Dataset(f"s3a://{bucket_lake_gold}/btg/accounts_in_out_offshore/")

ACC_IN_OUT_GOLD = Dataset(f"s3a://{bucket_lake_gold}/btg/accounts_in_out/")

# Receita
AAI_LANDING = Dataset(f"s3a://{bucket_lake_landing}/btg/onedrive/receita/aai/")

AAI_BRONZE = Dataset(f"s3a://{bucket_lake_bronze}/btg/onedrive/receita/aai/")

AAI_SILVER_PRE_PROCESSADA = Dataset(
    f"s3://{bucket_lake_silver}/btg/onedrive/receita/aai_pre_processada/"
)

AAI_SILVER_PROCESSADA = Dataset(
    f"s3://{bucket_lake_silver}/btg/onedrive/receita/aai_processada/"
)

AVENUE_LANDING = Dataset(f"s3a://{bucket_lake_landing}/btg/onedrive/receita/avenue/")

AVENUE_BRONZE = Dataset(f"s3a://{bucket_lake_bronze}/btg/onedrive/receita/avenue/")

AVENUE_SILVER_PRE_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/avenue_pre_processada/"
)

AVENUE_SILVER_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/avenue_processada/"
)

CORBAN_LANDING = Dataset(f"s3a://{bucket_lake_landing}/btg/onedrive/receita/corban/")

CORBAN_BRONZE = Dataset(f"s3a://{bucket_lake_bronze}/btg/onedrive/receita/corban/")

CORBAN_SILVER_PRE_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_landing}/btg/onedrive/receita/corban_pre_processada/"
)

CORBAN_SILVER_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_bronze}/btg/onedrive/receita/corban_processada/"
)

EXTRAS_LANDING = Dataset(f"s3a://{bucket_lake_landing}/btg/onedrive/receita/extras/")

EXTRAS_BRONZE = Dataset(f"s3a://{bucket_lake_bronze}/btg/onedrive/receita/extras/")

EXTRAS_SILVER_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/extras_processada/"
)

ICATU_LANDING = Dataset(f"s3a://{bucket_lake_landing}/btg/onedrive/receita/icatu/")

ICATU_BRONZE = Dataset(f"s3a://{bucket_lake_bronze}/btg/onedrive/receita/icatu/")

ICATU_SILVER_PRE_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/icatu_pre_processada/"
)

ICATU_SILVER_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/icatu_processada/"
)

OFFSHORE_US_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/btg/onedrive/receita/offshore_us/"
)

OFFSHORE_US_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/btg/onedrive/receita/offshore_us/"
)

OFFSHORE_US_SILVER_PRE_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/offshore_us_pre_processada/"
)

OFFSHORE_US_SILVER_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/offshore_us_processada/"
)

OFFSHORE_CY_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/btg/onedrive/receita/offshore_cy/"
)

OFFSHORE_CY_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/btg/onedrive/receita/offshore_cy/"
)

OFFSHORE_CY_SILVER_PRE_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/offshore_cy_pre_processada/"
)

OFFSHORE_CY_SILVER_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/offshore_cy_processada/"
)

ONEWM_LANDING = Dataset(f"s3a://{bucket_lake_landing}/btg/onedrive/receita/onewm/")

ONEWM_BRONZE = Dataset(f"s3a://{bucket_lake_bronze}/btg/onedrive/receita/onewm/")

ONEWM_SILVER_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/onewm_processada/"
)

OURIBANK_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/btg/onedrive/receita/ouribank/"
)

OURIBANK_BRONZE = Dataset(f"s3a://{bucket_lake_bronze}/btg/onedrive/receita/ouribank/")

OURIBANK_SILVER_PRE_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/ouribank_pre_processada/"
)

OURIBANK_SILVER_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/ouribank_processada/"
)

PREV_BTG_LANDING = Dataset(f"s3a://{bucket_lake_landing}/btg/onedrive/receita/prevbtg/")

PREV_BTG_BRONZE = Dataset(f"s3a://{bucket_lake_bronze}/btg/onedrive/receita/prevbtg/")

PREV_BTG_SILVER_PRE_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/prevbtg_pre_processada/"
)

PREV_BTG_SILVER_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/prevbtg_processada/"
)

SEMANAL_LANDING = Dataset(f"s3a://{bucket_lake_landing}/btg/onedrive/receita/semanal/")

SEMANAL_BRONZE = Dataset(f"s3a://{bucket_lake_bronze}/btg/onedrive/receita/semanal/")

SEMANAL_SILVER_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/semanal/"
)

PRUDENTIAL_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/btg/onedrive/receita/prudential/"
)

PRUDENTIAL_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/btg/onedrive/receita/prudential/"
)

PRUDENTIAL_SILVER_PROCESSADA = Dataset(
    f"s3a://{bucket_lake_silver}/btg/onedrive/receita/prudential/"
)

PROJETOS_TECH_NOTION_API_LANDING = Dataset(
    f"s3a://{bucket_lake_landing}/notion/api_notion/tecnologia/projetos/"
)

PROJETOS_TECH_NOTION_API_BRONZE = Dataset(
    f"s3a://{bucket_lake_bronze}/notion/api_notion/tecnologia/projetos/"
)

PROJETOS_TECH_NOTION_API_GOLD = Dataset(
    f"s3a://{bucket_lake_gold}/notion/api_notion/tecnologia/projetos/"
)

# Base de e-mails
BASE_EMAILS_SILVER = Dataset(
    f"s3a://{bucket_lake_silver}/one/api_graph/users/mail/list_messages/"
)
