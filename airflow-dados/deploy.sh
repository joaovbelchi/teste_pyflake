#!/bin/bash

# Função para obter o valor da Conta AWS
obter_conta_aws() {
    local account_value=$(aws sts get-caller-identity --query "Account" --output text)
    if [ $? -ne 0 ]; then
        echo "Erro ao executar 'aws sts get-caller-identity'"
        exit 1
    fi
    echo "$account_value"
}

# Função para obter o valor da Região AWS
obter_regiao_aws() {
    local region_value=$(aws configure get region)
    if [ $? -ne 0 ]; then
        echo "Erro ao executar 'aws configure get region'"
        exit 1
    fi
    echo "$region_value"
}

# Função para obter o ARN do cluster de k8s conectado
obter_arn_cluster() {
    local arn_cluster=$(kubectl config current-context)
    if [ $? -ne 0 ]; then
        echo "Erro ao executar 'kubectl config current-context'"
        exit 1
    fi
    echo "$arn_cluster"
}

# Função para solicitar confirmação
solicitar_confirmacao() {
    read -p "Deseja prosseguir com a execução do script? (s/N): " confirm
    confirm=${confirm,,}
    if [ "$confirm" != "s" ]; then
        echo "Execução do script cancelada."
        exit 0
    fi
}

# Função para obter nome dos pods que devem puxar uma imagem nova do ECR
obter_nome_dos_pods() {
    kubectl get pods -n airflow | grep -E "scheduler|triggerer|webserver" | awk '{print $1}'
}

account_value=$(obter_conta_aws)
echo "Conta AWS: $account_value"

region_value=$(obter_regiao_aws)
echo "Região AWS: $region_value"

repo_name="airflow-v2"
echo "Repositório ECR: $repo_name"

cluster_arn=$(obter_arn_cluster)
echo "Cluster Kubernetes: $cluster_arn"

solicitar_confirmacao

aws ecr get-login-password --region $region_value | docker login --username AWS --password-stdin ${account_value}.dkr.ecr.${region_value}.amazonaws.com
docker build -t ${repo_name} ./${input_value}/
docker tag ${repo_name}:latest ${account_value}.dkr.ecr.${region_value}.amazonaws.com/${repo_name}:latest
docker push ${account_value}.dkr.ecr.${region_value}.amazonaws.com/${repo_name}:latest

pod_names=$(obter_nome_dos_pods)
if [ -n "$pod_names" ]; then
    kubectl delete pod -n airflow $pod_names
else
    echo "Não foi encontrado nenhum pod de scheduler, triggerer ou webserver em execução. Nenhum pod foi excluído!"
fi