# airflow-dados
Projeto que contém códigos-fonte de pipelines de dados e automações construídas utilizando Airflow

## Pré-requisitos para Deploy
- Terminal Linux (WSL2 instalado é recomendado no caso da utilização de Windows) ([Documentação](https://learn.microsoft.com/pt-br/windows/wsl/install))
- CLI da AWS instalada e logada na conta e region desejadas. Usuário IAM deve ter permissões para realizar o push no repositório de ECR desejado ([Documentação](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html))
- kube_ctl instalado e logado no cluster desejado. Usuário IAM deve estar com acessos configurados no aws_auth configmap do cluster de Kubernetes ([Documentação](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/))
- Docker Desktop instalado e ligado ([Documentação](https://docs.docker.com/desktop/wsl/))

## Instruções para Deploy
1. Executar comando `./deploy.sh` na raíz do repositório que, onde está o Dockerfile