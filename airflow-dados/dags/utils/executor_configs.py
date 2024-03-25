from kubernetes.client import models as k8s


def k8s_executor_config_override(cpu: float = 0.5, memory: int = 1000) -> dict:
    """Gera um dicionário a ser passado para o parâmetro 'executor_config'
    de tasks que utilizam o Kubernetes Executor. Configurações padrão dos
    pods alocados para executar as tasks são sobrescritas pelos valores
    passados para essa função. O caso de uso mais comum é personalizar
    quantidade de memória e CPU alocados para o pod.

    Args:
        cpu (float): Número de núcleos de CPU alocados para o pod.
        Pode ser um valor inteiro ou um decimal. Exemplos de valores:
        '1', '2', '1.5'
        memory (int): Valor de memória alocada para o pod em MBs.

    Returns:
        dict: dicionário com chave 'pod_override' e valor igual a instância
        de classe 'k8s.V1Pod', com os devidos parâmetros configurados para
        sobrescrever as configurações dsejadas.
    """
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        resources=k8s.V1ResourceRequirements(
                            requests={"cpu": cpu, "memory": f"{memory}M"},
                            limits={"cpu": cpu, "memory": f"{memory}M"},
                        ),
                    )
                ]
            )
        )
    }
