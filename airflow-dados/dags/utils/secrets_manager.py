import boto3
import os


def get_secret_value(
    secret_name, region_name="us-east-1", append_prefix: bool = False
) -> str:
    """Coleta credenciais armazenadas no Secrets Manager. Adiciona
    sufixo ao nome do segredo, a depender do valor atribuído
    para a variável de ambiente 'AMBIENTE'.

    Args:
        secret_name (str): Nome do segredo sem o sufixo 'prd/' ou 'dev/'.
        region_name (str, optional): Region do Secret. Valor default: "us-east-1".
        append_prefix (bool, optional): Determina se deve ser adicionado
        um prefixo ao nome do secret, com o valor atribuído à variável de
        ambiente 'AMBIENTE'.

    Returns:
        str: String em formato JSON contendo as credenciais.
    """
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    if append_prefix:
        secret_name = f"{os.environ['AMBIENTE']}/{secret_name}"

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    return get_secret_value_response["SecretString"]
