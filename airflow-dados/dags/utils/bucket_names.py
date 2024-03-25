import os

bucket_names = {
    "whatsapp": {"dev": "one-whatsapp", "prd": "one-whatsapp"},
    "lake-landing": {
        "dev": "dev--dados-lake-landing-oneinvest",
        "prd": "prd-dados-lake-landing-oneinvest",
    },
    "lake-bronze": {
        "dev": "dev--dados-lake-bronze-oneinvest",
        "prd": "prd-dados-lake-bronze-oneinvest",
    },
    "lake-silver": {
        "dev": "dev--dados-lake-silver-oneinvest",
        "prd": "prd-dados-lake-silver-oneinvest",
    },
    "lake-gold": {
        "dev": "dev--dados-lake-gold-oneinvest",
        "prd": "prd-dados-lake-gold-oneinvest",
    },
    "videos-whatsapp": {
        "dev": "videoswpp",
        "prd": "videoswpp",
    },
    "modelos-ml": {
        "dev": "prd-dados-modelos-ml-oneinvest",
        "prd": "prd-dados-modelos-ml-oneinvest",
    },
    "airflow-logs": {
        "dev": "prd-dados-logs-airflow-oneinvest",
        "prd": "prd-dados-logs-airflow-oneinvest",
    },
    "airflow-logs-archive": {
        "dev": "prd-dados-logs-airflow-archive-oneinvest",
        "prd": "prd-dados-logs-airflow-archive-oneinvest",
    },
}


def get_bucket_name(proxy_name: str, env: str = None) -> str:
    """Retorna o nome real do bucket ao passar seu 'proxy_name'.
    Solução adotada para usar o mesmo código em DEV e PRD.

    Args:
        proxy_name (str): Nome da chave que leva ao nome real
        do bucket, que varia por ambiente.

    Returns:
        str: Nome real do bucket
    """

    env = os.environ["AMBIENTE"] if not env else env
    return bucket_names[proxy_name][env]
