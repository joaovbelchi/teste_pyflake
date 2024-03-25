import json
import requests
import uuid
from utils.secrets_manager import get_secret_value
from urllib.request import urlopen


def create_session() -> requests.Session:
    """Cria sessão com cabeçalho de autenticação
    para fluxo de autorização na API do BTG

    Returns:
        requests.Session: Sessão autenticada.
    """
    creds = json.loads(get_secret_value("prd/apis/btgpactual"))
    session = requests.Session()
    session.auth = (creds["client_id"], creds["client_secret"])
    return session


def get_token_btg(session: requests.Session) -> str:
    """Usa sessão com cabeçalho de autenticação para obter token
    de acesso para API do BTG.

    Args:
        session (requests.Session): Sessão autenticada

    Returns:
        str: Token de acesso para API do BTG
    """
    url_auth = "https://api.btgpactual.com/iaas-auth"
    header = {"x-id-partner-request": str(uuid.uuid4())}
    data = {"grant_type": "client_credentials"}
    return session.post(
        f"{url_auth}/api/v1/authorization/oauth2/accesstoken", headers=header, data=data
    ).headers["access_token"]


def get_position_partner(session: requests.Session, token: str) -> bytes:
    """Obtém resultados de API get-partner-position do BTG

    Args:
        session (requests.Session): Sessão autenticada
        token (str): Token de acesso

    Returns:
        bytes: Conteúdo do download em bytes
    """
    # Coleta URL Download Posição
    url_posicao = "https://api.btgpactual.com/iaas-api-position"
    header = {"x-id-partner-request": str(uuid.uuid4()), "access_token": token}
    rsp = session.get(f"{url_posicao}/api/v1/position/partner", headers=header)

    # Baixa Posição
    url_download = rsp.json()["response"]["url"]
    return urlopen(url_download).read()


def get_position_account(
    session: requests.Session, token: str, account: str
) -> requests.Response:
    """Obtém resultados de API get-position-by-account do BTG

    Args:
        session (requests.Session): Sessão autenticada
        token (str): Token de acesso
        account (str): Conta para a qual se deseja obter a posição atualizada

    Returns:
        requests.Response: Response da chamada da API
    """
    url_posicao = "https://api.btgpactual.com/iaas-api-position"
    header = {"x-id-partner-request": str(uuid.uuid4()), "access_token": token}
    return session.get(f"{url_posicao}/api/v1/position/{account}", headers=header)


def get_position_account_date(
    session: requests.Session,
    token: str,
    account: str,
    date: str,
) -> requests.Response:
    """Obtém resultados de API get-position-by-account do BTG

    Args:
        session (requests.Session): Sessão autenticada
        token (str): Token de acesso
        account (str): Conta para a qual se deseja obter a posição atualizada
        date (str): Data para a qual se deseja obtwr a posição da conta

    Returns:
        requests.Response: Response da chamada da API
    """
    url_posicao = "https://api.btgpactual.com/iaas-api-position"
    header = {"x-id-partner-request": str(uuid.uuid4()), "access_token": token}
    data = {"date": date}
    return session.post(
        f"{url_posicao}/api/v1/position/{account}", headers=header, json=data
    )
