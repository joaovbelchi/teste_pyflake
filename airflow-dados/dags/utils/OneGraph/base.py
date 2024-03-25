import msal
from utils.OneGraph.requests import create_http_session
from requests import Response


def refresh_token_decorator(func):
    """Decorator que garante que o token de acesso é valido
    antes da execução de métodos que interagem com a API da Graph

    Args:
        func: Função encapsulada pelo decorator
    """

    def wrapper(self, *args, **kwargs):
        self._refresh_token()  # Execute o método _refresh_token antes de chamar o método original
        return func(self, *args, **kwargs)

    return wrapper


class OneGraphBase:
    """Classe base para interagir com a API do Microsoft Graph"""

    GRAPH_API_URL = "https://graph.microsoft.com/v1.0"
    CONTENT_TYPES = {
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xls": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xlsm": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".csv": "text/csv",
        ".parquet": "application/octet-stream",
        ".pkl": "application/octet-stream",
    }

    def __init__(self, client_id: str, client_secret: str, tenant_id: str):
        """
        Args:
            client_id (str): ID do registro de aplicativo
            client_secret (str): Chave secretaa do registro de aplicativo
            tenant_id (str): ID do Tenant no qual o aplicativo reside
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id

        # Cliente de autenticação da Microsoft
        self.client = msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret,
        )

        # Header com token de acesso
        self.auth_header = None

        # Sessão HTTP/HTTPS
        self.http_session = create_http_session()

    def _refresh_token(self):
        """Garante que o valor de 'self.token' é um token de
        acesso válido
        """

        scope = ["https://graph.microsoft.com/.default"]

        # First, try to lookup an access token in cache
        token_result = self.client.acquire_token_silent(scope, account=None)

        # If the token is available in cache, save it to a variable
        if token_result:
            access_token = "Bearer " + token_result["access_token"]

        # If the token is not available in cache, acquire a new one from Azure AD and save it to a variable
        else:
            token_result = self.client.acquire_token_for_client(scopes=scope)
            access_token = "Bearer " + token_result["access_token"]

        self.auth_header = {"Authorization": access_token}

    def _api_call(
        self, method: str, endpoint: str = None, custom_url: str = None, **kwargs
    ) -> Response:
        """Abstrai a lógica de realização de chamadas na API do Graph, ou em alguma
        outra URL customizada. Adiciona o cabeçalho com token de autenticação,
        concatena a URL da API do Graph com o endpoint desejado e realiza a requisição
        utilizando uma sessão HTTP com regras customizadas.

        Args:
            method (str): Método da request. 'GET', 'POST', etc
            endpoint (str, optional): Endpoint da API do Graph para o qual se
            deseja enviar a requisição
            custom_url (str, optional): Caso seja necessário enviar uma requisição
            para alguma outra URL, como no caso de uma requisição PUT em uma URL
            de sessão de upload, passar o valor da URL neste parâmetro

        Returns:
            Response: Response da requisição HTTP
        """
        url = custom_url if custom_url else f"{self.GRAPH_API_URL}/{endpoint}"
        headers = kwargs.pop("headers", {})
        merged_headers = {**self.auth_header, **headers}

        return self.http_session.request(method, url, headers=merged_headers, **kwargs)
