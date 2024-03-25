from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from requests import Session, Response


def http_rise_for_status_hook(response: Response, *args, **kwargs) -> None:
    """Esta função irá subir uma exceção caso a requisição https resulte em um
    código de erro.

    Args:
        response (Response): Objeto Response da lib requests

    """
    response.raise_for_status()

    return None


def create_http_session() -> Session:
    """Esta função retorna um objeto do tipo Session que realiza requisições HTTP e HTTPS.
    Este objeto possui políticas de retry predefinidas mas que podem ser personalizadas
    pelo parâmetro politica_retry que deve ser um objeto do tipo Retry da biblioteca urllib3.
    A politica de retry padrão irá fazer 5 tentativas quando a requisição retornar os códigos
    de erro 429, 500, 502, 503 e 504, entre cada tentativa será aplicado os seguintes time sleeps:
    2, 4, 8, 16 e 32. Caso não haja sucesso nas tentativas ou a requisição retorne um outro
    código de erro está função irá subir uma exceção indicando falha na requisição.

    Args:
        politica_retry (Retry, optional): Este parametro recebe uma politica de Retry customizada. Defaults to None.

    Returns:
        Session: Objeto Session da lib requests
    """
    retry_policy = Retry(
        total=5,  # Número de retrys
        status_forcelist=[
            429,
            500,
            502,
            503,
            504,
        ],  # Status codes que acionam o retry
        allowed_methods=[
            "HEAD",
            "GET",
            "POST",
            "PUT",
            "DELETE",
            "OPTIONS",
            "TRACE",
        ],  # Métodos de requisição que funcionam com retry
        backoff_factor=1,  # Time sleep entre tentativas, metodo de calculo: {backoff factor} * (2 ** ({number of previous retries}))
    )

    # Cria um adapter com a politica de retry
    adapter = HTTPAdapter(max_retries=retry_policy)

    # Cria a session e inclui as politicas de retry.
    session = Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Caso a requisição retorne código de erro sobe uma exceção
    session.hooks["response"].append(http_rise_for_status_hook)

    return session
