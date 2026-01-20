import requests
from tenacity import stop_after_attempt, wait_exponential, retry_if_exception_type, retry


class ApiClient:
    """
    Cliente HTTP con:
    - Auth (token bearer)
    - Reintentos con backoff exponencial para errores de red/HTTP
    """

    def __init__(self, base_url: str, timeout_seconds: int = 20):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.token: str | None = None

    def auth(self, user: str, password: str) -> None:
        resp = self.session.post(
            f"{self.base_url}/auth",
            json={"user": user, "password": password},
            timeout=self.timeout_seconds,
        )
        resp.raise_for_status()
        data = resp.json()
        token = data.get("access_token") or data.get("token")
        if not token:
            raise ValueError("Auth response does not contain access_token/token")
        self.token = str(token)
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=20),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def get_json(self, path: str):
        resp = self.session.get(
            f"{self.base_url}{path}",
            timeout=self.timeout_seconds,
        )
        resp.raise_for_status()
        return resp.json()