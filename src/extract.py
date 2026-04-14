import requests
import os
import time
from typing import Dict, Any, List
from dotenv import load_dotenv, set_key

ENV_PATH = ".env"


class Extract:
    """
    Classe responsável por extrair dados da API do RD Station CRM.
    Gerencia a autenticação, renovação de tokens, controle de taxa e paginação.
    """

    def __init__(self):
        """
        Carrega as variáveis de ambiente e inicializa as configurações da API.
        """
        load_dotenv(ENV_PATH)

        self.base_url = "https://api.rd.services/crm/v2"

        self.access_token = os.getenv("RD_ACCESS_TOKEN")
        self.refresh_token = os.getenv("RD_REFRESH_TOKEN")
        self.client_id = os.getenv("RD_CLIENT_ID")
        self.client_secret = os.getenv("RD_CLIENT_SECRET")

        self.token_created_at = time.time()
        self.token_expires_in = 3600

        self.session = requests.Session()
        self._set_headers()

        self.request_interval = 0.6  
        self.max_retries = 5

        self.endpoints = [
            "deals",
            "contacts",
            "organizations",
            "users",
            "tasks",
            "pipelines",
        ]

    def _set_headers(self) -> None:
        """Configura os cabeçalhos padrão da sessão HTTP."""
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.access_token}",
                "Accept": "application/json",
            }
        )

    def _token_expired(self) -> bool:
        """Verifica se o token de acesso expirou ou está prestes a expirar."""
        return (time.time() - self.token_created_at) > (self.token_expires_in - 60)

    def _update_env(self) -> None:
        """Atualiza o arquivo .env com os novos tokens gerados."""
        set_key(ENV_PATH, "RD_ACCESS_TOKEN", self.access_token)
        set_key(ENV_PATH, "RD_REFRESH_TOKEN", self.refresh_token)

    def _refresh_token(self) -> None:
        """
        Solicita um novo par de tokens de acesso e atualização à API.
        """
        print("[AUTH] Refresh token")
        url = "https://api.rd.services/auth/token"

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        response = requests.post(url, data=payload)

        if response.status_code == 200:
            tokens = response.json()
            self.access_token = tokens["access_token"]
            self.refresh_token = tokens["refresh_token"]
            self.token_created_at = time.time()

            self._set_headers()
            self._update_env()
        else:
            raise Exception(f"Erro ao renovar token: {response.text}")

    def _fix_url(self, url: str) -> str:
        """Ajusta a URL de paginação caso a API retorne um caminho incorreto."""
        if "/api/v2/" in url:
            return url.replace("/api/v2/", "/crm/v2/")
        return url

    def _request_with_retry(self, url: str) -> Dict[str, Any]:
        """
        Executa uma requisição GET com controle de limite de taxa e tentativas automáticas.

        Args:
            url (str): O endpoint a ser consultado.

        Returns:
            Dict[str, Any]: A resposta JSON da requisição.
        """
        for attempt in range(self.max_retries):
            try:
                if self._token_expired():
                    self._refresh_token()

                response = self.session.get(url)

                if response.status_code == 429:
                    wait = 2 ** attempt
                    print(f"[RATE LIMIT] Esperando {wait}s...")
                    time.sleep(wait)
                    continue

                if response.status_code >= 500:
                    wait = 2 ** attempt
                    print(f"[SERVER ERROR] Retry em {wait}s...")
                    time.sleep(wait)
                    continue

                if response.status_code == 401:
                    self._refresh_token()
                    continue

                response.raise_for_status()

                time.sleep(self.request_interval)
                return response.json()

            except requests.exceptions.RequestException as e:
                wait = 2 ** attempt
                print(f"[ERRO] {e} | retry em {wait}s")
                time.sleep(wait)

        raise Exception(f"Falha após {self.max_retries} tentativas")

    def _fetch_all_pages(self, endpoint: str) -> List[Dict[str, Any]]:
        """
        Percorre todas as páginas de um endpoint específico.

        Args:
            endpoint (str): O nome da entidade a ser consultada.

        Returns:
            List[Dict[str, Any]]: Lista contendo todos os registros extraídos.
        """
        print(f"[EXTRACT] {endpoint}")
        url = f"{self.base_url}/{endpoint}"
        all_data = []
        page = 1

        while url:
            data = self._request_with_retry(url)
            items = data.get("data", [])
            all_data.extend(items)

            print(f"  página {page} -> {len(items)} registros")
            next_url = data.get("links", {}).get("next")

            if next_url:
                url = self._fix_url(next_url)
                page += 1
            else:
                url = None

        print(f"  -> TOTAL: {len(all_data)} registros")
        return all_data

    def fetch_all(self) -> Dict[str, Any]:
        """
        Orquestra a extração de dados de todos os endpoints configurados.

        Returns:
            Dict[str, Any]: Um dicionário onde as chaves são os endpoints e os valores são listas de registros.
        """
        data = {}

        for endpoint in self.endpoints:
            try:
                data[endpoint] = self._fetch_all_pages(endpoint)
            except Exception as e:
                print(f"[ERRO EXTRACT] {endpoint}: {e}")
                data[endpoint] = []

        return data