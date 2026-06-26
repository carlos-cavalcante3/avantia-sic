import os
import logging

from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

class TokenError(Exception):
    """Erro irrecuperável na cadeia de tokens OAuth2. Deve interromper o ETL."""
    pass


class ExtractionError(Exception):
    """Erro recuperável em um recurso específico. O pipeline pode continuar."""
    pass


# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

RD_API_BASE_URL = "https://crm.rdstation.com/api/v1"

# Identificador da linha de auth na tabela api_auth
AUTH_ROW_ID = 1


class Extract:
    """
    Responsável por autenticar com a API do RD Station CRM e extrair dados.
    """

    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or self._configurar_logger()

        # Credenciais estáticas (não mudam entre runs)
        self.client_id     = os.environ["RD_CLIENT_ID"]
        self.client_secret = os.environ["RD_CLIENT_SECRET"]

        # Cliente Supabase — usa service_role para gravar tokens
        supabase_url = os.environ["SUPABASE_URL"]
        supabase_key = os.environ["SUPABASE_KEY"]  # deve ser a service_role key
        self.supabase: Client = create_client(supabase_url, supabase_key)

        # Tokens são carregados sob demanda (lazy), não no __init__
        self._access_token  = None
        self._refresh_token = None

        self.logger.info("Extract inicializado. Token será carregado na primeira requisição.")

    # -----------------------------------------------------------------------
    # Configuração
    # -----------------------------------------------------------------------

    @staticmethod
    def _configurar_logger() -> logging.Logger:
        logger = logging.getLogger("extract")
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
            ))
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    # -----------------------------------------------------------------------
    # Gerenciamento de tokens (lazy + atômico)
    # -----------------------------------------------------------------------

    def _carregar_tokens_do_supabase(self) -> None:
        """
        Lê o par de tokens mais recente do Supabase.
        Sempre relê antes de qualquer renovação para garantir
        que está usando o token mais atual (outra instância pode ter
        rotacionado antes desta).
        """
        try:
            resp = (
                self.supabase
                .table("api_auth")
                .select("access_token, refresh_token")
                .eq("id", AUTH_ROW_ID)
                .single()
                .execute()
            )
            data = resp.data
            if not data or not data.get("refresh_token"):
                raise TokenError(
                    "Tabela api_auth está vazia ou sem refresh_token. "
                    "Execute o fluxo de autorização inicial via Postman."
                )
            self._access_token  = data["access_token"]
            self._refresh_token = data["refresh_token"]
            self.logger.info("Tokens carregados do Supabase com sucesso.")
        except TokenError:
            raise
        except Exception as e:
            raise TokenError(f"Falha ao ler tokens do Supabase: {e}") from e