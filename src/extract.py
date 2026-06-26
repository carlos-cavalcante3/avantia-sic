import os
import logging
import requests

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

RD_TOKEN_URL    = "https://api.rd.services/auth/token"
RD_API_BASE_URL = "https://crm.rdstation.com/api/v1"

# Número máximo de tentativas de renovação de token por execução
MAX_TOKEN_RENEWAL_ATTEMPTS = 3

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

        # Contador de renovações nesta execução (evita loop infinito)
        self._token_renewal_count = 0

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

    def _salvar_tokens_no_supabase(self, access_token: str, refresh_token: str) -> None:
        """
        Persiste o novo par de tokens de forma atômica.
        Se esta operação falhar, levanta TokenError imediatamente —
        pois o token antigo já foi invalidado pela RD Station e
        não há como recuperar sem intervenção manual.
        """
        try:
            self.supabase.table("api_auth").update({
                "access_token":  access_token,
                "refresh_token": refresh_token,
            }).eq("id", AUTH_ROW_ID).execute()
            self.logger.info("Novos tokens salvos no Supabase com sucesso.")
        except Exception as e:
            # Situação crítica: token novo foi gerado mas não persistido.
            # A cadeia está quebrada — é necessário reautorizar manualmente.
            raise TokenError(
                f"[CRÍTICO] Token renovado na RD Station mas FALHOU ao salvar no Supabase. "
                f"A cadeia de tokens está quebrada. Reautorize manualmente. Erro: {e}"
            ) from e

    def _renovar_token(self) -> None:
        """
        Renova o par de tokens via refresh_token.
        """
        if self._token_renewal_count >= MAX_TOKEN_RENEWAL_ATTEMPTS:
            raise TokenError(
                f"Limite de {MAX_TOKEN_RENEWAL_ATTEMPTS} renovações de token atingido "
                "nesta execução. Interrompendo para evitar invalidação em loop."
            )

        self.logger.warning(
            f"Token expirado ou inválido. Iniciando renovação "
            f"(tentativa {self._token_renewal_count + 1}/{MAX_TOKEN_RENEWAL_ATTEMPTS})..."
        )

        try:
            # Relê o banco — outra instância pode ter renovado enquanto esperávamos o lock
            token_em_memoria = self._access_token
            self._carregar_tokens_do_supabase()

            if self._access_token != token_em_memoria and token_em_memoria is not None:
                # Outra instância já renovou; usa o token novo do banco
                self.logger.info(
                    "Token já foi renovado por outra instância. "
                    "Usando o token atualizado do Supabase."
                )
                self._token_renewal_count += 1
                return  # não precisa chamar a API de renovação

            # Chama o endpoint de renovação com o refresh_token atual
            payload = {
                "client_id":     self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self._refresh_token,
                "grant_type":    "refresh_token",
            }
            resp = requests.post(RD_TOKEN_URL, json=payload, timeout=30)

            if resp.status_code == 401:
                raise TokenError(
                    "REFRESH_TOKEN INVÁLIDO (401). A cadeia de tokens está quebrada. "
                    "Execute o fluxo de autorização inicial: obtenha um novo 'code' "
                    "no navegador, troque por access_token + refresh_token via Postman "
                    "(grant_type=authorization_code) e insira AMBOS na tabela api_auth."
                )

            if not resp.ok:
                raise TokenError(
                    f"Falha ao renovar token. Status {resp.status_code}: {resp.text}"
                )

            dados = resp.json()
            novo_access  = dados.get("access_token")
            novo_refresh = dados.get("refresh_token")

            if not novo_access or not novo_refresh:
                raise TokenError(
                    f"Resposta de renovação não contém os tokens esperados: {dados}"
                )

            # Persiste atomicamente (levanta TokenError se falhar)
            self._salvar_tokens_no_supabase(novo_access, novo_refresh)

            # Atualiza memória
            self._access_token  = novo_access
            self._refresh_token = novo_refresh
            self._token_renewal_count += 1

            self.logger.info("Token renovado e persistido com sucesso.")

        finally:
            pass