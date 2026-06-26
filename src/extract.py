import os
import time
import logging
import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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

# Back-off base para rate limit (segundos); dobra a cada tentativa
RATE_LIMIT_BACKOFF_BASE = 30

# Tempo máximo de espera para lock distribuído (segundos)
LOCK_WAIT_TIMEOUT = 60

# Identificador da linha de auth na tabela api_auth
AUTH_ROW_ID = 1


class Extract:
    """
    Responsável por autenticar com a API do RD Station CRM e extrair dados
    paginados para cada recurso configurado no pipeline.

    Autenticação:
        - Utiliza OAuth2 com Refresh Token Rotation.
        - O access_token e refresh_token ficam armazenados em silver.api_auth.
        - A renovação é LAZY: só ocorre quando a API retorna 401.
        - Um lock distribuído via Supabase evita corrida entre processos.
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

        # Sessão HTTP com retry automático para erros de rede (5xx)
        self.session = self._criar_sessao_http()

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

    @staticmethod
    def _criar_sessao_http() -> requests.Session:
        """
        Cria sessão com retry automático para falhas de rede (5xx).
        NÃO faz retry em 401/429 — esses são tratados manualmente
        para ter controle total sobre o fluxo de tokens.
        """
        session = requests.Session()
        retry = Retry(
            total=int(os.getenv("HTTP_MAX_RETRIES", "3")),
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://",  adapter)
        return session

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

        Fluxo:
          1. Adquire lock distribuído no Supabase (previne race condition).
          2. Relê o token atual do banco (pode ter sido renovado por outra instância).
          3. Verifica se o access_token em memória ainda é o mesmo do banco —
             se divergir, outra instância já renovou; apenas atualiza a memória.
          4. Se for o mesmo, chama o endpoint de renovação.
          5. Salva atomicamente e libera o lock.
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

        lock_adquirido = self._adquirir_lock()
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
            resp = self.session.post(RD_TOKEN_URL, json=payload, timeout=30)

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
            if lock_adquirido:
                self._liberar_lock()

    # -----------------------------------------------------------------------
    # Lock distribuído (previne race condition entre instâncias paralelas)
    # -----------------------------------------------------------------------

    def _adquirir_lock(self) -> bool:
        """
        Tenta adquirir um lock de renovação de token no Supabase.
        Aguarda até LOCK_WAIT_TIMEOUT segundos antes de prosseguir sem lock.
        Retorna True se o lock foi adquirido, False caso contrário.

        Implementação simples via coluna `renovando` na tabela api_auth.
        Para produção de alta concorrência, substituir por pg_advisory_lock via RPC.
        """
        deadline = time.time() + LOCK_WAIT_TIMEOUT
        while time.time() < deadline:
            try:
                # Tenta setar renovando=True apenas se ainda for False
                resp = (
                    self.supabase.table("api_auth")
                    .update({"renovando": True})
                    .eq("id", AUTH_ROW_ID)
                    .eq("renovando", False)   # condição: só atualiza se False
                    .execute()
                )
                # Se atualizou alguma linha, adquirimos o lock
                if resp.data:
                    self.logger.debug("Lock de renovação adquirido.")
                    return True
            except Exception as e:
                self.logger.warning(f"Erro ao tentar adquirir lock: {e}")

            self.logger.debug("Aguardando lock de renovação ser liberado...")
            time.sleep(5)

        self.logger.warning(
            "Timeout ao aguardar lock de renovação. "
            "Prosseguindo sem lock — verificação de token duplicado está ativa."
        )
        return False

    def _liberar_lock(self) -> None:
        """Libera o lock de renovação."""
        try:
            self.supabase.table("api_auth").update(
                {"renovando": False}
            ).eq("id", AUTH_ROW_ID).execute()
            self.logger.debug("Lock de renovação liberado.")
        except Exception as e:
            self.logger.warning(f"Falha ao liberar lock (será resolvido no próximo run): {e}")

    # -----------------------------------------------------------------------
    # Requisição blindada (lazy refresh + rate limit)
    # -----------------------------------------------------------------------

    def _headers(self) -> dict:
        """Retorna os headers de autorização com o token atual em memória."""
        if not self._access_token:
            # Primeira requisição — carrega do banco
            self._carregar_tokens_do_supabase()
        return {"Authorization": self._access_token}

    def _requisicao_blindada(
        self,
        url: str,
        params: dict = None,
        tentativa: int = 0,
        max_tentativas: int = 3,
    ) -> dict:
        """
        Executa uma requisição GET com tratamento de:
          - 401 Unauthorized → renova token (lazy) e retenta
          - 429 Too Many Requests → espera com back-off exponencial
          - Erros de rede → gerenciados pelo retry da sessão HTTP

        Levanta:
          - TokenError  : quando a renovação falha irrecuperavelmente
          - ExtractionError : quando o limite de tentativas é atingido
        """
        resp = self.session.get(
            url,
            headers=self._headers(),
            params=params,
            timeout=30,
        )

        # ---- 401: token expirado ----
        if resp.status_code == 401:
            if tentativa >= max_tentativas:
                raise TokenError(
                    f"Ainda recebendo 401 após {tentativa} renovações de token. "
                    "A cadeia de tokens pode estar corrompida."
                )
            self.logger.warning(f"401 em {url}. Renovando token...")
            self._renovar_token()
            return self._requisicao_blindada(url, params, tentativa + 1, max_tentativas)

        # ---- 429: rate limit ----
        if resp.status_code == 429:
            wait = RATE_LIMIT_BACKOFF_BASE * (2 ** tentativa)
            self.logger.warning(
                f"429 Rate Limit em {url}. Aguardando {wait}s antes de retentar..."
            )
            time.sleep(wait)
            if tentativa >= max_tentativas:
                raise ExtractionError(
                    f"Rate limit persistente em {url} após {tentativa} tentativas."
                )
            return self._requisicao_blindada(url, params, tentativa + 1, max_tentativas)

        # ---- outros erros HTTP ----
        if not resp.ok:
            raise ExtractionError(
                f"Erro HTTP {resp.status_code} em {url}: {resp.text[:300]}"
            )

        # ---- parse JSON ----
        try:
            return resp.json()
        except ValueError as e:
            raise ExtractionError(
                f"Resposta não é JSON válido de {url}: {e} | Body: {resp.text[:200]}"
            ) from e

    # -----------------------------------------------------------------------
    # Extração paginada
    # -----------------------------------------------------------------------

    def extrair_recurso(
        self,
        recurso: str,
        endpoint: str,
        chave_dados: str,
        params_extras: dict = None,
    ) -> list:
        """
        Extrai todos os registros de um endpoint paginado.

        Args:
            recurso     : Nome legível do recurso (usado nos logs).
            endpoint    : Path relativo, ex: "/deals", "/contacts".
            chave_dados : Chave no JSON da resposta que contém a lista, ex: "deals".
            params_extras : Parâmetros adicionais de filtro (ex: {"win": "true"}).

        Returns:
            Lista com todos os registros coletados.

        Raises:
            ExtractionError : se a extração falhar definitivamente.
            TokenError      : se os tokens estiverem irrecuperáveis.
        """
        url    = f"{RD_API_BASE_URL}{endpoint}"
        pagina = 1
        total  = 0
        dados  = []

        params = {"page": pagina, "limit": 200}
        if params_extras:
            params.update(params_extras)

        self.logger.info(f"[{recurso}] Iniciando extração | params: {params_extras or {}}")

        while True:
            params["page"] = pagina

            try:
                resposta = self._requisicao_blindada(url, params)
            except TokenError:
                # Erros de token interrompem TODO o pipeline — re-levanta
                raise
            except ExtractionError as e:
                # Erros de extração: loga e aborta apenas este recurso
                self.logger.error(
                    f"[{recurso}] Falha na página {pagina}: {e}. "
                    "Abortando extração deste recurso."
                )
                raise

            registros = resposta.get(chave_dados, [])
            total_api  = resposta.get("total", None)

            if not registros:
                self.logger.info(
                    f"[{recurso}] Página {pagina}: sem mais registros. "
                    f"Total extraído: {total}."
                )
                break

            dados.extend(registros)
            total += len(registros)

            self.logger.info(
                f"[{recurso}] Página {pagina}: {len(registros)} registros "
                f"(acumulado: {total}{f'/{total_api}' if total_api else ''})."
            )

            # Verifica se chegamos à última página
            has_more = resposta.get("has_more", None)
            if has_more is False:
                break
            if has_more is None:
                # Fallback: para se a página veio vazia ou menos que o limit
                if len(registros) < params["limit"]:
                    break

            pagina += 1

        self.logger.info(f"[{recurso}] Extração concluída. {total} registros no total.")
        return dados

    # -----------------------------------------------------------------------
    # Extração especial: Deals (3 varreduras para cobrir todos os estados)
    # -----------------------------------------------------------------------

    def extrair_deals(self) -> list:
        """
        A API do RD Station, por padrão, oculta negócios fechados (ganhos/perdidos).
        São necessárias 3 chamadas para cobrir todos os estados:
          1. Ganhos       (?win=true)
          2. Perdidos     (?win=false)
          3. Em aberto    (sem filtro de win)

        Usa um set de IDs para deduplicar caso haja sobreposição.
        """
        self.logger.info("[deals] Iniciando extração com 3 varreduras (ganhos/perdidos/abertos).")

        todos_os_deals = {}  # id → deal (deduplicação automática)

        varreduras = [
            {"win": "true",  "label": "ganhos"},
            {"win": "false", "label": "perdidos"},
            {                "label": "abertos"},  # sem filtro de win
        ]

        for varredura in varreduras:
            label  = varredura.pop("label")
            params = varredura  # pode ser {} ou {"win": "true"/"false"}
            try:
                registros = self.extrair_recurso(
                    recurso     = f"deals/{label}",
                    endpoint    = "/deals",
                    chave_dados = "deals",
                    params_extras=params if params else None,
                )
                novos = 0
                for deal in registros:
                    deal_id = deal.get("id")
                    if deal_id and deal_id not in todos_os_deals:
                        todos_os_deals[deal_id] = deal
                        novos += 1
                self.logger.info(
                    f"[deals/{label}] {len(registros)} registros retornados, "
                    f"{novos} novos (únicos)."
                )
            except ExtractionError as e:
                # Uma varredura falha não cancela as outras
                self.logger.error(
                    f"[deals/{label}] Falha nesta varredura: {e}. "
                    "Continuando com as demais varreduras."
                )

        resultado = list(todos_os_deals.values())
        self.logger.info(
            f"[deals] Extração completa. {len(resultado)} deals únicos no total."
        )
        return resultado