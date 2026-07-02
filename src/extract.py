import os
import time
import logging
import requests

from datetime import datetime, timedelta, timezone

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
RD_API_BASE_URL = "https://api.rd.services/crm/v2"

# Número máximo de tentativas de renovação de token por execução
MAX_TOKEN_RENEWAL_ATTEMPTS = 3

# Back-off base para rate limit (segundos); dobra a cada tentativa
RATE_LIMIT_BACKOFF_BASE = 30

# Tempo máximo de espera para lock distribuído (segundos)
LOCK_WAIT_TIMEOUT = 60

# Se um lock estiver ativo há mais tempo que isso, é considerado "travado"
# (ex: runner do GitHub Actions foi cancelado/matou o processo no meio de
# uma renovação e nunca chegou a chamar _liberar_lock). Nesse caso o lock
# é forçadamente liberado — sem isso, TODA execução futura ficaria presa
# esperando um processo que não existe mais.
LOCK_STALE_SECONDS = 120

# Margem de segurança para renovação PROATIVA: renova o token se faltar
# menos que isso para expirar, em vez de esperar o 401 acontecer.
# Importante em CI/CD: a primeira chamada de cada execução não deve
# "gastar" um round-trip descobrindo que o token já morreu.
TOKEN_EXPIRY_BUFFER_SECONDS = 300

# Identificador da linha de auth na tabela api_auth
AUTH_ROW_ID = 1


class Extract:
    """
    Responsável por autenticar com a API do RD Station CRM e extrair dados
    paginados para cada recurso configurado no pipeline.

    Autenticação:
        - Utiliza OAuth2 com Refresh Token Rotation.
        - O access_token, refresh_token e expires_at ficam armazenados em
          silver.api_auth — essa tabela é a ÚNICA fonte de verdade sobre
          o estado da cadeia de tokens, o que é o que torna esse desenho
          seguro para rodar em runners efêmeros (GitHub Actions não tem
          estado entre execuções; o Supabase tem).
        - A renovação é HÍBRIDA: proativa (checa expires_at antes de cada
          lote de requisições) + reativa (ainda trata 401 defensivamente,
          caso o relógio local esteja dessincronizado ou a RD Station
          revogue o token antes do previsto).
        - Um lock distribuído via Supabase evita corrida entre processos,
          com detecção de staleness para não travar automações futuras
          caso um processo morra no meio da renovação.
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
        self._expires_at    = None  # datetime tz-aware ou None

        # Contador de renovações nesta execução (evita loop infinito)
        self._token_renewal_count = 0

        # Sessão HTTP com retry automático para erros de rede (5xx)
        self.session = self._criar_sessao_http()

        self.logger.info("Extract inicializado. Token será carregado/validado na primeira requisição.")

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
    # Gerenciamento de tokens (lazy + proativo + atômico)
    # -----------------------------------------------------------------------

    @staticmethod
    def _parse_timestamp(valor) -> "datetime | None":
        if not valor:
            return None
        try:
            # Supabase retorna ISO 8601; normaliza 'Z' para compatibilidade
            return datetime.fromisoformat(str(valor).replace("Z", "+00:00"))
        except ValueError:
            return None

    def _carregar_tokens_do_supabase(self) -> None:
        """
        Lê o par de tokens mais recente do Supabase (incluindo expires_at).
        Sempre relê antes de qualquer renovação para garantir
        que está usando o token mais atual (outra instância pode ter
        rotacionado antes desta).
        """
        try:
            resp = (
                self.supabase
                .schema("silver")
                .table("api_auth")
                .select("access_token, refresh_token, expires_at")
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
            self._expires_at    = self._parse_timestamp(data.get("expires_at"))
            self.logger.info("Tokens carregados do Supabase com sucesso.")
        except TokenError:
            raise
        except Exception as e:
            raise TokenError(f"Falha ao ler tokens do Supabase: {e}") from e

    def _salvar_tokens_no_supabase(self, access_token: str, refresh_token: str, expires_in: int = None) -> None:
        """
        Persiste o novo par de tokens de forma atômica, junto com o
        timestamp calculado de expiração.
        Se esta operação falhar, levanta TokenError imediatamente —
        pois o token antigo já foi invalidado pela RD Station e
        não há como recuperar sem intervenção manual.
        """
        expira_em_iso = None
        if expires_in:
            expira_em_iso = (
                datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))
            ).isoformat()

        try:
            payload = {
                "access_token":  access_token,
                "refresh_token": refresh_token,
            }
            if expira_em_iso:
                payload["expires_at"] = expira_em_iso

            self.supabase.schema("silver").table("api_auth").update(payload).eq(
                "id", AUTH_ROW_ID
            ).execute()
            self.logger.info(
                f"Novos tokens salvos no Supabase com sucesso."
                + (f" Expira em {expira_em_iso}." if expira_em_iso else "")
            )
        except Exception as e:
            # Situação crítica: token novo foi gerado mas não persistido.
            # A cadeia está quebrada — é necessário reautorizar manualmente.
            raise TokenError(
                f"[CRÍTICO] Token renovado na RD Station mas FALHOU ao salvar no Supabase. "
                f"A cadeia de tokens está quebrada. Reautorize manualmente. Erro: {e}"
            ) from e

    def _token_precisa_renovar(self) -> bool:
        """
        Determina se o token atual deve ser renovado PROATIVAMENTE,
        antes de qualquer requisição, com base no expires_at armazenado.

        Se não houver expires_at registrado (ex: primeira execução após
        a migration, ou token obtido manualmente via Postman), o método
        retorna False — a renovação reativa (401) continua como rede de
        segurança nesse caso.
        """
        if not self._expires_at:
            return False
        limite = self._expires_at - timedelta(seconds=TOKEN_EXPIRY_BUFFER_SECONDS)
        return datetime.now(timezone.utc) >= limite

    def _garantir_token_valido(self) -> None:
        """
        Ponto de entrada único para garantir que há um access_token
        utilizável antes de qualquer requisição. Deve ser chamado no
        início de cada execução do pipeline (ver run.py / pipeline.py)
        e também antes de cada lote de requisições longas.

        Essencial para automação: numa execução via GitHub Actions, o
        primeiro request do dia não pode depender de um 401 reativo
        para descobrir que o token expirou durante as horas em que o
        pipeline ficou ocioso.
        """
        if not self._access_token:
            self._carregar_tokens_do_supabase()

        if self._token_precisa_renovar():
            self.logger.info(
                "Token próximo da expiração (ou já expirado) — renovando proativamente."
            )
            self._renovar_token()

    def _renovar_token(self) -> None:
        """
        Renova o par de tokens via refresh_token.

        Fluxo:
          1. Adquire lock distribuído no Supabase (previne race condition).
          2. Relê o token atual do banco (pode ter sido renovado por outra instância).
          3. Verifica se o access_token em memória ainda é o mesmo do banco —
             se divergir, outra instância já renovou; apenas atualiza a memória.
          4. Se for o mesmo, chama o endpoint de renovação.
          5. Salva atomicamente (incluindo expires_at) e libera o lock.
        """
        if self._token_renewal_count >= MAX_TOKEN_RENEWAL_ATTEMPTS:
            raise TokenError(
                f"Limite de {MAX_TOKEN_RENEWAL_ATTEMPTS} renovações de token atingido "
                "nesta execução. Interrompendo para evitar invalidação em loop."
            )

        self.logger.warning(
            f"Iniciando renovação de token "
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

            # Se, após reler, o token recém-carregado já está válido por
            # tempo suficiente, não há necessidade de rotacionar de novo.
            if self._expires_at and not self._token_precisa_renovar():
                self.logger.info("Token recarregado do banco já é válido. Renovação desnecessária.")
                self._token_renewal_count += 1
                return

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
                    "(grant_type=authorization_code) e insira AMBOS na tabela api_auth. "
                    "Isso exige intervenção manual — a automação via GitHub Actions não "
                    "pode se recuperar sozinha desse estado."
                )

            if not resp.ok:
                raise TokenError(
                    f"Falha ao renovar token. Status {resp.status_code}: {resp.text}"
                )

            dados = resp.json()
            novo_access   = dados.get("access_token")
            novo_refresh  = dados.get("refresh_token")
            novo_expires  = dados.get("expires_in")

            if not novo_access or not novo_refresh:
                raise TokenError(
                    f"Resposta de renovação não contém os tokens esperados: {dados}"
                )

            # Persiste atomicamente (levanta TokenError se falhar)
            self._salvar_tokens_no_supabase(novo_access, novo_refresh, novo_expires)

            # Atualiza memória
            self._access_token  = novo_access
            self._refresh_token = novo_refresh
            self._expires_at = (
                datetime.now(timezone.utc) + timedelta(seconds=int(novo_expires))
                if novo_expires else None
            )
            self._token_renewal_count += 1

            self.logger.info("Token renovado e persistido com sucesso.")

        finally:
            if lock_adquirido:
                self._liberar_lock()

    # -----------------------------------------------------------------------
    # Lock distribuído (previne race condition entre instâncias paralelas,
    # com detecção de staleness para não travar automações futuras)
    # -----------------------------------------------------------------------

    def _adquirir_lock(self) -> bool:
        """
        Tenta adquirir um lock de renovação de token no Supabase.
        Aguarda até LOCK_WAIT_TIMEOUT segundos antes de prosseguir sem lock.
        Retorna True se o lock foi adquirido, False caso contrário.

        Antes de esperar, verifica se um lock existente está "travado"
        (renovando=True há mais de LOCK_STALE_SECONDS) — cenário típico
        quando um workflow do GitHub Actions é cancelado ou atinge
        timeout no meio de uma renovação e nunca chega a liberar o lock.
        Nesse caso, o lock é forçadamente assumido, em vez de deixar
        todas as execuções futuras esperando um processo que não existe mais.
        """
        self._liberar_lock_se_travado()

        deadline = time.time() + LOCK_WAIT_TIMEOUT
        while time.time() < deadline:
            try:
                agora_iso = datetime.now(timezone.utc).isoformat()
                # Tenta setar renovando=True apenas se ainda for False
                resp = (
                    self.supabase.schema("silver").table("api_auth")
                    .update({"renovando": True, "renovando_desde": agora_iso})
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
            self._liberar_lock_se_travado()

        self.logger.warning(
            "Timeout ao aguardar lock de renovação. "
            "Prosseguindo sem lock — verificação de token duplicado está ativa."
        )
        return False

    def _liberar_lock_se_travado(self) -> None:
        """
        Verifica se o lock atual está travado (renovando=True há mais
        tempo que LOCK_STALE_SECONDS) e, se estiver, força a liberação.
        Protege contra deadlock permanente causado por um runner que
        morreu no meio de uma renovação.
        """
        try:
            resp = (
                self.supabase.schema("silver").table("api_auth")
                .select("renovando, renovando_desde")
                .eq("id", AUTH_ROW_ID)
                .single()
                .execute()
            )
            data = resp.data
            if not data or not data.get("renovando"):
                return

            renovando_desde = self._parse_timestamp(data.get("renovando_desde"))
            if not renovando_desde:
                # Lock antigo sem timestamp (pré-migration) — libera por segurança
                self.logger.warning("Lock sem 'renovando_desde' registrado — liberando por segurança.")
                self._liberar_lock()
                return

            idade_segundos = (datetime.now(timezone.utc) - renovando_desde).total_seconds()
            if idade_segundos > LOCK_STALE_SECONDS:
                self.logger.warning(
                    f"Lock travado detectado (ativo há {idade_segundos:.0f}s, "
                    f"limite {LOCK_STALE_SECONDS}s). Provavelmente um processo anterior "
                    "morreu antes de liberar o lock. Forçando liberação."
                )
                self._liberar_lock()
        except Exception as e:
            self.logger.warning(f"Erro ao checar staleness do lock: {e}")

    def _liberar_lock(self) -> None:
        """Libera o lock de renovação."""
        try:
            self.supabase.schema("silver").table("api_auth").update(
                {"renovando": False, "renovando_desde": None}
            ).eq("id", AUTH_ROW_ID).execute()
            self.logger.debug("Lock de renovação liberado.")
        except Exception as e:
            self.logger.warning(f"Falha ao liberar lock (será resolvido no próximo run): {e}")

    # -----------------------------------------------------------------------
    # Requisição blindada (lazy refresh + rate limit)
    # -----------------------------------------------------------------------

    def _headers(self) -> dict:
        """Retorna os headers de autorização, garantindo primeiro que o
        token em memória é válido (renovação proativa se necessário)."""
        self._garantir_token_valido()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def _requisicao_blindada(
        self,
        url: str,
        params: dict = None,
        tentativa: int = 0,
        max_tentativas: int = 3,
    ) -> dict:
        """
        Executa uma requisição GET com tratamento de:
          - 401 Unauthorized → renova token (reativo, rede de segurança) e retenta
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

        # ---- 401: token expirado (rede de segurança; a via proativa
        #           deveria ter evitado isso na maioria dos casos) ----
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

        if resp.status_code in [403, 404]:
            self.logger.warning(f"Endpoint não encontrado ou acesso negado (Status {resp.status_code}): {url}")
            return {"__abort": True}

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

    def extrairProximaUrl(self, cargaDados: dict) -> str:
        if isinstance(cargaDados, dict):
            links = cargaDados.get("links", {})
            if isinstance(links, dict):
                link_proximo = links.get("next", "")
                if isinstance(link_proximo, dict):
                    return link_proximo.get("href", "")
                if isinstance(link_proximo, str):
                    return link_proximo
        return ""

    def corrigirUrlPaginacao(self, urlOriginal: str) -> str:
        if not urlOriginal:
            return ""
        url = urlOriginal.replace("http://", "https://")
        url = url.replace("/api/v2/", "/crm/v2/")
        url = url.replace("/crm/crm/", "/crm/")
        if "api.rd.services" in url and "/crm/v2/" not in url:
            parts = url.split("api.rd.services")
            url = "https://api.rd.services/crm/v2" + parts[-1]
        return url

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
        filtros = ""
        if params_extras:
            filtros = "?" + "&".join([f"{k}={v}" for k, v in params_extras.items()])

        separador = "&" if "?" in filtros else "?"
        url_requisicao = f"{RD_API_BASE_URL}{endpoint}{filtros}{separador}page[number]=1"

        pagina_atual = 1
        dados_globais = []
        urls_visitadas = set()

        self.logger.info(f"[{recurso}] Iniciando extração | params: {params_extras or {}}")

        while url_requisicao:
            if url_requisicao in urls_visitadas:
                self.logger.warning(f"Loop evitado: {url_requisicao}")
                break

            urls_visitadas.add(url_requisicao)
            self.logger.info(f"[{recurso}] Buscando (Pag {pagina_atual}): {url_requisicao}")

            try:
                carga_dados = self._requisicao_blindada(url_requisicao)
            except TokenError:
                raise
            except ExtractionError as e:
                self.logger.error(f"[{recurso}] Falha: {e}. Abortando extração deste recurso.")
                raise

            if isinstance(carga_dados, dict) and carga_dados.get("__abort"):
                break

            registros_extraidos = []
            if isinstance(carga_dados, list):
                registros_extraidos = carga_dados
            elif isinstance(carga_dados, dict):
                if chave_dados in carga_dados and carga_dados[chave_dados]:
                    registros_extraidos = carga_dados[chave_dados]
                elif "data" in carga_dados and carga_dados["data"]:
                    registros_extraidos = carga_dados["data"]
                elif "items" in carga_dados and carga_dados["items"]:
                    registros_extraidos = carga_dados["items"]

            if not registros_extraidos:
                self.logger.info(f"[{recurso}] Página {pagina_atual}: sem mais registros.")
                break

            dados_globais.extend(registros_extraidos)

            url_proxima = self.extrairProximaUrl(carga_dados)
            url_proxima = self.corrigirUrlPaginacao(url_proxima)

            if url_proxima:
                url_requisicao = url_proxima
                pagina_atual += 1
            else:
                indicador = carga_dados.get("has_more", False) if isinstance(carga_dados, dict) else False
                if indicador:
                    pagina_atual += 1
                    url_requisicao = f"{RD_API_BASE_URL}{endpoint}{filtros}{separador}page[number]={pagina_atual}"
                else:
                    break

            time.sleep(0.5)

        self.logger.info(f"[{recurso}] Extração concluída. {len(dados_globais)} registros no total.")
        return dados_globais

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

    # -----------------------------------------------------------------------
    # Adaptador de Compatibilidade (Bridge para o pipeline.py antigo)
    # -----------------------------------------------------------------------

    def extrairRecurso(self, nomeRecurso: str) -> list:
        """
        Garante a retrocompatibilidade com o pipeline.py original que
        ainda chama 'extrairRecurso(nomeRecurso)' em camelCase.
        """
        # Se for deals, roteia para a função especial que faz as 3 varreduras
        if nomeRecurso == "deals":
            return self.extrair_deals()

        # Para os restantes recursos (contacts, users, pipelines, etc.),
        # roteia para o método paginado padrão.
        return self.extrair_recurso(
            recurso=nomeRecurso,
            endpoint=f"/{nomeRecurso}",
            chave_dados=nomeRecurso
        )