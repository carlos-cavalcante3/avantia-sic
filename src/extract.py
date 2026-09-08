import logging
import os
import time
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from supabase import Client, create_client
from urllib3.util.retry import Retry

load_dotenv()


class TokenError(Exception):
    """Erro irrecuperável na cadeia OAuth2; interrompe o ETL."""


class ExtractionError(Exception):
    """Erro recuperável durante a extração de um recurso."""


# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

RD_TOKEN_URL = "https://api.rd.services/oauth2/token"
RD_API_BASE_URL = "https://api.rd.services/crm/v2"

AUTH_SCHEMA = "silver"
AUTH_TABLE = "api_auth"
AUTH_ROW_ID = 1

HTTP_TIMEOUT_SECONDS = 30
HTTP_MAX_RETRIES = 3

MAX_TOKEN_RENEWAL_ATTEMPTS = 3
TOKEN_EXPIRY_BUFFER_SECONDS = 300

LOCK_WAIT_TIMEOUT_SECONDS = 60
LOCK_RETRY_INTERVAL_SECONDS = 5
LOCK_STALE_SECONDS = 120

RATE_LIMIT_BACKOFF_BASE_SECONDS = 30


class Extract:
    """
    Cliente de extração para RD Station CRM.

    Os tokens OAuth ficam em silver.api_auth no Supabase. A tabela é a fonte
    única de verdade para permitir execuções locais e GitHub Actions sem
    depender de estado em memória entre execuções.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or self._configurar_logger()

        self.client_id = self._obter_variavel_obrigatoria("RD_CLIENT_ID")
        self.client_secret = self._obter_variavel_obrigatoria("RD_CLIENT_SECRET")

        supabase_url = self._obter_variavel_obrigatoria("SUPABASE_URL")
        supabase_key = self._obter_variavel_obrigatoria("SUPABASE_KEY")

        self.supabase: Client = create_client(supabase_url, supabase_key)
        self.session = self._criar_sessao_http()

        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._expires_at: Optional[datetime] = None
        self._token_renewal_count = 0

        self.logger.info(
            "Extract inicializado. Tokens serão carregados na primeira requisição."
        )

    # -----------------------------------------------------------------------
    # Configuração e utilitários
    # -----------------------------------------------------------------------

    @staticmethod
    def _configurar_logger() -> logging.Logger:
        logger = logging.getLogger("extract")

        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
            )
            logger.addHandler(handler)

        logger.setLevel(logging.INFO)
        return logger

    @staticmethod
    def _obter_variavel_obrigatoria(nome: str) -> str:
        valor = os.getenv(nome)

        if not valor:
            raise TokenError(
                f"Variável de ambiente obrigatória ausente ou vazia: {nome}. "
                "Verifique o arquivo .env local ou os GitHub Secrets."
            )

        return valor

    @staticmethod
    def _criar_sessao_http() -> requests.Session:
        session = requests.Session()

        retry = Retry(
            total=HTTP_MAX_RETRIES,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    @staticmethod
    def _parse_timestamp(valor: Any) -> Optional[datetime]:
        if not valor:
            return None

        try:
            data = datetime.fromisoformat(str(valor).replace("Z", "+00:00"))

            if data.tzinfo is None:
                return data.replace(tzinfo=timezone.utc)

            return data.astimezone(timezone.utc)

        except (TypeError, ValueError):
            return None

    @staticmethod
    def _achatar_registro(d: dict, parent_key: str = '', sep: str = '_') -> dict:
        """
        Achata estruturas aninhadas para garantir que dados como `custom_fields`
        (que vêm como listas ou dicionários embutidos) não sejam descartados
        pela etapa de carga no Supabase.
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(Extract._achatar_registro(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Listas (ex: múltiplos custom fields) viram strings JSON seguras
                items.append((new_key, json.dumps(v, ensure_ascii=False)))
            else:
                items.append((new_key, v))
        return dict(items)

    # -----------------------------------------------------------------------
    # Supabase: tokens e lock distribuído
    # -----------------------------------------------------------------------

    def _carregar_tokens_do_supabase(self) -> None:
        try:
            resposta = (
                self.supabase.schema(AUTH_SCHEMA)
                .table(AUTH_TABLE)
                .select("access_token, refresh_token, expires_at")
                .eq("id", AUTH_ROW_ID)
                .single()
                .execute()
            )

            dados = resposta.data

            if not dados:
                raise TokenError(
                    f"Nenhuma linha encontrada em {AUTH_SCHEMA}.{AUTH_TABLE} "
                    f"para id={AUTH_ROW_ID}."
                )

            if not dados.get("access_token") or not dados.get("refresh_token"):
                raise TokenError(
                    "A tabela de autenticação não possui access_token e refresh_token "
                    "válidos. Execute novamente o fluxo OAuth inicial."
                )

            self._access_token = dados["access_token"]
            self._refresh_token = dados["refresh_token"]
            self._expires_at = self._parse_timestamp(dados.get("expires_at"))

            self.logger.info("Tokens carregados do Supabase com sucesso.")

        except TokenError:
            raise
        except Exception as erro:
            raise TokenError(f"Falha ao carregar tokens do Supabase: {erro}") from erro

    def _salvar_tokens_no_supabase(
        self,
        access_token: str,
        refresh_token: str,
        expires_in: Optional[int],
    ) -> None:
        try:
            expires_at = None

            if expires_in:
                expires_at = (
                    datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))
                ).isoformat()

            payload = {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "expires_at": expires_at,
                "renovando": False,
                "renovando_desde": None,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

            resposta = (
                self.supabase.schema(AUTH_SCHEMA)
                .table(AUTH_TABLE)
                .update(payload)
                .eq("id", AUTH_ROW_ID)
                .execute()
            )

            if resposta.data is not None and len(resposta.data) == 0:
                raise TokenError(
                    f"Nenhuma linha foi atualizada em {AUTH_SCHEMA}.{AUTH_TABLE}."
                )

            self.logger.info(
                "Novo par de tokens salvo no Supabase com sucesso. "
                f"Expiração registrada: {expires_at or 'não informada'}."
            )

        except TokenError:
            raise
        except Exception as erro:
            raise TokenError(
                "O RD Station renovou o token, mas o novo par não pôde ser salvo "
                f"no Supabase. Reautorização manual pode ser necessária. Erro: {erro}"
            ) from erro

    def _liberar_lock(self) -> None:
        try:
            (
                self.supabase.schema(AUTH_SCHEMA)
                .table(AUTH_TABLE)
                .update(
                    {
                        "renovando": False,
                        "renovando_desde": None,
                    }
                )
                .eq("id", AUTH_ROW_ID)
                .execute()
            )

            self.logger.debug("Lock de renovação liberado.")

        except Exception as erro:
            self.logger.warning(f"Não foi possível liberar o lock de renovação: {erro}")

    def _liberar_lock_se_travado(self) -> None:
        try:
            resposta = (
                self.supabase.schema(AUTH_SCHEMA)
                .table(AUTH_TABLE)
                .select("renovando, renovando_desde")
                .eq("id", AUTH_ROW_ID)
                .single()
                .execute()
            )

            dados = resposta.data

            if not dados or not dados.get("renovando"):
                return

            inicio_lock = self._parse_timestamp(dados.get("renovando_desde"))

            if not inicio_lock:
                self.logger.warning(
                    "Lock ativo sem renovando_desde. Liberando por segurança."
                )
                self._liberar_lock()
                return

            idade = (datetime.now(timezone.utc) - inicio_lock).total_seconds()

            if idade > LOCK_STALE_SECONDS:
                self.logger.warning(
                    f"Lock travado detectado ({idade:.0f}s). Liberando lock."
                )
                self._liberar_lock()

        except Exception as erro:
            self.logger.warning(f"Falha ao verificar lock travado: {erro}")

    def _adquirir_lock(self) -> bool:
        self._liberar_lock_se_travado()

        limite = time.time() + LOCK_WAIT_TIMEOUT_SECONDS

        while time.time() < limite:
            try:
                agora = datetime.now(timezone.utc).isoformat()

                resposta = (
                    self.supabase.schema(AUTH_SCHEMA)
                    .table(AUTH_TABLE)
                    .update(
                        {
                            "renovando": True,
                            "renovando_desde": agora,
                        }
                    )
                    .eq("id", AUTH_ROW_ID)
                    .eq("renovando", False)
                    .execute()
                )

                if resposta.data:
                    self.logger.debug("Lock de renovação adquirido.")
                    return True

            except Exception as erro:
                self.logger.warning(f"Erro ao adquirir lock de renovação: {erro}")

            time.sleep(LOCK_RETRY_INTERVAL_SECONDS)
            self._liberar_lock_se_travado()

        self.logger.warning(
            "Tempo limite ao aguardar lock de renovação. "
            "Nenhuma renovação será feita sem lock."
        )
        return False

    # -----------------------------------------------------------------------
    # OAuth2
    # -----------------------------------------------------------------------

    def _token_precisa_renovar(self) -> bool:
        if not self._expires_at:
            return False

        margem = timedelta(seconds=TOKEN_EXPIRY_BUFFER_SECONDS)

        return datetime.now(timezone.utc) >= self._expires_at - margem

    def _garantir_token_valido(self) -> None:
        if not self._access_token:
            self._carregar_tokens_do_supabase()

        if self._token_precisa_renovar():
            self.logger.info(
                "Access token expirado ou próximo da expiração. "
                "Iniciando renovação preventiva."
            )
            self._renovar_token()

    def _renovar_token(self) -> None:
        if self._token_renewal_count >= MAX_TOKEN_RENEWAL_ATTEMPTS:
            raise TokenError(
                "Limite de renovações de token atingido nesta execução. "
                "Execução interrompida para evitar loop OAuth."
            )

        self.logger.warning(
            "Iniciando renovação de token "
            f"({self._token_renewal_count + 1}/{MAX_TOKEN_RENEWAL_ATTEMPTS})."
        )

        access_token_anterior = self._access_token
        lock_adquirido = self._adquirir_lock()

        if not lock_adquirido:
            self._carregar_tokens_do_supabase()

            if (
                self._access_token != access_token_anterior
                or not self._token_precisa_renovar()
            ):
                self.logger.info(
                    "Tokens foram atualizados por outra execução enquanto o lock "
                    "era aguardado. Usando o estado atual do Supabase."
                )
                return

            raise TokenError(
                "Não foi possível adquirir o lock de renovação. A execução foi "
                "interrompida para evitar uso concorrente do refresh_token."
            )

        try:
            self._carregar_tokens_do_supabase()

            if (
                access_token_anterior
                and self._access_token != access_token_anterior
                and not self._token_precisa_renovar()
            ):
                self.logger.info(
                    "Outra instância já renovou os tokens. "
                    "Nenhuma renovação adicional será realizada."
                )
                return

            if not self._token_precisa_renovar() and access_token_anterior:
                self.logger.info(
                    "Token atual do banco ainda está válido. Renovação dispensada."
                )
                return

            payload = {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
            }

            resposta = self.session.post(
                RD_TOKEN_URL,
                data=payload,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                },
                timeout=HTTP_TIMEOUT_SECONDS,
            )

            if resposta.status_code == 401:
                raise TokenError(
                    "REFRESH_TOKEN INVÁLIDO (401). Gere um novo code no fluxo OAuth, "
                    "troque-o por access_token e refresh_token e atualize ambos em "
                    "silver.api_auth."
                )

            if not resposta.ok:
                raise TokenError(
                    "Falha ao renovar token no RD Station. "
                    f"Status={resposta.status_code}; resposta={resposta.text[:500]}"
                )

            dados = resposta.json()

            novo_access_token = dados.get("access_token")
            novo_refresh_token = dados.get("refresh_token") or self._refresh_token
            expires_in = dados.get("expires_in")

            if not novo_access_token or not novo_refresh_token:
                raise TokenError(
                    "A resposta OAuth não contém os tokens esperados. "
                    f"Campos retornados: {list(dados.keys())}"
                )

            self._salvar_tokens_no_supabase(
                access_token=novo_access_token,
                refresh_token=novo_refresh_token,
                expires_in=expires_in,
            )

            self._access_token = novo_access_token
            self._refresh_token = novo_refresh_token
            self._expires_at = (
                datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))
                if expires_in
                else None
            )

            self._token_renewal_count += 1
            self.logger.info("Token RD Station renovado com sucesso.")

        except requests.RequestException as erro:
            raise TokenError(f"Erro de rede durante renovação OAuth: {erro}") from erro

        finally:
            self._liberar_lock()

    # -----------------------------------------------------------------------
    # Requisições HTTP
    # -----------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        self._garantir_token_valido()

        return {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _requisicao_blindada(
        self,
        url: str,
        params: Optional[dict] = None,
        tentativa: int = 0,
        max_tentativas: int = 3,
    ) -> dict:
        try:
            resposta = self.session.get(
                url,
                headers=self._headers(),
                params=params,
                timeout=HTTP_TIMEOUT_SECONDS,
            )
        except requests.RequestException as erro:
            raise ExtractionError(f"Erro de rede ao consultar {url}: {erro}") from erro

        if resposta.status_code == 401:
            if tentativa >= max_tentativas:
                raise TokenError(
                    f"401 persistente após {tentativa} tentativa(s) de renovação."
                )

            self.logger.warning(f"401 em {url}. Renovando token reativamente.")
            self._renovar_token()

            return self._requisicao_blindada(
                url=url,
                params=params,
                tentativa=tentativa + 1,
                max_tentativas=max_tentativas,
            )

        if resposta.status_code == 429:
            if tentativa >= max_tentativas:
                raise ExtractionError(
                    f"Rate limit persistente em {url} após {tentativa} tentativa(s)."
                )

            espera = RATE_LIMIT_BACKOFF_BASE_SECONDS * (2**tentativa)

            self.logger.warning(f"Rate limit em {url}. Nova tentativa em {espera}s.")

            time.sleep(espera)

            return self._requisicao_blindada(
                url=url,
                params=params,
                tentativa=tentativa + 1,
                max_tentativas=max_tentativas,
            )

        if resposta.status_code in (403, 404):
            self.logger.warning(
                f"Endpoint indisponível ou sem acesso ({resposta.status_code}): {url}"
            )
            return {"__abort": True}

        if not resposta.ok:
            raise ExtractionError(
                f"Erro HTTP {resposta.status_code} em {url}: {resposta.text[:500]}"
            )

        try:
            return resposta.json()
        except ValueError as erro:
            raise ExtractionError(
                f"Resposta inválida de {url}: {resposta.text[:300]}"
            ) from erro

    # -----------------------------------------------------------------------
    # Paginação
    # -----------------------------------------------------------------------

    @staticmethod
    def extrairProximaUrl(carga_dados: dict) -> str:
        if not isinstance(carga_dados, dict):
            return ""

        links = carga_dados.get("links", {})

        if not isinstance(links, dict):
            return ""

        proxima = links.get("next", "")

        if isinstance(proxima, dict):
            return proxima.get("href", "")

        return proxima if isinstance(proxima, str) else ""

    @staticmethod
    def corrigirUrlPaginacao(url_original: str) -> str:
        if not url_original:
            return ""

        url = url_original.replace("http://", "https://")
        url = url.replace("/api/v2/", "/crm/v2/")
        url = url.replace("/crm/crm/", "/crm/")

        if "api.rd.services" in url and "/crm/v2/" not in url:
            partes = url.split("api.rd.services", maxsplit=1)
            url = f"{RD_API_BASE_URL}{partes[-1]}"

        return url

    def extrair_recurso(
        self,
        recurso: str,
        endpoint: str,
        chave_dados: str,
        params_extras: Optional[dict] = None,
    ) -> list:
        filtros = ""

        if params_extras:
            filtros = "?" + "&".join(
                f"{chave}={valor}" for chave, valor in params_extras.items()
            )

        separador = "&" if filtros else "?"
        url_requisicao = (
            f"{RD_API_BASE_URL}{endpoint}{filtros}{separador}page[number]=1"
        )

        pagina_atual = 1
        dados_globais = []
        urls_visitadas = set()

        self.logger.info(
            f"[{recurso}] Iniciando extração | filtros={params_extras or {}}"
        )

        while url_requisicao:
            if url_requisicao in urls_visitadas:
                self.logger.warning(
                    f"[{recurso}] Loop de paginação evitado: {url_requisicao}"
                )
                break

            urls_visitadas.add(url_requisicao)

            self.logger.info(
                f"[{recurso}] Buscando página {pagina_atual}: {url_requisicao}"
            )

            carga_dados = self._requisicao_blindada(url_requisicao)

            if carga_dados.get("__abort"):
                break

            registros = []

            if isinstance(carga_dados, list):
                registros = carga_dados
            elif isinstance(carga_dados, dict):
                registros = (
                    carga_dados.get(chave_dados)
                    or carga_dados.get("data")
                    or carga_dados.get("items")
                    or []
                )

            if not registros:
                self.logger.info(
                    f"[{recurso}] Página {pagina_atual} sem registros adicionais."
                )
                break

            # -----------------------------------------------------------------
            # NOVO: Aqui os dados (incluindo os custom_fields) são achatados
            # Isso garante que no retorno do ETL, as listas aninhadas virem strings
            # e os sub-dicionários virem chaves independentes
            # -----------------------------------------------------------------
            registros_tratados = [self._achatar_registro(r) for r in registros]
            dados_globais.extend(registros_tratados)

            proxima_url = self.corrigirUrlPaginacao(self.extrairProximaUrl(carga_dados))

            if proxima_url:
                url_requisicao = proxima_url
                pagina_atual += 1
                time.sleep(0.5)
                continue

            if isinstance(carga_dados, dict) and carga_dados.get("has_more"):
                pagina_atual += 1
                url_requisicao = (
                    f"{RD_API_BASE_URL}{endpoint}{filtros}"
                    f"{separador}page[number]={pagina_atual}"
                )
                time.sleep(0.5)
                continue

            break

        self.logger.info(
            f"[{recurso}] Extração concluída: {len(dados_globais)} registro(s)."
        )

        return dados_globais

    # -----------------------------------------------------------------------
    # Deals
    # -----------------------------------------------------------------------

    def extrair_deals(self) -> list:
        """Extrai deals ganhos, perdidos e em aberto, removendo duplicados."""
        self.logger.info("[deals] Iniciando varreduras de ganhos, perdidos e abertos.")

        deals_por_id = {}

        varreduras = [
            ("ganhos", {"win": "true"}),
            ("perdidos", {"win": "false"}),
            ("abertos", None),
        ]

        for nome_varredura, filtros in varreduras:
            try:
                registros = self.extrair_recurso(
                    recurso=f"deals/{nome_varredura}",
                    endpoint="/deals",
                    chave_dados="deals",
                    params_extras=filtros,
                )

                novos = 0

                for deal in registros:
                    deal_id = deal.get("id")

                    if deal_id and deal_id not in deals_por_id:
                        deals_por_id[deal_id] = deal
                        novos += 1

                self.logger.info(
                    f"[deals/{nome_varredura}] "
                    f"{len(registros)} retornados; {novos} novo(s)."
                )

            except ExtractionError as erro:
                self.logger.error(
                    f"[deals/{nome_varredura}] Falha na varredura: {erro}"
                )

        resultado = list(deals_por_id.values())

        self.logger.info(
            f"[deals] Extração concluída: {len(resultado)} deal(s) único(s)."
        )

        return resultado

    # -----------------------------------------------------------------------
    # Compatibilidade com pipeline legado
    # -----------------------------------------------------------------------

    def extrairRecurso(self, nomeRecurso: str) -> list:
        """Mantém compatibilidade com pipeline.py que usa camelCase."""
        if nomeRecurso == "deals":
            return self.extrair_deals()

        return self.extrair_recurso(
            recurso=nomeRecurso,
            endpoint=f"/{nomeRecurso}",
            chave_dados=nomeRecurso,
        )
