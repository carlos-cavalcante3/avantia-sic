import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import List, Dict, Any
from supabase import create_client, Client

class Extract:
    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.urlBase = "https://api.rd.services/crm/v2"
        self.urlAutenticacao = "https://api.rd.services/auth/token"
        self.identificadorCliente = os.environ.get("RD_CLIENT_ID")
        self.segredoCliente = os.environ.get("RD_CLIENT_SECRET")
        urlBanco = os.environ.get("SUPABASE_URL")
        chaveBanco = os.environ.get("SUPABASE_KEY")
        
        self.clienteSupabase: Client = create_client(str(urlBanco), str(chaveBanco))
        self.sessaoHttp = self.criarSessaoResiliente()
        self.renovacaoRealizada = False
        
        self.carregarCredenciaisDoBanco()
        
        self.renovarTokenAcesso()

    def criarSessaoResiliente(self) -> requests.Session:
        sessaoResiliente = requests.Session()
        estrategiaTentativas = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504], 
            allowed_methods=["GET", "POST"]
        )
        adaptadorHttp = HTTPAdapter(max_retries=estrategiaTentativas)
        sessaoResiliente.mount("http://", adaptadorHttp)
        sessaoResiliente.mount("https://", adaptadorHttp)
        return sessaoResiliente

    def carregarCredenciaisDoBanco(self) -> None:
        respostaAuth = self.clienteSupabase.schema("silver").table("api_auth").select("*").eq("id", 1).execute()
        if respostaAuth.data:
            self.tokenAcesso = respostaAuth.data[0]["access_token"]
            self.tokenRenovacao = respostaAuth.data[0]["refresh_token"]
        else:
            raise Exception("Tabela api_auth vazia.")

    def obterCabecalhos(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.tokenAcesso}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def salvarArquivoRaw(self, dados: List[Dict[str, Any]], nomeRecurso: str) -> None:
        caminhoDiretorio = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
        os.makedirs(caminhoDiretorio, exist_ok=True)
        dataHoraFormatada = datetime.now().strftime("%d-%m-%Y_%H%M%S")
        caminhoArquivo = os.path.join(caminhoDiretorio, f"{nomeRecurso}_{dataHoraFormatada}.csv")
        try:
            tabelaDadosRaw = pd.DataFrame(dados)
            tabelaDadosRaw.to_csv(caminhoArquivo, index=False, encoding="utf-8-sig", lineterminator='\r\n')
        except Exception as erroBackup:
            self.logger.error(f"Falha ao gerar arquivo raw: {str(erroBackup)}")

    def renovarTokenAcesso(self) -> None:
        self.logger.info("Executando geracao proativa de novo token OAuth2...")
        cargaDadosAutenticacao = {
            "client_id": self.identificadorCliente,
            "client_secret": self.segredoCliente,
            "refresh_token": self.tokenRenovacao,
            "grant_type": "refresh_token"
        }
        
        respostaHttp = self.sessaoHttp.post(self.urlAutenticacao, json=cargaDadosAutenticacao)
        
        if respostaHttp.status_code == 200:
            dadosResposta = respostaHttp.json()
            self.tokenAcesso = dadosResposta.get("access_token")
            # É crucial salvar o novo refresh_token da cadeia de rotação
            self.tokenRenovacao = dadosResposta.get("refresh_token", self.tokenRenovacao)
            
            # Grava na camada Silver
            resposta_banco = self.clienteSupabase.schema("silver").table("api_auth").update({
                "access_token": self.tokenAcesso,
                "refresh_token": self.tokenRenovacao,
                "expires_in": dadosResposta.get("expires_in", 3600),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }).eq("id", 1).execute()
            
            # VALIDAÇÃO DE SEGURANÇA: Evita que o script continue caso a gravação falhe
            if not resposta_banco.data:
                raise Exception("CRITICO: O token foi renovado junto a API, mas o Supabase rejeitou o UPDATE na tabela api_auth (possivel falha de permissao ou service_role ausente). O processo foi abortado para proteger a integridade do token atual.")
            
            self.renovacaoRealizada = True
            self.logger.info("Tokens renovados e persistidos no banco de dados com segurança total.")
            
        elif respostaHttp.status_code in [400, 401]:
            raise Exception(f"REFRESH_TOKEN INVALIDO ({respostaHttp.status_code}). Gere um novo Code via URL do RD Station, solicite via Postman e insira na tabela manualmente uma ultima vez.")
        else:
            raise Exception(f"Falha na API de autenticacao: Status {respostaHttp.status_code}")

    def renovarTokenPosCarga(self) -> None:
        self.logger.info("Carga concluída. Preparando tokens fresquinhos para a execução de amanhã...")
        self.renovarTokenAcesso()

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

    def extrairProximaUrl(self, cargaDados: Any) -> str:
        if isinstance(cargaDados, dict):
            if "links" in cargaDados and isinstance(cargaDados["links"], dict):
                linkProximo = cargaDados["links"].get("next", "")
                if isinstance(linkProximo, dict):
                    return linkProximo.get("href", "")
                if isinstance(linkProximo, str):
                    return linkProximo
        return ""

    def extrairDadosJson(self, cargaDados: Any, nomeRecurso: str) -> List[Dict[str, Any]]:
        if isinstance(cargaDados, list):
            return cargaDados
        if isinstance(cargaDados, dict):
            if nomeRecurso in cargaDados:
                return cargaDados[nomeRecurso]
            if "data" in cargaDados:
                return cargaDados["data"]
            if "items" in cargaDados:
                return cargaDados["items"]
        return []

    def _requisicaoBlindada(self, url: str) -> requests.Response:
        while True:
            respostaHttp = self.sessaoHttp.get(url, headers=self.obterCabecalhos())
            
            if respostaHttp.status_code == 401:
                if not self.renovacaoRealizada:
                    self.logger.warning("Token expirou no meio da carga! Renovando emergencialmente...")
                    self.renovarTokenAcesso()
                    continue
                break
                
            if respostaHttp.status_code == 429:
                tempoEspera = int(respostaHttp.headers.get("Retry-After", 5))
                self.logger.warning(f"RD Station bloqueou por limite de chamadas (429). Aguardando {tempoEspera} segundos...")
                time.sleep(tempoEspera)
                continue
                
            self.renovacaoRealizada = False
            return respostaHttp

    def extrairRecurso(self, nomeRecurso: str) -> List[Dict[str, Any]]:
        self.logger.info(f"Iniciando extração do recurso: {nomeRecurso}")

        filtros_api = [""]

        if nomeRecurso == "deals":
            filtros_api = ["?win=true", "?win=false", ""]

        dadosGlobais = []
        urlsVisitadas = set()

        for filtro in filtros_api:

            paginaAtual = 1
            baseEndpoint = f"{nomeRecurso}{filtro}"
            separador = "&" if "?" in baseEndpoint else "?"

            urlRequisicao = (
                f"{self.urlBase}/"
                f"{baseEndpoint}"
                f"{separador}"
                f"page[number]=1"
            )

            while urlRequisicao:

                if urlRequisicao in urlsVisitadas:
                    self.logger.warning(f"Loop evitado: {urlRequisicao}")
                    break

                urlsVisitadas.add(urlRequisicao)

                self.logger.info(f"Buscando (Pag {paginaAtual}): {urlRequisicao}")

                try:
                    respostaHttp = self._requisicaoBlindada(urlRequisicao)

                    if respostaHttp.status_code in [401, 403, 404]:
                        self.logger.warning(f"Extracao interrompida (Status {respostaHttp.status_code})")
                        break

                    respostaHttp.raise_for_status()

                    cargaDados = respostaHttp.json()

                    registrosExtraidos = self.extrairDadosJson(cargaDados, nomeRecurso)

                    if not registrosExtraidos:
                        self.logger.info("Sem registros encontrados.")
                        break

                    dadosGlobais.extend(registrosExtraidos)

                    # -----------------------------
                    # PAGINAÇÃO SEGURA (CORRIGIDA)
                    # -----------------------------
                    urlProxima = self.extrairProximaUrl(cargaDados)

                    if urlProxima:
                        self.logger.debug(f"Next original: {urlProxima}")

                    urlProxima = self.corrigirUrlPaginacao(urlProxima)

                    if urlProxima:
                        urlRequisicao = urlProxima
                        paginaAtual += 1
                    else:
                        indicadorMaisPaginas = cargaDados.get("has_more", False)

                        if indicadorMaisPaginas:
                            paginaAtual += 1
                            urlRequisicao = (
                                f"{self.urlBase}/"
                                f"{baseEndpoint}"
                                f"{separador}"
                                f"page[number]={paginaAtual}"
                            )
                        else:
                            break

                    time.sleep(0.5)

                except Exception as e:
                    try:
                        self.logger.error(f"""
ERRO REQUISIÇÃO

URL:
{urlRequisicao}

STATUS:
{respostaHttp.status_code}

BODY:
{respostaHttp.text}

ERRO:
{str(e)}
""")
                    except:
                        self.logger.error(str(e))
                    break

        if dadosGlobais:
            self.salvarArquivoRaw(dadosGlobais, nomeRecurso)

        self.logger.info(f"Total extraido para {nomeRecurso}: {len(dadosGlobais)} registros.")

        return dadosGlobais