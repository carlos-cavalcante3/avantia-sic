import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import set_key
from typing import List, Dict, Any
from supabase import create_client, Client

class Extract:
    """
    Extrai dados da API do RD Station CRM utilizando a base oficial /crm/v2.
    Implementa tratamento de rate limit, renovacao inteligente de tokens, 
    paginacao robusta exclusiva com page[size] e links.next, prevenindo loops 
    infinitos e armazenando o raw data em CSV na camada correta.
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.urlBase = "https://api.rd.services/crm/v2"
        self.urlAutenticacao = "https://api.rd.services/auth/token"
        
        self.identificadorCliente = os.environ.get("RD_CLIENT_ID")
        self.segredoCliente = os.environ.get("RD_CLIENT_SECRET")
        
        urlBanco = os.environ.get("SUPABASE_URL")
        chaveBanco = os.environ.get("SUPABASE_KEY")
        self.clienteSupabase = create_client(urlBanco, chaveBanco)
        
        respostaAuth = self.clienteSupabase.schema("silver").table("api_auth").select("*").eq("id", 1).execute()
        
        if respostaAuth.data:
            self.tokenAcesso = respostaAuth.data[0]["access_token"]
            self.tokenRenovacao = respostaAuth.data[0]["refresh_token"]
        else:
            raise Exception("Tabela api_auth vazia. Insira os tokens iniciais no Supabase.")
        
        self.sessaoHttp = self.criarSessaoResiliente()
        self.renovacaoRealizada = False

    def criarSessaoResiliente(self) -> requests.Session:
        sessaoResiliente = requests.Session()
        estrategiaTentativas = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adaptadorHttp = HTTPAdapter(max_retries=estrategiaTentativas)
        sessaoResiliente.mount("http://", adaptadorHttp)
        sessaoResiliente.mount("https://", adaptadorHttp)
        return sessaoResiliente

    def salvarArquivoRaw(self, dados: List[Dict[str, Any]], nomeRecurso: str) -> None:
        caminhoDiretorio = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
        os.makedirs(caminhoDiretorio, exist_ok=True)
        
        dataHoraFormatada = datetime.now().strftime("%d-%m-%Y_%H%M%S")
        caminhoArquivo = os.path.join(caminhoDiretorio, f"{nomeRecurso}_{dataHoraFormatada}.csv")
        
        try:
            tabelaDadosRaw = pd.DataFrame(dados)
            tabelaDadosRaw.to_csv(caminhoArquivo, index=False, encoding="utf-8-sig", lineterminator='\r\n')
            self.logger.info(f"Arquivo raw salvo com sucesso em {caminhoArquivo}")
        except Exception as erroBackup:
            self.logger.error(f"Falha ao gerar arquivo raw para {nomeRecurso}: {str(erroBackup)}")

    def renovarTokenAcesso(self) -> None:
        self.logger.info("Iniciando solicitacao de renovacao do token de acesso")
        
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
            self.tokenRenovacao = dadosResposta.get("refresh_token", self.tokenRenovacao)
            
            from datetime import datetime, timezone
            self.clienteSupabase.schema("silver").table("api_auth").update({
                "access_token": self.tokenAcesso,
                "refresh_token": self.tokenRenovacao,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }).eq("id", 1).execute()
            
            self.logger.info("Token de acesso renovado e salvo com sucesso no Supabase.")
            self.renovacaoRealizada = True
        elif respostaHttp.status_code in [400, 401]:
            mensagemErro = f"REFRESH_TOKEN INVALIDO ou expirado (Status {respostaHttp.status_code}). Gere um novo token manualmente."
            self.logger.critical(mensagemErro)
            raise Exception(mensagemErro)
        else:
            mensagemErro = f"Falha na API de autenticacao. Status: {respostaHttp.status_code} - {respostaHttp.text}"
            self.logger.error(mensagemErro)
            raise Exception(mensagemErro)

    def obterCabecalhos(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.tokenAcesso}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def corrigirUrlPaginacao(self, urlOriginal: str) -> str:
        if not urlOriginal:
            return ""
        urlCorrigida = urlOriginal.replace("https://api.rd.services/api/v2/", "https://api.rd.services/crm/v2/")
        urlCorrigida = urlCorrigida.replace("https://api.rd.services/v2/", "https://api.rd.services/crm/v2/")
        urlCorrigida = urlCorrigida.replace("http://api.rd.services/crm/v2/", "https://api.rd.services/crm/v2/")
        urlCorrigida = urlCorrigida.replace("http://api.rd.services/v2/", "https://api.rd.services/crm/v2/")
        return urlCorrigida

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

    def extrairRecurso(self, nomeRecurso: str) -> List[Dict[str, Any]]:
        self.logger.info(f"Iniciando extracao na base CRM V2 para o recurso: {nomeRecurso}")
        
        todosRegistros = []
        urlRequisicao = f"{self.urlBase}/{nomeRecurso}?page[size]=100"
        possuiMaisPaginas = True
        urlsVisitadas = set()
        paginaAtual = 1
        
        while possuiMaisPaginas and urlRequisicao:
            if urlRequisicao in urlsVisitadas:
                self.logger.warning(f"Loop infinito evitado. A URL ja foi processada anteriormente: {urlRequisicao}")
                break
                
            urlsVisitadas.add(urlRequisicao)
            self.logger.info(f"Requisitando {urlRequisicao}")
            
            respostaHttp = self.sessaoHttp.get(urlRequisicao, headers=self.obterCabecalhos())
            
            if respostaHttp.status_code == 401:
                if not self.renovacaoRealizada:
                    self.logger.warning("Access Token expirado (401) detectado. Acionando mecanismo de renovacao.")
                    self.renovarTokenAcesso()
                    urlsVisitadas.remove(urlRequisicao)
                    continue
                else:
                    self.logger.error("Renovacao ja foi realizada mas o acesso continua negado (401). Interrompendo para evitar loop.")
                    break
                    
            if respostaHttp.status_code == 429:
                tempoEspera = int(respostaHttp.headers.get("Retry-After", 10))
                self.logger.warning(f"Rate limit atingido (429). Congelando processo por {tempoEspera} segundos.")
                urlsVisitadas.remove(urlRequisicao)
                time.sleep(tempoEspera)
                continue
                
            if respostaHttp.status_code in [403, 404]:
                self.logger.warning(f"Endpoint rejeitou acesso ({respostaHttp.status_code}) para a rota: {urlRequisicao}")
                break
                
            respostaHttp.raise_for_status()
            self.renovacaoRealizada = False 
            
            cargaDados = respostaHttp.json()
            registrosExtraidos = self.extrairDadosJson(cargaDados, nomeRecurso)
            
            if not registrosExtraidos:
                self.logger.info("Nenhum dado retornado nesta pagina. Concluindo ciclo de paginacao.")
                possuiMaisPaginas = False
                break
                
            todosRegistros.extend(registrosExtraidos)
            self.logger.info(f"Integrados {len(registrosExtraidos)} registros a fila. Total parcial: {len(todosRegistros)}")
            
            urlProximaOriginal = self.extrairProximaUrl(cargaDados)
            urlProximaCorrigida = self.corrigirUrlPaginacao(urlProximaOriginal)
            
            if urlProximaCorrigida and urlProximaCorrigida != urlRequisicao:
                urlRequisicao = urlProximaCorrigida
                paginaAtual += 1
            else:
                indicadorMaisPaginas = cargaDados.get("has_more", False)
                if indicadorMaisPaginas:
                    paginaAtual += 1
                    urlRequisicao = f"{self.urlBase}/{nomeRecurso}?page={paginaAtual}"
                    self.logger.warning(f"Fallback manual ativado para prevenir perda de dados. Forcando avanco para pagina {paginaAtual}")
                else:
                    possuiMaisPaginas = False
            
            time.sleep(0.5)
            
        if todosRegistros:
            self.salvarArquivoRaw(todosRegistros, nomeRecurso)
            
        self.logger.info(f"Extracao consolidada para {nomeRecurso}. Total final: {len(todosRegistros)}")
        return todosRegistros