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
        
        self.tokenAcesso = os.environ.get("RD_ACCESS_TOKEN")
        self.tokenRenovacao = os.environ.get("RD_REFRESH_TOKEN")
        self.identificadorCliente = os.environ.get("RD_CLIENT_ID")
        self.segredoCliente = os.environ.get("RD_CLIENT_SECRET")
        self.caminhoArquivoAmbiente = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
        
        self.sessaoHttp = self.criarSessaoResiliente()
        self.renovacaoRealizada = False

    def criarSessaoResiliente(self) -> requests.Session:
        sessaoResiliente = requests.Session()
        estrategiaTentativas = Retry(
            total=4,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adaptadorHttp = HTTPAdapter(max_retries=estrategiaTentativas)
        sessaoResiliente.mount("http://", adaptadorHttp)
        sessaoResiliente.mount("https://", adaptadorHttp)
        return sessaoResiliente

    def salvarArquivoRaw(self, dados: List[Dict[str, Any]], nomeRecurso: str) -> None:
        """
        Gera o arquivo CSV na camada data/raw/ com a nomenclatura exigida,
        salvaguardando a informacao bruta extraida da API.
        """
        caminhoDiretorio = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
        os.makedirs(caminhoDiretorio, exist_ok=True)
        
        dataHoraFormatada = datetime.now().strftime("%d-%m-%Y_%H%M%S")
        caminhoArquivo = os.path.join(caminhoDiretorio, f"{nomeRecurso}_{dataHoraFormatada}.csv")
        
        try:
            tabelaDadosRaw = pd.DataFrame(dados)
            tabelaDadosRaw.to_csv(caminhoArquivo, index=False, encoding="utf-8")
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
            
            set_key(self.caminhoArquivoAmbiente, "RD_ACCESS_TOKEN", self.tokenAcesso)
            set_key(self.caminhoArquivoAmbiente, "RD_REFRESH_TOKEN", self.tokenRenovacao)
            
            self.logger.info("Token de acesso renovado, arquivos atualizados e persistidos com sucesso.")
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
            
            if not urlProximaCorrigida:
                possuiMaisPaginas = False
            elif urlProximaCorrigida == urlRequisicao:
                self.logger.warning("A proxima URL e identica a atual. Encerrando paginacao para evitar loop infinito.")
                possuiMaisPaginas = False
            else:
                urlRequisicao = urlProximaCorrigida
                time.sleep(0.5)
                
        if todosRegistros:
            self.salvarArquivoRaw(todosRegistros, nomeRecurso)
            
        self.logger.info(f"Extracao consolidada para {nomeRecurso}. Total final: {len(todosRegistros)}")
        return todosRegistros