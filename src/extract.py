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
        
        self.headers: Dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": ""
        }
        
        respostaAuth = self.clienteSupabase.schema("silver").table("api_auth").select("*").eq("id", 1).execute()
        if respostaAuth.data:
            self.tokenAcesso = respostaAuth.data[0]["access_token"]
            self.tokenRenovacao = respostaAuth.data[0]["refresh_token"]
            self.headers["Authorization"] = f"Bearer {self.tokenAcesso}"
        else:
            raise Exception("Tabela api_auth vazia.")
            
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
        except Exception as erroBackup:
            self.logger.error(f"Falha ao gerar arquivo raw: {str(erroBackup)}")

    def renovarTokenAcesso(self) -> None:
        """
        Renova as credenciais OAuth2 na API da RD Station utilizando o token de renovacao mais recente
        e atualiza o armazenamento no Supabase e os cabecalhos em memoria.
        """
        self.logger.info("Iniciando renovação do Access Token via Refresh Token...")
        
        auth_data = self.clienteSupabase.schema("silver").table("api_auth").select("refresh_token").eq("id", 1).execute()
        if not auth_data.data:
            raise Exception("Nenhum registro de autenticação encontrado na tabela silver.api_auth (id=1).")
            
        refresh_token_atual = auth_data.data[0]["refresh_token"]
        
        payload = {
            "client_id": self.identificadorCliente,
            "client_secret": self.segredoCliente,
            "refresh_token": refresh_token_atual.strip(),
            "grant_type": "refresh_token"
        }
        
        headers_auth = {"Content-Type": "application/json"}
        
        resposta = requests.post(self.urlAutenticacao, json=payload, headers=headers_auth)
        
        if resposta.status_code == 200:
            novos_tokens = resposta.json()
            novo_access = novos_tokens["access_token"]
            novo_refresh = novos_tokens["refresh_token"]
            
            self.tokenAcesso = novo_access
            self.tokenRenovacao = novo_refresh
            self.headers["Authorization"] = f"Bearer {novo_access}"
            
            update_payload = {
                "access_token": novo_access,
                "refresh_token": novo_refresh,
                "updated_at": "now()" 
            }
            
            self.clienteSupabase.schema("silver").table("api_auth").update(update_payload).eq("id", 1).execute()
            self.logger.info("Token renovado e atualizado no Supabase com sucesso!")
        else:
            erro_msg = f"Falha crítica ao renovar token na RD Station: {resposta.status_code} - {resposta.text}"
            self.logger.error(erro_msg)
            raise Exception(erro_msg)

    def obterCabecalhos(self) -> Dict[str, str]:
        return self.headers

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

    def requisicaoBlindada(self, url: str) -> requests.Response:
        """
        Executa a chamada HTTP com tratamento de expiracao de credenciais.
        Caso retorne status 401, aciona a renovacao e reprocessa a chamada atual com os novos cabecalhos.
        """
        resposta = requests.get(url, headers=self.headers)
        
        if resposta.status_code == 401:
            self.logger.warning(f"Token expirado (401) ao tentar acessar {url}. Tentando renovar...")
            self.renovarTokenAcesso()
            
            resposta = requests.get(url, headers=self.headers)
            
            if resposta.status_code != 200:
                raise Exception(f"Requisição falhou mesmo após renovação do token: {resposta.status_code} - {resposta.text}")
                
        elif resposta.status_code != 200:
            raise Exception(f"Erro na requisição da API RD: {resposta.status_code} - {resposta.text}")
            
        return resposta

    def extrairStages(self) -> List[Dict[str, Any]]:
        todosRegistros = []
        urlPipelines = f"{self.urlBase}/pipelines"
        respostaPipelines = self.requisicaoBlindada(urlPipelines)
        if respostaPipelines.status_code == 200:
            pipelines = respostaPipelines.json().get('data', [])
            for pipeline in pipelines:
                pipeline_id = pipeline.get('id')
                stage_ids = pipeline.get('stage_ids', [])
                for index, stage_id in enumerate(stage_ids):
                    urlStage = f"{self.urlBase}/pipelines/{pipeline_id}/stages/{stage_id}"
                    respStage = self.requisicaoBlindada(urlStage)
                    if respStage.status_code == 200:
                        stage_data = respStage.json().get('data', {})
                        stage_data['pipeline_id'] = pipeline_id
                        stage_data['order'] = index + 1
                        todosRegistros.append(stage_data)
                    time.sleep(0.2)
        if todosRegistros:
            self.salvarArquivoRaw(todosRegistros, "stages")
        return todosRegistros

    def extrairRecurso(self, nomeRecurso: str) -> List[Dict[str, Any]]:
        if nomeRecurso == "stages":
            return self.extrairStages()
        todosRegistros = []
        urlRequisicao = f"{self.urlBase}/{nomeRecurso}?page[size]=100"
        possuiMaisPaginas = True
        urlsVisitadas = set()
        paginaAtual = 1
        while possuiMaisPaginas and urlRequisicao:
            if urlRequisicao in urlsVisitadas:
                break
            urlsVisitadas.add(urlRequisicao)
            respostaHttp = self.requisicaoBlindada(urlRequisicao)
            if respostaHttp.status_code in [401, 403, 404]:
                break
            respostaHttp.raise_for_status()
            cargaDados = respostaHttp.json()
            registrosExtraidos = self.extrairDadosJson(cargaDados, nomeRecurso)
            if not registrosExtraidos:
                possuiMaisPaginas = False
                break
            todosRegistros.extend(registrosExtraidos)
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
                else:
                    possuiMaisPaginas = False
            time.sleep(0.5)
        if todosRegistros:
            self.salvarArquivoRaw(todosRegistros, nomeRecurso)
        return todosRegistros