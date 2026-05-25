import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import List, Dict, Any
from supabase import create_client, Client
from dateutil.parser import isoparse

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
        
        self.sessaoHttp = self.criarSessaoResiliente()
        self.carregarCredenciaisDoBanco()
        self.validarTokenAntesExecucao()

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

    def carregarCredenciaisDoBanco(self) -> None:
        respostaAuth = self.clienteSupabase.schema("silver").table("api_auth").select("*").eq("id", 1).execute()
        if respostaAuth.data:
            self.tokenAcesso = respostaAuth.data[0].get("access_token", "")
            self.tokenRenovacao = respostaAuth.data[0].get("refresh_token", "")
            self.headers["Authorization"] = f"Bearer {self.tokenAcesso}"
        else:
            self.logger.warning("Tabela api_auth vazia ou registro id=1 não encontrado.")
            self.tokenAcesso = ""
            self.tokenRenovacao = ""

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

    def validarTokenAntesExecucao(self) -> None:
        auth_data = self.clienteSupabase.schema("silver").table("api_auth").select("updated_at, expires_in").eq("id", 1).execute()
        if not auth_data.data:
            return

        updated_at = auth_data.data[0].get("updated_at")
        expires_in = auth_data.data[0].get("expires_in", 3600)

        if not updated_at:
            return

        dataAtualizacao = isoparse(updated_at)
        dataExpiracao = dataAtualizacao + timedelta(seconds=expires_in)

        if datetime.now(timezone.utc) >= dataExpiracao - timedelta(minutes=5):
            self.logger.info("Token próximo da expiração. Renovando preventivamente...")
            self.renovarTokenAcesso()

    def renovarTokenAcesso(self) -> None:
        self.logger.info("Iniciando processo de renovação/resgate de tokens da RD Station...")
        
        auth_data = self.clienteSupabase.schema("silver").table("api_auth").select("*").eq("id", 1).execute()
        token_salvo = auth_data.data[0].get("refresh_token", "").strip() if auth_data.data else self.tokenRenovacao.strip()
        
        if not token_salvo:
            raise Exception("Nenhum token disponível no banco para renovação.")
        
        payload_refresh = {
            "client_id": self.identificadorCliente,
            "client_secret": self.segredoCliente,
            "refresh_token": token_salvo,
            "grant_type": "refresh_token"
        }
        
        resposta = self.sessaoHttp.post(self.urlAutenticacao, json=payload_refresh, timeout=30)
        
        if resposta.status_code in [400, 401]:
            self.logger.warning("Token atual falhou (inválido ou expirado). Tentando resgate automático assumindo que você colou um Authorization Code novo no banco...")
            payload_code = {
                "client_id": self.identificadorCliente,
                "client_secret": self.segredoCliente,
                "code": token_salvo,
                "grant_type": "authorization_code"
            }
            resposta_resgate = self.sessaoHttp.post(self.urlAutenticacao, json=payload_code, timeout=30)
            
            if resposta_resgate.status_code == 200:
                self.logger.info("RESGATE BEM-SUCEDIDO! O Authorization Code foi convertido em novos tokens permanentes.")
                resposta = resposta_resgate
            else:
                raise Exception(f"FALHA IRRECUPERÁVEL. O valor no Supabase não é um Refresh Token válido nem um Authorization Code novo. Gere um novo Code na RD Station e cole no Supabase (coluna refresh_token). Detalhes: {resposta_resgate.text}")

        elif resposta.status_code != 200:
            raise Exception(f"Falha desconhecida ao renovar token: {resposta.status_code} - {resposta.text}")
        
        dados = resposta.json()
        novo_access = dados.get("access_token")
        novo_refresh = dados.get("refresh_token", token_salvo)
        expires_in = dados.get("expires_in", 3600)
        
        if not novo_access:
            raise Exception("A API da RD respondeu com sucesso, mas não retornou o access_token.")
        
        update_payload = {
            "id": 1,
            "access_token": novo_access,
            "refresh_token": novo_refresh,
            "expires_in": expires_in,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        resultado = self.clienteSupabase.schema("silver").table("api_auth").upsert(update_payload).execute()
        
        if not resultado.data:
            raise Exception("Falha silenciosa do Supabase ao tentar salvar os novos tokens (upsert falhou).")
        
        self.tokenAcesso = novo_access
        self.tokenRenovacao = novo_refresh
        self.headers["Authorization"] = f"Bearer {novo_access}"
        self.logger.info("Tokens renovados e salvos no banco de dados com sucesso.")

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
        resposta = self.sessaoHttp.get(url, headers=self.headers, timeout=30)
        
        if resposta.status_code in [401, 403]:
            self.logger.warning(f"Token expirado ou inválido (401/403) durante extração. Forçando renovação...")
            self.renovarTokenAcesso()
            
            resposta = self.sessaoHttp.get(url, headers=self.headers, timeout=30)
            if resposta.status_code != 200:
                raise Exception(f"Requisição falhou irremediavelmente após renovação: {resposta.status_code} - {resposta.text}")
                
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
            if respostaHttp.status_code == 404:
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