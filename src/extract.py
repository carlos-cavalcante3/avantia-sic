import os
import json
import time
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv, set_key

CAMINHO_ENV = ".env"

class Extract:
    """
    Classe responsável por extrair dados da API do RD Station CRM V2.
    Gerencia autenticação OAuth (com renovação automática no .env), 
    sessões persistentes, controle de taxa, correção de rotas e paginação.
    """
    def __init__(self):
        load_dotenv(CAMINHO_ENV, override=True)

        self.url_base = "https://api.rd.services/crm/v2"
        
        self.token_acesso = os.getenv("RD_ACCESS_TOKEN")
        self.token_atualizacao = os.getenv("RD_REFRESH_TOKEN")
        self.id_cliente = os.getenv("RD_CLIENT_ID")
        self.segredo_cliente = os.getenv("RD_CLIENT_SECRET")

        self.criacao_token = time.time()
        self.expiracao_token = 3600

        self.sessao = requests.Session()
        self._configurar_cabecalhos()

        self.intervalo_requisicao = 0.6  
        self.max_tentativas = 5

        self.diretorio_backup = "data/raw"
        os.makedirs(self.diretorio_backup, exist_ok=True)
        self.logger = logging.getLogger("Extract")

    def _configurar_cabecalhos(self):
        self.sessao.headers.update({
            "Authorization": f"Bearer {self.token_acesso}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        })

    def _token_expirado(self):
        return (time.time() - self.criacao_token) > (self.expiracao_token - 60)

    def _atualizar_env(self):
        set_key(CAMINHO_ENV, "RD_ACCESS_TOKEN", self.token_acesso)
        set_key(CAMINHO_ENV, "RD_REFRESH_TOKEN", self.token_atualizacao)

    def _renovar_token(self):
        self.logger.info("Token expirado (ou 401). Solicitando renovação automática...")
        url = "https://api.rd.services/auth/token"

        carga = {
            "grant_type": "refresh_token",
            "refresh_token": self.token_atualizacao,
            "client_id": self.id_cliente,
            "client_secret": self.segredo_cliente,
        }

        resposta = requests.post(url, data=carga)

        if resposta.status_code == 200:
            tokens = resposta.json()
            self.token_acesso = tokens["access_token"]
            self.token_atualizacao = tokens["refresh_token"]
            self.criacao_token = time.time()

            self._configurar_cabecalhos()
            self._atualizar_env()
            self.logger.info("Token de acesso atualizado")
        else:
            self.logger.critical(f"Falha irreversível ao renovar token: {resposta.text}")
            raise Exception(f"Erro ao renovar token: {resposta.text}")

    def testar_conexao(self):
        self.logger.info("Teste de autenticação com API ")
        try:
            if self._token_expirado():
                self._renovar_token()
                
            url = f"{self.url_base}/users"
            resposta = self.sessao.get(url, params={"page[size]": 1}, timeout=15)
            
            if resposta.status_code == 401:
                self._renovar_token()
                resposta = self.sessao.get(url, params={"page[size]": 1}, timeout=15)
                
            resposta.raise_for_status()
            self.logger.info("Conexão com RD Station V2 estabelecida")
            return True
            
        except Exception as erro:
            self.logger.error(f"Falha de conexão com RD Station: {erro}")
            return False

    def _corrigir_url(self, url):
        if "/api/v2/" in url:
            return url.replace("/api/v2/", "/crm/v2/")
        return url

    def _fazer_requisicao(self, url):
        for tentativa in range(self.max_tentativas):
            try:
                if self._token_expirado():
                    self._renovar_token()

                resposta = self.sessao.get(url)

                if resposta.status_code == 429:
                    espera = 2 ** tentativa
                    self.logger.warning(f"Rate Limit 429. Aguardando {espera}s...")
                    time.sleep(espera)
                    continue

                if resposta.status_code >= 500:
                    espera = 2 ** tentativa
                    self.logger.warning(f"Erro no Servidor {resposta.status_code}. Aguardando {espera}s")
                    time.sleep(espera)
                    continue

                if resposta.status_code == 401:
                    self._renovar_token()
                    continue

                resposta.raise_for_status()

                time.sleep(self.intervalo_requisicao)
                return resposta.json()

            except requests.exceptions.RequestException as e:
                espera = 2 ** tentativa
                self.logger.warning(f"Falha de rede: {e}. Aguardando {espera}s")
                time.sleep(espera)

        raise Exception(f"Falha na requisição para {url} após {self.max_tentativas} tentativas.")

    def extrair_endpoint(self, nome_endpoint):
        self.logger.info(f"[{nome_endpoint}] Iniciando extração")
        url = f"{self.url_base}/{nome_endpoint}?page[size]=100"
        
        dados_completos = []
        pagina = 1

        while url:
            dados_pagina = self._fazer_requisicao(url)
            elementos = dados_pagina.get("data", [])
            dados_completos.extend(elementos)

            self.logger.info(f"[{nome_endpoint}] Lote {pagina} concluído -> Total baixado: {len(dados_completos)} registros")

            proxima_url = dados_pagina.get("links", {}).get("next")
            
            if proxima_url:
                url = self._corrigir_url(proxima_url)
                pagina += 1
            else:
                url = None

        if dados_completos:
            self._salvar_backup(nome_endpoint, dados_completos)

        return dados_completos

    def _salvar_backup(self, nome_endpoint, dados):
        data_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_arquivo = f"{nome_endpoint}_{data_hora}.json"
        caminho_arquivo = os.path.join(self.diretorio_backup, nome_arquivo)

        with open(caminho_arquivo, "w", encoding="utf-8") as arquivo:
            json.dump(dados, arquivo, ensure_ascii=False, indent=2)
        
        self.logger.info(f"[{nome_endpoint}] Backup salvo em: {caminho_arquivo}")