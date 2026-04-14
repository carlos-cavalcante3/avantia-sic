import logging
from src.extract import Extract
from src.transform import Transform
from src.load import Load

class Pipeline:
    """
    Classe que orquestra o fluxo ETL completo.
    Realiza pre-flight checks (testes de conexão) antes de iniciar as extrações.
    """
    def __init__(self, url_supabase, chave_supabase, esquemas_tabelas):
        self.extrator = Extract() 
        self.transformador = Transform(esquemas_tabelas)
        self.carregador = Load(url_supabase, chave_supabase)
        self.logger = logging.getLogger("Pipeline")
        self.endpoints = [
            "users", "teams", "organizations", "pipelines",
            "contacts", "deals", "tasks"
        ]

    def executar(self):
        self.logger.info("PIPELINE AVANTIA-SIG INICIADA")
        
        if not self.extrator.testar_conexao():
            self.logger.critical("Abortando pipeline devido a falha na origem (RD Station).")
            return
            
        if not self.carregador.testar_conexao():
            self.logger.critical("Abortando pipeline devido a falha no destino (Supabase).")
            return

        self.logger.info("Todos os sistemas operacionais. Iniciando fluxo de dados")

        for endpoint in self.endpoints:
            try:
                self.logger.info(f"--- Processando endpoint: {endpoint.upper()} ---")

                dados_brutos = self.extrator.extrair_endpoint(endpoint)
                
                if not dados_brutos:
                    self.logger.info(f"[{endpoint}] Nenhum dado localizado. Pulando para o próximo.")
                    continue

                dados_prontos = self.transformador.preparar_dados(endpoint, dados_brutos)

                self.carregador.carregar_dados(endpoint, dados_prontos)

            except Exception as erro:
                self.logger.error(f"Falha não tratada no fluxo de {endpoint}: {erro}", exc_info=True)
                self.logger.info("Continuando com o próximo endpoint...")

        self.logger.info("=== PIPELINE ETL FINALIZADA ===")