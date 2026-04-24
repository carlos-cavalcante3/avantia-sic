import logging
import os
from supabase import create_client, Client
from dotenv import load_dotenv

class LoadSilver:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        load_dotenv(override=True)
        
        urlBanco = os.environ.get("SUPABASE_URL")
        chaveBanco = os.environ.get("SUPABASE_KEY")
        
        if not urlBanco or not chaveBanco:
            self.logger.critical("Credenciais do Supabase ausentes no arquivo .env")
            raise ValueError("Variaveis SUPABASE_URL e SUPABASE_KEY sao obrigatorias.")
            
        self.clienteSupabase: Client = create_client(urlBanco, chaveBanco)

    def obterMapeamentoEtapasAtuais(self):
        mapaEtapas = {}
        tamanhoLote = 1000
        indiceInicio = 0
        
        while True:
            indiceFim = indiceInicio + tamanhoLote - 1
            respostaApi = self.clienteSupabase.schema("silver").table("deals").select("id,stage_id").range(indiceInicio, indiceFim).execute()
            registrosPagina = respostaApi.data
            
            if not registrosPagina:
                break
                
            for registro in registrosPagina:
                mapaEtapas[registro['id']] = registro['stage_id']
                
            if len(registrosPagina) < tamanhoLote:
                break
                
            indiceInicio += tamanhoLote
            
        return mapaEtapas

    def carregarDados(self, dadosTransformados, nomeTabela):
        if not dadosTransformados:
            return

        totalRegistros = len(dadosTransformados)
        tamanhoLote = 500
        pacotesEnvio = [dadosTransformados[i:i + tamanhoLote] for i in range(0, totalRegistros, tamanhoLote)]
        
        try:
            for pacote in pacotesEnvio:
                self.clienteSupabase.schema("silver").table(nomeTabela).upsert(pacote).execute()
            self.logger.info(f"Transformação bronze layer -> silver layer: {totalRegistros} registros na tabela {nomeTabela}.")
        except Exception as erroCarga:
            self.logger.error(f"Falha de integridade ao aplicar upsert na tabela {nomeTabela}: {str(erroCarga)}")
            raise

    def carregarHistoricoDeals(self, dadosHistorico):
        if not dadosHistorico:
            return

        totalRegistros = len(dadosHistorico)
        tamanhoLote = 500
        pacotesEnvio = [dadosHistorico[i:i + tamanhoLote] for i in range(0, totalRegistros, tamanhoLote)]
        
        try:
            for pacote in pacotesEnvio:
                self.clienteSupabase.schema("silver").table("deals_historico").insert(pacote).execute()
            self.logger.info(f"Historico de alteracoes salvo: {totalRegistros} mudancas de etapa registradas.")
        except Exception as erroCarga:
            self.logger.error(f"Falha ao registrar historico na tabela deals_historico: {str(erroCarga)}")
            raise

    def gerarSnapshotDiario(self):
        try:
            self.clienteSupabase.rpc("gerar_snapshot_deals", {}).execute()
            self.logger.info("Snapshot diario gerado com sucesso na camada Gold.")
        except Exception as erroSnapshot:
            self.logger.error(f"Falha ao acionar a rotina de snapshot diario: {str(erroSnapshot)}")
            raise

    def atualizarCamadaGold(self):
        """
        Aciona a rotina interna do banco para atualizar todas as 
        Materialized Views da camada Gold.
        """
        try:
            self.clienteSupabase.rpc("atualizar_camada_gold", {}).execute()
            self.logger.info("Camada Gold (Materialized Views) atualizada com sucesso.")
        except Exception as erroGold:
            self.logger.error(f"Falha ao atualizar as visões da Gold: {str(erroGold)}")
            raise