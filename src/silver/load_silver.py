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

    # O "pulo do gato": Adicionamos o nomeSchema dinâmico, com "silver" como padrão
    def carregarDados(self, dadosTransformados, nomeTabela, nomeSchema="silver"):
        if not dadosTransformados:
            return

        totalRegistros = len(dadosTransformados)
        tamanhoLote = 500
        pacotesEnvio = [dadosTransformados[i:i + tamanhoLote] for i in range(0, totalRegistros, tamanhoLote)]

        try:
            for pacote in pacotesEnvio:
                self.clienteSupabase.schema(nomeSchema).table(nomeTabela).upsert(
                    pacote,
                    on_conflict="id"
                ).execute()
            self.logger.info(f"Carga no schema '{nomeSchema}' finalizada para a tabela '{nomeTabela}'.")
        except Exception as e:
            self.logger.error(f"Erro ao carregar dados na camada {nomeSchema} ({nomeTabela}): {str(e)}")
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
        try:
            self.clienteSupabase.rpc("atualizar_camada_gold", {}).execute()
            self.logger.info("Camada Gold (Materialized Views) atualizada com sucesso.")
        except Exception as erroGold:
            self.logger.error(f"Falha ao atualizar as visões da Gold: {str(erroGold)}")
            raise
