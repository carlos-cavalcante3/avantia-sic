import os
import math
import logging
import requests
from supabase import create_client, Client, ClientOptions
from typing import List, Dict, Any, Set

class Load:
    """
    Componente super resiliente responsavel pela interacao transacional com o Supabase.
    Implementa introspeccao dinâmica de schema, deduplicacao em memoria,
    normalizacao robusta de inteiros corrompidos pelo Pandas e insercao massiva em lotes 
    com controle estrito de idempotencia (upsert on_conflict).
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.urlSupabase = os.environ.get("SUPABASE_URL")
        self.chaveSupabase = os.environ.get("SUPABASE_KEY")
        
        if not self.urlSupabase or not self.chaveSupabase:
            raise ValueError("Credenciais do Supabase nao foram detectadas ou estao incorretas.")
        
        opcoesCliente = ClientOptions(schema="bronze")
        self.clienteSupabase: Client = create_client(self.urlSupabase, self.chaveSupabase, options=opcoesCliente)

    def descobrirColunasSchema(self, nomeTabela: str) -> List[str]:
        """
        Consome a definicao OpenAPI nativa do PostgREST no Supabase para descobrir 
        dinamicamente as colunas fisicas reais da tabela no schema alvo.
        """
        urlOpenApi = f"{self.urlSupabase}/rest/v1/"
        cabecalhos = {
            "apikey": self.chaveSupabase,
            "Authorization": f"Bearer {self.chaveSupabase}",
            "Accept-Profile": "bronze"
        }
        
        try:
            respostaHttp = requests.get(urlOpenApi, headers=cabecalhos, timeout=10)
            if respostaHttp.status_code == 200:
                especificacao = respostaHttp.json()
                
                definicoes = especificacao.get("definitions", {})
                if not definicoes:
                    definicoes = especificacao.get("components", {}).get("schemas", {})
                    
                tabelaAlvo = definicoes.get(nomeTabela)
                
                if tabelaAlvo and "properties" in tabelaAlvo:
                    colunasMapeadas = list(tabelaAlvo["properties"].keys())
                    self.logger.info(f"Schema descoberto para '{nomeTabela}': {len(colunasMapeadas)} colunas mapeadas fisicamente.")
                    return colunasMapeadas
        except Exception as erroDescoberta:
            self.logger.warning(f"Falha ao tentar introspectar schema da tabela {nomeTabela} via OpenAPI: {str(erroDescoberta)}")
            
        return []

    def alinharSchema(self, dados: List[Dict[str, Any]], nomeTabela: str) -> List[Dict[str, Any]]:
        """
        Filtra estritamente o payload removendo colunas dinamicas geradas no transform 
        que nao existem na tabela destino do PostgreSQL.
        """
        colunasPermitidas = self.descobrirColunasSchema(nomeTabela)
        
        if not colunasPermitidas:
            self.logger.warning(f"Introspeccao inoperante para {nomeTabela}. Nao havera corte de colunas dinâmicas neste ciclo.")
            return dados
            
        colunasIgnoradasGlobais: Set[str] = set()
        dadosAlinhados = []
        
        for registro in dados:
            registroFiltrado = {}
            for chave, valor in registro.items():
                if chave in colunasPermitidas:
                    registroFiltrado[chave] = valor
                else:
                    colunasIgnoradasGlobais.add(chave)
            dadosAlinhados.append(registroFiltrado)
            
        if colunasIgnoradasGlobais:
            self.logger.info(f"Colunas ignoradas por inexistencia no schema da tabela '{nomeTabela}': {', '.join(sorted(colunasIgnoradasGlobais))}")
            
        return dadosAlinhados

    def removerDuplicidades(self, dados: List[Dict[str, Any]], nomeTabela: str) -> List[Dict[str, Any]]:
        """
        Garante a unicidade dos registros pelo ID antes de fatiar os lotes.
        Aplica logica de prioridade para registros mais atualizados ou mais densos.
        """
        registrosUnicos = {}
        totalDuplicadosRemovidos = 0
        
        for registro in dados:
            chavePrimaria = registro.get("id")
            
            if not chavePrimaria:
                continue
                
            if chavePrimaria in registrosUnicos:
                totalDuplicadosRemovidos += 1
                registroExistente = registrosUnicos[chavePrimaria]
                
                dataAtualizacaoNovo = registro.get("updated_at")
                dataAtualizacaoExistente = registroExistente.get("updated_at")
                
                if dataAtualizacaoNovo and dataAtualizacaoExistente:
                    if str(dataAtualizacaoNovo) > str(dataAtualizacaoExistente):
                        registrosUnicos[chavePrimaria] = registro
                else:
                    densidadeNovo = sum(1 for valor in registro.values() if valor is not None)
                    densidadeExistente = sum(1 for valor in registroExistente.values() if valor is not None)
                    if densidadeNovo > densidadeExistente:
                        registrosUnicos[chavePrimaria] = registro
            else:
                registrosUnicos[chavePrimaria] = registro
                
        if totalDuplicadosRemovidos > 0:
            self.logger.warning(f"{totalDuplicadosRemovidos} registros duplicados removidos da entidade {nomeTabela} em memoria")
            
        registrosSemId = [r for r in dados if not r.get("id")]
        if registrosSemId:
            self.logger.warning(f"{len(registrosSemId)} registros sem chave primaria 'id' detectados em {nomeTabela}. Insercao pode gerar comportamento imprevisivel.")
            
        return list(registrosUnicos.values()) + registrosSemId

    def normalizarInteiros(self, dados: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normaliza explicitamente colunas que devem ser inteiros puros,
        corrigindo o artefato do Pandas que transforma ints com NaN em floats.
        """
        colunasAlvoEstaticas = {
            "id", "pipeline_id", "stage_id", "organization_id", 
            "lost_reason_id", "owner_id", "campaign_id", "source_id", "rating"
        }
        
        for registro in dados:
            for chave, valor in registro.items():
                if valor is None:
                    continue
                    
                if chave in colunasAlvoEstaticas or str(chave).endswith("_id"):
                    if isinstance(valor, list):
                        novoArray = []
                        for item in valor:
                            try:
                                novoArray.append(int(float(item)))
                            except (ValueError, TypeError):
                                novoArray.append(item)
                        registro[chave] = novoArray
                    else:
                        try:
                            registro[chave] = int(float(valor))
                        except (ValueError, TypeError):
                            pass
                            
        return dados

    def sanitizarDados(self, dadosEntrada: Any) -> Any:
        """
        Escudo final e recursivo: Varre os dados uma ultima vez antes da insercao, 
        garantindo conformidade estrita do JSON (bloqueio absoluto contra NaN/Inf).
        """
        if isinstance(dadosEntrada, dict):
            return {chave: self.sanitizarDados(valor) for chave, valor in dadosEntrada.items()}
        if isinstance(dadosEntrada, list):
            return [self.sanitizarDados(item) for item in dadosEntrada]
            
        if isinstance(dadosEntrada, float):
            if math.isnan(dadosEntrada) or math.isinf(dadosEntrada):
                return None
            if dadosEntrada.is_integer():
                return int(dadosEntrada)
                
        if isinstance(dadosEntrada, str):
            if dadosEntrada.endswith(".0"):
                try:
                    valFloat = float(dadosEntrada)
                    if valFloat.is_integer():
                        return int(valFloat)
                except ValueError:
                    pass
                    
        return dadosEntrada

    def carregarDados(self, dadosTransformados: List[Dict[str, Any]], nomeTabela: str) -> None:
        """
        Orquestra a passagem do payload por todos os escudos de protecao estrutural
        e aplica o upsert idempotente loteado na camada Bronze.
        """
        if not dadosTransformados:
            self.logger.info(f"Volume zero detectado para tabela {nomeTabela}.")
            return

        self.logger.info(f"Iniciando ciclo de protecao de dados e insercao para {len(dadosTransformados)} registros em bronze.{nomeTabela}")
        
        dadosDeduplicados = self.removerDuplicidades(dadosTransformados, nomeTabela)
        dadosAlinhados = self.alinharSchema(dadosDeduplicados, nomeTabela)
        dadosNormalizados = self.normalizarInteiros(dadosAlinhados)
        
        tamanhoLote = 500
        totalLotes = (len(dadosNormalizados) // tamanhoLote) + (1 if len(dadosNormalizados) % tamanhoLote > 0 else 0)
        
        for indiceRegistro in range(0, len(dadosNormalizados), tamanhoLote):
            loteAtual = dadosNormalizados[indiceRegistro : indiceRegistro + tamanhoLote]
            loteAtualSanitizado = self.sanitizarDados(loteAtual)
            numeroLoteAtual = (indiceRegistro // tamanhoLote) + 1
            
            try:
                self.logger.info(f"Gravando pacote transacional {numeroLoteAtual}/{totalLotes} na entidade {nomeTabela}")
                self.clienteSupabase.table(nomeTabela).upsert(
                    loteAtualSanitizado, 
                    on_conflict="id", 
                    returning="minimal"
                ).execute()
                
            except Exception as erroInsercao:
                self.logger.error(f"PostgreSQL rejeitou transacao no lote {numeroLoteAtual} da entidade {nomeTabela}. Causa base: {str(erroInsercao)}")
                raise

        self.logger.info(f"Camada Bronze atualizada integralmente com sucesso na rota {nomeTabela}")