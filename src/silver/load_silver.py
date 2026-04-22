import os
import logging
from supabase import create_client, ClientOptions

class LoadSilver:
    """
    Responsavel por estabelecer conexao isolada com o schema Silver do Supabase
    e persistir os dados analiticos utilizando operacoes transacionais e idempotentes.
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        urlBanco = os.environ.get("SUPABASE_URL")
        chaveBanco = os.environ.get("SUPABASE_KEY")
        
        if not urlBanco or not chaveBanco:
            raise ValueError("Credenciais do banco de dados inválidas")
            
        opcoesCliente = ClientOptions(schema="silver")
        self.clienteSupabase = create_client(urlBanco, chaveBanco, options=opcoesCliente)

    def carregarDados(self, listaRegistros, nomeTabela):
        if not listaRegistros:
            return
        
        tamanhoLote = 500
        totalRegistros = len(listaRegistros)
        
        for indice in range(0, totalRegistros, tamanhoLote):
            loteAtual = listaRegistros[indice : indice + tamanhoLote]
            self.clienteSupabase.table(nomeTabela).upsert(loteAtual, on_conflict="id", returning="minimal").execute()
            
        self.logger.info(f"Transformação bronze layer -> silver layer: {totalRegistros} registros na tabela {nomeTabela}.")