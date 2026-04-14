import logging
from supabase import create_client, ClientOptions

class Load:
    """
    Classe responsável por carregar os dados transformados no Supabase.
    Possui validação prévia de conexão com o banco.
    """
    def __init__(self, url, chave):
        opcoes = ClientOptions(schema="bronze")
        self.cliente = create_client(url, chave, options=opcoes)
        self.logger = logging.getLogger("Load")

    def testar_conexao(self):
        self.logger.info("Teste de conexão com Supabase")
        try:
            self.cliente.table("users").select("id").limit(1).execute()
            self.logger.info("Conexão com Supabase estabelecida")
            return True
        except Exception as erro:
            self.logger.error(f"Falha na conexão com Supabase: {erro}")
            return False

    def carregar_dados(self, nome_tabela, dados, tamanho_lote=500):
        total_registros = len(dados)
        self.logger.info(f"[{nome_tabela}] Iniciando carregamento de {total_registros} registros em lotes de {tamanho_lote}")

        for indice in range(0, total_registros, tamanho_lote):
            lote = dados[indice:indice + tamanho_lote]

            try:
                self.cliente.table(nome_tabela).upsert(lote).execute()
                self.logger.info(f"[{nome_tabela}] Lote carregado: registros {indice} a {indice + len(lote)}")
            except Exception as erro:
                self.logger.error(f"[{nome_tabela}] Erro ao carregar lote ({indice} a {indice + len(lote)}): {erro}")
                
        self.logger.info(f"[{nome_tabela}] Processo de carregamento finalizado.")