from supabase import create_client, ClientOptions
import os
from typing import Dict, List, Any
from dotenv import load_dotenv

load_dotenv()


class Load:
    """
    Classe responsável por carregar os dados transformados em um banco de dados Supabase.
    """

    def __init__(self):
        """
        Inicializa a conexão com o Supabase, direcionando as requisições para o schema correto.
        """
        print("[LOAD] Conexão com Supabase")

        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_KEY")

        opts = ClientOptions(schema="bronze")
        
        self.supabase = create_client(self.url, self.key, options=opts)
        self.schema = "bronze"

    def _insert_batch(self, table: str, rows: List[Dict[str, Any]], batch_size: int = 500) -> None:
        """
        Insere registros de forma particionada para prevenir rejeições por payload excessivo.

        Args:
            table (str): Nome da tabela destino.
            rows (List[Dict[str, Any]]): Lista de registros para inserção.
            batch_size (int): Quantidade de registros enviados por requisição.
        """

        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]

            (
                self.supabase
                .table(table)
                .insert(batch)
                .execute()
            )

    def load_all(self, data: Dict[str, List[Dict[str, Any]]]) -> None:
        """
        Itera pelas entidades processadas e executa a carga no banco.

        Args:
            data (Dict[str, List[Dict[str, Any]]]): Dicionário com os dados a serem carregados.
        """
        for table, rows in data.items():
            if not rows:
                continue

            try:
                print(f"[LOAD] Inserindo em {self.schema}.{table} ({len(rows)} registros)")
                self._insert_batch(table, rows)
            except Exception as e:
                print(f"[ERRO LOAD] {table}: {e}")