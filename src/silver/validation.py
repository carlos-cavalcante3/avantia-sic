import logging
from src.silver.load_silver import LoadSilver

class Validate:
    """
    Responsavel por auditar a integridade volumetrica da pipeline,
    comparando os registros extraidos na camada Bronze contra os dados persistidos na camada Silver.
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        instanciaCarga = LoadSilver()
        self.clienteSupabase = instanciaCarga.clienteSupabase 

    def validarPerdaDados(self, nomeTabela):
        try:
            respostaBronze = self.clienteSupabase.schema("bronze").table(nomeTabela).select("id", count="exact").limit(1).execute()
            quantidadeBronze = respostaBronze.count if respostaBronze.count else 0

            respostaSilver = self.clienteSupabase.schema("silver").table(nomeTabela).select("id", count="exact").limit(1).execute()
            quantidadeSilver = respostaSilver.count if respostaSilver.count else 0

            if quantidadeBronze == 0:
                return

            diferencaPercentual = abs(quantidadeBronze - quantidadeSilver) / quantidadeBronze

            if diferencaPercentual > 0.01:
                self.logger.warning(f"Alerta de integridade na tabela {nomeTabela}. Bronze: {quantidadeBronze} | Silver: {quantidadeSilver}")
            else:
                self.logger.info(f"Integridade validada para a tabela {nomeTabela}.")
                
        except Exception as erroValidacao:
            self.logger.error(f"Falha ao validar integridade da tabela {nomeTabela}: {str(erroValidacao)}")