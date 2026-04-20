import logging
from src.extract import Extract
from src.transform import Transform
from src.load import Load

class Pipeline:
    """
    Agregador estrutural que unifica os modulos em um unico pipeline robusto,
    controlando a execucao em ordem sequencial e estancando falhas irreversiveis por objeto.
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.moduloExtracao = Extract()
        self.moduloTransformacao = Transform()
        self.moduloCarga = Load()
        
        self.listaRecursos = [
            "deals",
            "contacts",
            "organizations",
            "users",
            "pipelines",
            "tasks",
            "teams"
        ]

    def processarRecurso(self, nomeRecurso: str) -> None:
        """
        Encapsula o ciclo de vida completo de uma extracao unica isolada por bloco try/except,
        garantindo que se tasks falhar, teams prossiga operando.
        """
        try:
            self.logger.info(f"=== Operacao ETL em curso - Analisando contexto: {nomeRecurso} ===")
            
            dadosBrutos = self.moduloExtracao.extrairRecurso(nomeRecurso)
            
            if not dadosBrutos:
                self.logger.warning(f"Sem resposta util para a rota {nomeRecurso}. Pulando para proximo ciclo.")
                return
                
            dadosTransformados = self.moduloTransformacao.transformarDados(dadosBrutos, nomeRecurso)
            self.moduloCarga.carregarDados(dadosTransformados, nomeRecurso)
            
            self.logger.info(f"=== Operacao ETL fechada para o contexto: {nomeRecurso} ===")
            
        except Exception as erroProcessamento:
            self.logger.error(f"Erro catastrófico retido no bloco do recurso {nomeRecurso}: {str(erroProcessamento)}", exc_info=True)

    def executarPipeline(self) -> None:
        """
        Dispara massivamente o processamento das rotas declaradas ate sua conclusao sistêmica.
        """
        self.logger.info("Partida automatizada da esteira ETL acionada")
        
        for recursoAtual in self.listaRecursos:
            self.processarRecurso(recursoAtual)
            
        self.logger.info("Processamento percorrido por todos os nos. Pipeline em estado ocioso e estavel.")