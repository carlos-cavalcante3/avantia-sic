import logging
from src.extract import Extract
from src.transform import Transform
from src.load import Load

class Pipeline:
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
            "stages",
            "tasks",
            "teams"
        ]

    def processarRecurso(self, nomeRecurso: str) -> None:
        try:
            dadosBrutos = self.moduloExtracao.extrairRecurso(nomeRecurso)
            if not dadosBrutos:
                return
            dadosTransformados = self.moduloTransformacao.transformarDados(dadosBrutos, nomeRecurso)
            self.moduloCarga.carregarDados(dadosTransformados, nomeRecurso)
        except Exception as e:
            self.logger.error(f"Erro crítico ao processar o recurso '{nomeRecurso}': {str(e)}")
            raise

    def executarPipeline(self) -> None:
        for recursoAtual in self.listaRecursos:
            self.processarRecurso(recursoAtual)