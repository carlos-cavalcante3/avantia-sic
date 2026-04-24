import logging
from src.load import Load
from src.silver.transform_silver import TransformSilver
from src.silver.load_silver import LoadSilver
from src.silver.validation import Validate

class PipelineSilver:
    """
    Orquestrador oficial da camada Silver. Efetua processamento hibrido com suporte
    a Slowly Changing Dimensions (SCD4) e execucao do snapshot diario para a camada Gold.
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        instanciaLeituraBronze = Load()
        self.leitorBronze = instanciaLeituraBronze.clienteSupabase
        self.moduloTransformacao = TransformSilver()
        self.moduloCarga = LoadSilver()
        self.moduloValidacao = Validate()

    def lerDadosBronze(self, nomeTabela):
        todosRegistros = []
        tamanhoLote = 1000
        indiceInicio = 0
        
        while True:
            indiceFim = indiceInicio + tamanhoLote - 1
            respostaApi = self.leitorBronze.schema("bronze").table(nomeTabela).select("*").range(indiceInicio, indiceFim).execute()
            
            registrosPagina = respostaApi.data
            if not registrosPagina:
                break
                
            todosRegistros.extend(registrosPagina)
            
            if len(registrosPagina) < tamanhoLote:
                break
                
            indiceInicio += tamanhoLote
            
        return todosRegistros

    def executarPipeline(self):
        self.logger.info("Pipeline Camada bronze -> Camada Silver iniciada")

        dadosContatosBronze = self.lerDadosBronze("contacts")
        contatosTransformados = self.moduloTransformacao.processarContatos(dadosContatosBronze)
        self.moduloCarga.carregarDados(contatosTransformados, "contacts")
        self.moduloValidacao.validarPerdaDados("contacts")

        mapaEtapasDeals = self.moduloCarga.obterMapeamentoEtapasAtuais()
        dadosNegociosBronze = self.lerDadosBronze("deals")
        negociosTransformados, historicoGerado = self.moduloTransformacao.processarNegocios(dadosNegociosBronze, mapaEtapasDeals)
        self.moduloCarga.carregarDados(negociosTransformados, "deals")
        if historicoGerado:
            self.moduloCarga.carregarHistoricoDeals(historicoGerado)
        self.moduloValidacao.validarPerdaDados("deals")

        dadosOrganizacoesBronze = self.lerDadosBronze("organizations")
        organizacoesTransformadas = self.moduloTransformacao.processarOrganizacoes(dadosOrganizacoesBronze)
        self.moduloCarga.carregarDados(organizacoesTransformadas, "organizations")
        self.moduloValidacao.validarPerdaDados("organizations")

        dadosPipelinesBronze = self.lerDadosBronze("pipelines")
        pipelinesTransformados = self.moduloTransformacao.processarPipelines(dadosPipelinesBronze)
        self.moduloCarga.carregarDados(pipelinesTransformados, "pipelines")
        self.moduloValidacao.validarPerdaDados("pipelines")

        dadosTasksBronze = self.lerDadosBronze("tasks")
        tasksTransformadas = self.moduloTransformacao.processarTasks(dadosTasksBronze)
        self.moduloCarga.carregarDados(tasksTransformadas, "tasks")
        self.moduloValidacao.validarPerdaDados("tasks")

        dadosTeamsBronze = self.lerDadosBronze("teams")
        teamsTransformados = self.moduloTransformacao.processarTeams(dadosTeamsBronze)
        self.moduloCarga.carregarDados(teamsTransformados, "teams")
        self.moduloValidacao.validarPerdaDados("teams")

        dadosUsersBronze = self.lerDadosBronze("users")
        usersTransformados = self.moduloTransformacao.processarUsers(dadosUsersBronze)
        self.moduloCarga.carregarDados(usersTransformados, "users")
        self.moduloValidacao.validarPerdaDados("users")
        
        self.moduloCarga.gerarSnapshotDiario()

        self.moduloCarga.atualizarCamadaGold()

        self.logger.info("FInalizado com exito")