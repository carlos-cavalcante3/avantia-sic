import logging
from src.load import Load
from src.silver.transform_silver import TransformSilver
from src.silver.load_silver import LoadSilver
from src.silver.validation import Validate

class PipelineSilver:
    """
    Orquestrador oficial da camada Silver, encarregado de coordenar a leitura paginada dos dados brutos,
    acionar a transformacao de negocio, efetuar a carga analitica e validar a integridade da operacao
    para todos os endpoints do CRM.
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

        dadosNegociosBronze = self.lerDadosBronze("deals")
        negociosTransformados = self.moduloTransformacao.processarNegocios(dadosNegociosBronze)
        self.moduloCarga.carregarDados(negociosTransformados, "deals")
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

        self.logger.info("FInalizado com exito")