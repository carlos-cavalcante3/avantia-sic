import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from src.pipeline import Pipeline

def configurarLogs() -> None:
    """
    Configura o sistema de logging centralizado para toda a aplicacao, 
    padronizando a saida para monitoramento em producao via console 
    e persistindo o historico em arquivos de log dedicados.
    """
    diretorioBase = os.path.dirname(os.path.abspath(__file__))
    diretorioLogs = os.path.join(diretorioBase, "logs")
    os.makedirs(diretorioLogs, exist_ok=True)
    
    dataHoraFormatada = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    caminhoArquivoLog = os.path.join(diretorioLogs, f"pipeline_{dataHoraFormatada}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(caminhoArquivoLog, encoding="utf-8")
        ]
    )

def main() -> None:
    """
    Ponto de entrada do sistema. Carrega as variaveis de ambiente e orquestra 
    a inicializacao da pipeline de dados garantindo tratamento global de falhas.
    """
    load_dotenv(override=True)
    configurarLogs()
    
    loggerPrincipal = logging.getLogger("Main")
    loggerPrincipal.info("Iniciando a execucao da Pipeline ETL RD Station CRM para Supabase")
    
    try:
        pipelinePrincipal = Pipeline()
        pipelinePrincipal.executarPipeline()
        loggerPrincipal.info("Pipeline ETL finalizada com sucesso")
    except Exception as erroExecucao:
        loggerPrincipal.error(f"Falha critica na execucao da Pipeline: {str(erroExecucao)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()