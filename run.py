import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from src.pipeline import Pipeline
from src.pipeline_silver import PipelineSilver

def configurarLogs():
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
    
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

def main():
    load_dotenv(override=True)
    configurarLogs()
    
    loggerPrincipal = logging.getLogger("Main")
    loggerPrincipal.info("Iniciando a execucao da Pipeline ETL RD Station (Bronze e Silver)")
    
    try:
        pipelineBronze = Pipeline()
        pipelineBronze.executarPipeline()
        
        pipelineSilver = PipelineSilver()
        pipelineSilver.executarPipeline()
        
        loggerPrincipal.info("Pipeline ETL completa finalizada com exito")
    except Exception as erroExecucao:
        loggerPrincipal.error(f"Falha critica na execucao da Pipeline: {str(erroExecucao)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
    