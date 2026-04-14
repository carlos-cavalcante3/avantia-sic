import sys
from src.pipeline import Pipeline


def main() -> None:
    """
    Ponto de entrada principal para a execução do ETL.
    Responsável por instanciar a classe orquestradora e tratar falhas críticas de execução.
    """
    print("Iniciando o sistema de integração de dados...")
    
    try:
        etl = Pipeline()
        
        etl.run()
        
    except Exception as e:
        print(f"\nO pipeline foi interrompido devido a um erro inesperado: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()