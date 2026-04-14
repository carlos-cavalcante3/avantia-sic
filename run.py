import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from src.pipeline import Pipeline

def configurar_logs():
    diretorio_logs = "logs"
    os.makedirs(diretorio_logs, exist_ok=True)

    data_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
    caminho_log = os.path.join(diretorio_logs, f"execucao_{data_hora}.log")

    formatacao = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    
    # Limpa handlers anteriores para evitar logs duplicados no console
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format=formatacao,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(caminho_log, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    # O Segredo: Silencia a poluição do httpx (requisições do Supabase)
    logging.getLogger("httpx").setLevel(logging.WARNING)

def main():
    configurar_logs()
    load_dotenv(override=True)

    url_supabase = os.getenv("SUPABASE_URL")
    chave_supabase = os.getenv("SUPABASE_KEY")

    esquemas = {
        "users": ["id", "name", "email", "created_at", "updated_at"],
        "teams": ["id", "name", "created_at", "updated_at"],
        
        "organizations": [
            "id", "name", "owner_id", "cnpj", "razao_social", "nome_fantasia", 
            "cidade", "estado", "endereco", "telefone", "segment_ids", 
            "created_at", "updated_at"
        ],
        
        "pipelines": [
            "id", "name", "order", "stage_ids", "created_at", "updated_at"
        ],
        
        "contacts": [
            "id", "name", "job_title", "organization_id", "email", "phones", 
            "created_at", "updated_at"
        ],
        
        "deals": [
            "id", "name", "status", "total_price", "recurrence_price", 
            "one_time_price", "expected_close_date", "closed_at", "pipeline_id", 
            "stage_id", "owner_id", "organization_id", "lost_reason_id", 
            "source_id", "campaign_id", "contact_ids", "rating", "created_at", 
            "updated_at", "tipo_de_contrato", "proposta_entregue", 
            "audio_e_video", "descricao", "amount", "deal_stage_id"
        ],
        
        "tasks": ["id", "subject", "type", "date", "created_at", "updated_at"]
    }

    fluxo = Pipeline(url_supabase, chave_supabase, esquemas)
    fluxo.executar()

if __name__ == "__main__":
    main()