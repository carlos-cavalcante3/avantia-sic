import os
import json
from src.extract import Extract
from src.transform import Transform
from src.load import Load
from datetime import datetime

class Pipeline:
    def __init__(self):
        self.extract = Extract()
        self.transform = Transform()
        self.load = Load()

    def _save_raw(self, raw_data: dict) -> None:
        raw_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))
        os.makedirs(raw_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(raw_dir, f"rd_crm_raw_{timestamp}.json")

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=4)
            
        print(f"[{datetime.now()}] Backup RAW salvo em: {filepath}")

    def run(self) -> None:
        print(f"\n[{datetime.now()}] Pipeline iniciado")

        raw_data = self.extract.fetch_all()
        print(f"[{datetime.now()}] Extração concluída")

        self._save_raw(raw_data)

        transformed_data = self.transform.process(raw_data)
        print(f"[{datetime.now()}] Transformação concluída")

        self.load.load_all(transformed_data)
        print(f"[{datetime.now()}] Load concluído")

        print(f"[{datetime.now()}] Finalizado\n")