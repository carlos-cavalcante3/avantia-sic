import os
import sys
from dotenv import load_dotenv
from supabase import create_client, ClientOptions

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
load_dotenv()

def test_bronze_insert():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    opts = ClientOptions(schema="bronze")
    
    supabase = create_client(url, key, options=opts)

    batch = [
        {"teste1": "Dado A", "teste2": "Dado B", "teste3": "Dado C"},
        {"teste1": "Dado X", "teste2": "Dado Y", "teste3": "Dado Z"}
    ]

    try:
        print("Inserção no schema bronze.TESTE")

        response = (
            supabase
            .table("TESTE")
            .insert(batch)
            .execute()
        )

        print(f"Dados inseridos: {response.data}")

    except Exception as e:
        print(f"Falha na carga do teste: {e}")

if __name__ == "__main__":
    test_bronze_insert()