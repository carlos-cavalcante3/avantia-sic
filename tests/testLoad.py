import os
from supabase import create_client, ClientOptions

def test_conexao_e_insercao_supabase():
    try:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        
        opcoes = ClientOptions(schema="bronze")
        cliente = create_client(url, key, options=opcoes)

        dado_teste = {
            "teste1": "verificacao de saude",
            "teste2": "pytest automatizado",
            "teste3": "sucesso"
        }
        
        resposta = cliente.table("TESTE").upsert(dado_teste, returning="minimal").execute()
        
        print("\ncarga no supabase realizada")
        print("Teste bem sucedido")
    except Exception as e:
        print("\nfalha na carga no supabase")
        print("Teste falhou")
        raise e