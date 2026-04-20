import os

def test_leitura_variaveis_env():
    try:
        assert os.environ.get("SUPABASE_URL") is not None
        assert os.environ.get("SUPABASE_KEY") is not None
        assert os.environ.get("RD_ACCESS_TOKEN") is not None
        assert os.environ.get("RD_REFRESH_TOKEN") is not None
        
        print("\nleitura de variaveis .env realizada")
        print("Teste bem sucedido")
    except Exception as e:
        print("\nfalha na leitura de variaveis .env")
        print("Teste falhou")
        raise e