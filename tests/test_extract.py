from src.extract import Extract

def test_autenticacao_api():
    try:
        extrator = Extract()
        cabecalhos = extrator.obterCabecalhos()
        
        assert "Authorization" in cabecalhos
        assert cabecalhos["Authorization"].startswith("Bearer ")
        assert "Accept" in cabecalhos
        
        print("\nautenticacao/token validada")
        print("Teste bem sucedido")
    except Exception as e:
        print("\nfalha na autenticacao/token")
        print("Teste falhou")
        raise e

def test_salvamento_csv_raw():
    try:
        extrator = Extract()
        dados_ficticios = [{"id": 999, "name": "Teste Pytest", "status": "ativo"}]
        
        extrator.salvarArquivoRaw(dados_ficticios, "teste_automatizado_pytest")
        
        print("\nsalvamento de CSV raw realizado")
        print("Teste bem sucedido")
    except Exception as e:
        print("\nfalha no salvamento de CSV raw")
        print("Teste falhou")
        raise e