from src.transform import Transform

def test_transformacao_dataframe():
    try:
        transformador = Transform()
        
        payload_bruto = [{
            "id": 1,
            "name": "Negocio Pytest",
            "custom_fields": {
                "data-da-criacao": "10/10/2025",
                "audio-e-video": ["Nao", "Sim"]
            }
        }]
        
        resultado = transformador.transformarDados(payload_bruto, "deals")
        
        assert isinstance(resultado, list)
        assert len(resultado) == 1
        
        registro = resultado[0]
        assert "custom_fields_data_da_criacao" in registro
        assert "custom_fields_audio_e_video" in registro
        assert registro["custom_fields_audio_e_video"] == "Nao, Sim"
        
        print("\ntransformacao com DataFrame realizada")
        print("Teste bem sucedido")
    except Exception as e:
        print("\nfalha na transformacao com DataFrame")
        print("Teste falhou")
        raise e