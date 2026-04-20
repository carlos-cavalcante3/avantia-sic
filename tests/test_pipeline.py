from src.pipeline import Pipeline

def test_integridade_minima_pipeline():
    try:
        pipeline = Pipeline()
        
        # Valida se os componentes foram instanciados corretamente
        assert pipeline.moduloExtracao is not None
        assert pipeline.moduloTransformacao is not None
        assert pipeline.moduloCarga is not None
        
        # Valida se os recursos obrigatorios estao configurados
        assert "deals" in pipeline.listaRecursos
        assert "contacts" in pipeline.listaRecursos
        
        print("\nintegridade minima da execucao da pipeline garantida")
        print("Teste bem sucedido")
    except Exception as e:
        print("\nfalha na integridade minima da execucao da pipeline")
        print("Teste falhou")
        raise e