import json
import math
import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Any

class Transform:
    """
    Normaliza os dados brutos mantendo arquitetura de camada Bronze,
    sanitizando invalid floats (NaN, inf), tratando timezones rigorosamente,
    achatando JSONs e convertendo arrays simples em strings separadas por virgula.
    Garante tambem a padronizacao de nomenclaturas para snake_case.
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    def limparTiposComplexos(self, valorDado: Any) -> Any:
        if isinstance(valorDado, dict):
            return json.dumps(valorDado, ensure_ascii=False)
        if isinstance(valorDado, list):
            if all(isinstance(itemLista, (str, int, float, bool)) for itemLista in valorDado):
                return ", ".join(str(itemLista) for itemLista in valorDado)
            listaLimpa = [self.limparTiposComplexos(itemAcesso) if isinstance(itemAcesso, (dict, list)) else itemAcesso for itemAcesso in valorDado]
            return json.dumps(listaLimpa, ensure_ascii=False)
        return valorDado

    def normalizarDados(self, dadosEntrada: List[Dict[str, Any]]) -> pd.DataFrame:
        if not dadosEntrada:
            return pd.DataFrame()
        tabelaNormalizada = pd.json_normalize(dadosEntrada, sep='_')
        tabelaNormalizada.columns = [str(nomeColuna).replace('-', '_') for nomeColuna in tabelaNormalizada.columns]
        return tabelaNormalizada

    def sanitizarEstruturasRecursivas(self, valorDado: Any) -> Any:
        if isinstance(valorDado, dict):
            return {chave: self.sanitizarEstruturasRecursivas(valor) for chave, valor in valorDado.items()}
        if isinstance(valorDado, list):
            return [self.sanitizarEstruturasRecursivas(item) for item in valorDado]
        if isinstance(valorDado, float):
            if math.isnan(valorDado) or math.isinf(valorDado):
                return None
        return valorDado

    def transformarDados(self, dadosBrutos: List[Dict[str, Any]], nomeRecurso: str) -> List[Dict[str, Any]]:
        self.logger.info(f"Iniciando transformacao para {nomeRecurso}. Registros brutos: {len(dadosBrutos)}")
        
        if not dadosBrutos:
            self.logger.warning(f"Nenhum dado fornecido para transformacao em {nomeRecurso}")
            return []

        tabelaDados = self.normalizarDados(dadosBrutos)
        
        for nomeColuna in tabelaDados.columns:
            tabelaDados[nomeColuna] = tabelaDados[nomeColuna].apply(self.limparTiposComplexos)
        
        tabelaDados.replace([np.inf, -np.inf], np.nan, inplace=True)
        tabelaDados = tabelaDados.where(pd.notnull(tabelaDados), None)
        
        palavrasChaveData = ['created_at', 'updated_at', 'closed_at', 'completed_at', 'due_date', 'data_de_', 'data_da_']
        for nomeColuna in tabelaDados.columns:
            if any(palavra in str(nomeColuna).lower() for palavra in palavrasChaveData):
                try:
                    tabelaDados[nomeColuna] = pd.to_datetime(tabelaDados[nomeColuna], utc=True, errors='coerce')
                    tabelaDados[nomeColuna] = tabelaDados[nomeColuna].dt.strftime('%Y-%m-%dT%H:%M:%SZ').where(pd.notnull(tabelaDados[nomeColuna]), None)
                except Exception as erroConversao:
                    self.logger.warning(f"Ignorando conversao forcada de data {nomeColuna}: {str(erroConversao)}")

        dadosTransformados = tabelaDados.to_dict(orient='records')
        dadosTransformados = self.sanitizarEstruturasRecursivas(dadosTransformados)
        
        self.logger.info(f"Transformacao validada para {nomeRecurso}. Colecao pronta para insercao.")
        return dadosTransformados