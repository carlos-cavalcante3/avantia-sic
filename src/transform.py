import logging

class Transform:
    """
    Classe responsável por adequar os dados brutos ao schema do banco destino.
    Aplica deduplicação inteligente (Merge/Coalesce) por ID para evitar conflitos
    de transação preservando o máximo de dados não-nulos.
    """
    def __init__(self, esquemas_tabelas):
        self.esquemas_tabelas = esquemas_tabelas
        self.logger = logging.getLogger("Transform")

    def preparar_dados(self, nome_endpoint, dados_brutos):
        self.logger.info(f"[{nome_endpoint}] Iniciando transformação de {len(dados_brutos)} registros...")
        
        colunas_esperadas = self.esquemas_tabelas.get(nome_endpoint, [])
        registros_unicos = {}

        for registro in dados_brutos:
            
            if nome_endpoint == "deals":
                registro["amount"] = registro.get("total_price")
                registro["deal_stage_id"] = registro.get("stage_id")
                
                campos_custom = registro.get("custom_fields")
                if isinstance(campos_custom, dict):
                    registro["tipo_de_contrato"] = campos_custom.get("tipo-de-contrato")
                    registro["proposta_entregue"] = campos_custom.get("proposta-entregue-ao-cliente")
                    registro["audio_e_video"] = campos_custom.get("audio-e-video")
                    registro["descricao"] = campos_custom.get("descricao")

            elif nome_endpoint == "contacts":
                lista_telefones = registro.get("phones")
                if isinstance(lista_telefones, list) and len(lista_telefones) > 0:
                    registro["phones"] = lista_telefones[0].get("phone")
                else:
                    registro["phones"] = None

                lista_emails = registro.get("emails")
                if isinstance(lista_emails, list) and len(lista_emails) > 0:
                    registro["email"] = lista_emails[0].get("email")
                else:
                    registro["email"] = None

                if not registro.get("job_title"):
                    registro["job_title"] = registro.get("title")

            elif nome_endpoint == "organizations":
                campos_custom = registro.get("custom_fields")
                if isinstance(campos_custom, dict):
                    registro["cnpj"] = registro.get("cnpj", campos_custom.get("cnpj"))
                    registro["razao_social"] = registro.get("razao_social", campos_custom.get("razao_social"))
                    registro["nome_fantasia"] = registro.get("nome_fantasia", campos_custom.get("nome_fantasia"))
                    registro["cidade"] = registro.get("cidade", campos_custom.get("cidade"))
                    registro["estado"] = registro.get("estado", campos_custom.get("estado"))
                    registro["endereco"] = registro.get("endereco", campos_custom.get("endereco"))
                    registro["telefone"] = registro.get("telefone", campos_custom.get("telefone"))

            registro_limpo = {}

            for chave, valor in registro.items():
                if colunas_esperadas and chave not in colunas_esperadas:
                    continue

                if chave == "id":
                    valor_tratado = str(valor)
                else:
                    valor_tratado = valor

                registro_limpo[chave] = valor_tratado

            if colunas_esperadas:
                for coluna in colunas_esperadas:
                    if coluna not in registro_limpo:
                        registro_limpo[coluna] = None

            id_registro = registro_limpo.get("id")
            if id_registro:
                if id_registro not in registros_unicos:
                    registros_unicos[id_registro] = registro_limpo
                else:
                    for chave, valor_novo in registro_limpo.items():
                        if valor_novo is not None and valor_novo != "":
                            registros_unicos[id_registro][chave] = valor_novo

        dados_transformados = list(registros_unicos.values())
        
        duplicatas_removidas = len(dados_brutos) - len(dados_transformados)
        if duplicatas_removidas > 0:
            self.logger.info(f"[{nome_endpoint}] Deduplicação: {duplicatas_removidas} conflitos mesclados para enriquecimento de dados.")

        self.logger.info(f"[{nome_endpoint}] Transformação concluída. Lote final: {len(dados_transformados)} registros.")
        return dados_transformados