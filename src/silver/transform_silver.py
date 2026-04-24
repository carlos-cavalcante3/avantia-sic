import pandas as pd
import numpy as np
import json
import ast
import logging

class TransformSilver:
    """
    Responsavel por executar transformacoes analiticas robustas nos dados brutos,
    deserializando JSONs complexos, padronizando strings e garantindo a limpeza
    absoluta dos registros transacionais antes da persistencia na camada Silver.
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def extrairChaveJson(self, valorAnalisado, chaveDesejada):
        if pd.isna(valorAnalisado) or not valorAnalisado:
            return None

        valorAtual = valorAnalisado
        limiteTentativas = 3

        for _ in range(limiteTentativas):
            if isinstance(valorAtual, str):
                try:
                    valorAtual = json.loads(valorAtual)
                except Exception:
                    try:
                        valorAtual = ast.literal_eval(valorAtual)
                    except Exception:
                        break

            if isinstance(valorAtual, list):
                if len(valorAtual) > 0:
                    valorAtual = valorAtual[0]
                else:
                    return None

            if isinstance(valorAtual, dict):
                return valorAtual.get(chaveDesejada, None)

        return None

    def formatarNomeProprio(self, nomeBruto):
        if pd.isna(nomeBruto) or not str(nomeBruto).strip():
            return None

        palavrasOriginal = str(nomeBruto).lower().split()
        preposicoes = {"de", "da", "do", "dos", "das"}
        palavrasTratadas = []

        for indice, palavra in enumerate(palavrasOriginal):
            if palavra in preposicoes and indice > 0:
                palavrasTratadas.append(palavra)
            else:
                palavrasTratadas.append(palavra.capitalize())

        return " ".join(palavrasTratadas)

    def padronizarDataframe(self, tabelaDados):
        tabelaDados.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        tabelaDados.replace({np.nan: None}, inplace=True)
        return tabelaDados

    def deduplicarInteligente(self, tabelaDados, chave="id"):
        tabelaDados['non_null_count'] = tabelaDados.notna().sum(axis=1)
        tabelaLimpa = tabelaDados.sort_values('non_null_count', ascending=False).drop_duplicates(subset=[chave], keep='first')
        return tabelaLimpa.drop(columns=['non_null_count'])

    def normalizarInteiros(self, listaRegistros):
        for registro in listaRegistros:
            for chave, valor in registro.items():
                if valor is None:
                    continue
                if str(chave).endswith("_id") or chave == "rating":
                    try:
                        registro[chave] = int(float(valor))
                    except (ValueError, TypeError):
                        pass
        return listaRegistros

    def processarContatos(self, dadosBronze):
        tabelaContatos = pd.DataFrame(dadosBronze)
        if tabelaContatos.empty:
            return []

        tabelaContatos['name'] = tabelaContatos['name'].apply(self.formatarNomeProprio)
        tabelaContatos['email_principal'] = tabelaContatos['emails'].apply(lambda valor: self.extrairChaveJson(valor, 'email'))
        tabelaContatos['telefone_principal'] = tabelaContatos['phones'].apply(lambda valor: self.extrairChaveJson(valor, 'phone'))
        
        colunasUteis = ['id', 'name', 'job_title', 'email_principal', 'telefone_principal', 'organization_id', 'context_origin', 'created_at', 'updated_at']
        colunasPresentes = [coluna for coluna in colunasUteis if coluna in tabelaContatos.columns]
        tabelaContatos = tabelaContatos[colunasPresentes]
        
        tabelaPadronizada = self.padronizarDataframe(tabelaContatos)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)
        
        listaDicionarios = tabelaDeduplicada.to_dict(orient='records')
        return self.normalizarInteiros(listaDicionarios)

    def processarNegocios(self, dadosBronze, mapaEtapasAtuais=None):
        tabelaNegocios = pd.DataFrame(dadosBronze)
        if tabelaNegocios.empty:
            return [], []

        tabelaNegocios.dropna(subset=['id', 'status'], inplace=True)
        
        colunasUteis = ['id', 'name', 'status', 'total_price', 'one_time_price', 'recurrence_price', 'expected_close_date', 'closed_at', 'pipeline_id', 'stage_id', 'owner_id', 'organization_id', 'lost_reason_id', 'rating', 'custom_fields_tipo_de_contrato', 'created_at', 'updated_at']
        colunasPresentes = [coluna for coluna in colunasUteis if coluna in tabelaNegocios.columns]
        tabelaNegocios = tabelaNegocios[colunasPresentes]
        
        tabelaPadronizada = self.padronizarDataframe(tabelaNegocios)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)
        
        listaDicionarios = tabelaDeduplicada.to_dict(orient='records')
        listaNegociosLimpos = self.normalizarInteiros(listaDicionarios)
        
        listaHistorico = []
        if mapaEtapasAtuais is not None:
            from datetime import datetime, timezone
            momentoAtual = datetime.now(timezone.utc).isoformat()
            
            for negocio in listaNegociosLimpos:
                idNegocio = str(negocio.get('id'))
                etapaNova = str(negocio.get('stage_id'))
                etapaAntiga = str(mapaEtapasAtuais.get(idNegocio))
                
                if etapaAntiga != "None" and etapaAntiga != etapaNova:
                    listaHistorico.append({
                        "deal_id": idNegocio,
                        "old_stage_id": etapaAntiga,
                        "new_stage_id": etapaNova,
                        "changed_at": momentoAtual
                    })

        return listaNegociosLimpos, listaHistorico

    def processarOrganizacoes(self, dadosBronze):
        tabelaOrganizacoes = pd.DataFrame(dadosBronze)
        if tabelaOrganizacoes.empty:
            return []

        tabelaOrganizacoes.dropna(subset=['id'], inplace=True)
        
        colunasUteis = ['id', 'name', 'owner_id', 'custom_fields_cidade', 'custom_fields_estado', 'custom_fields_razao_social', 'created_at', 'updated_at']
        colunasPresentes = [coluna for coluna in colunasUteis if coluna in tabelaOrganizacoes.columns]
        tabelaOrganizacoes = tabelaOrganizacoes[colunasPresentes]
        
        tabelaPadronizada = self.padronizarDataframe(tabelaOrganizacoes)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)
        
        listaDicionarios = tabelaDeduplicada.to_dict(orient='records')
        return self.normalizarInteiros(listaDicionarios)

    def processarPipelines(self, dadosBronze):
        tabelaPipelines = pd.DataFrame(dadosBronze)
        if tabelaPipelines.empty:
            return []

        tabelaPipelines.dropna(subset=['id'], inplace=True)
        
        colunasUteis = ['id', 'name', 'created_at', 'updated_at']
        colunasPresentes = [coluna for coluna in colunasUteis if coluna in tabelaPipelines.columns]
        tabelaPipelines = tabelaPipelines[colunasPresentes]
        
        tabelaPadronizada = self.padronizarDataframe(tabelaPipelines)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)
        
        listaDicionarios = tabelaDeduplicada.to_dict(orient='records')
        return self.normalizarInteiros(listaDicionarios)

    def processarTasks(self, dadosBronze):
        tabelaTasks = pd.DataFrame(dadosBronze)
        if tabelaTasks.empty:
            return []

        tabelaTasks.dropna(subset=['id'], inplace=True)
        
        if 'owner_ids' in tabelaTasks.columns:
            tabelaTasks['owner_ids'] = tabelaTasks['owner_ids'].astype(str).str.replace('"', '')
            # Atualização: substituindo inplace=True por reatribuição direta para evitar o ChainedAssignmentError do Pandas
            tabelaTasks['owner_ids'] = tabelaTasks['owner_ids'].replace('nan', np.nan)
            tabelaTasks['owner_ids'] = tabelaTasks['owner_ids'].replace('None', np.nan)
        
        colunasUteis = ['id', 'name', 'type', 'status', 'deal_id', 'owner_ids', 'due_date', 'completed_at', 'completed_by_id', 'created_by_id', 'description', 'created_at', 'updated_at']
        colunasPresentes = [coluna for coluna in colunasUteis if coluna in tabelaTasks.columns]
        tabelaTasks = tabelaTasks[colunasPresentes]
        
        tabelaPadronizada = self.padronizarDataframe(tabelaTasks)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)
        
        listaDicionarios = tabelaDeduplicada.to_dict(orient='records')
        return self.normalizarInteiros(listaDicionarios)

    def processarTeams(self, dadosBronze):
        tabelaTeams = pd.DataFrame(dadosBronze)
        if tabelaTeams.empty:
            return []

        tabelaTeams.dropna(subset=['id'], inplace=True)
        
        colunasUteis = ['id', 'name', 'created_at', 'updated_at']
        colunasPresentes = [coluna for coluna in colunasUteis if coluna in tabelaTeams.columns]
        tabelaTeams = tabelaTeams[colunasPresentes]
        
        tabelaPadronizada = self.padronizarDataframe(tabelaTeams)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)
        
        listaDicionarios = tabelaDeduplicada.to_dict(orient='records')
        return self.normalizarInteiros(listaDicionarios)

    def processarUsers(self, dadosBronze):
        tabelaUsers = pd.DataFrame(dadosBronze)
        if tabelaUsers.empty:
            return []

        tabelaUsers.dropna(subset=['id'], inplace=True)
        
        colunasUteis = ['id', 'name', 'email', 'created_at', 'updated_at']
        colunasPresentes = [coluna for coluna in colunasUteis if coluna in tabelaUsers.columns]
        tabelaUsers = tabelaUsers[colunasPresentes]
        
        tabelaPadronizada = self.padronizarDataframe(tabelaUsers)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)
        
        listaDicionarios = tabelaDeduplicada.to_dict(orient='records')
        return self.normalizarInteiros(listaDicionarios)