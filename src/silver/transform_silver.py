import pandas as pd
import numpy as np
import json
import ast
import logging

"""
TransformSilver: Camada de processamento e sanitização de dados da arquitetura Medallion.

Esta classe é responsável pela transição dos dados da camada Bronze (Raw) para a Silver (Curated).
Seu papel fundamental é garantir a integridade analítica através de:
1. Normalização de esquemas e padronização de nomenclatura.
2. Sanitização de valores nulos e tipagem rigorosa (especialmente datas e identificadores).
3. Deduplicação inteligente baseada em densidade de preenchimento.
4. Auto-cura de registros críticos (ex: negócios ganhos sem data de fechamento).

Seguindo princípios de OO, cada método é especializado na transformação de uma entidade específica
da API RD Station, promovendo baixo acoplamento e alta coesão entre os domínios de negócio.
"""
class TransformSilver:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def extrairChaveJson(self, valorAnalisado, chaveDesejada):
        if pd.isna(valorAnalisado) or not valorAnalisado:
            return None
        valorAtual = valorAnalisado
        for _ in range(3):
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

        for col in tabelaDados.columns:
            if pd.api.types.is_datetime64_any_dtype(tabelaDados[col]):
                tabelaDados[col] = tabelaDados[col].dt.strftime('%Y-%m-%d %H:%M:%S').replace({np.nan: None})

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
                elif isinstance(valor, pd.Timestamp):
                    registro[chave] = valor.strftime('%Y-%m-%d %H:%M:%S')
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

        tabelaNegocios.dropna(subset=['id'], inplace=True)

        if 'status' in tabelaNegocios.columns:
            tabelaNegocios['status'] = tabelaNegocios['status'].astype(str).str.lower().str.strip()

        for col in ['closed_at', 'expected_close_date', 'updated_at']:
            if col in tabelaNegocios.columns:
                tabelaNegocios[col] = pd.to_datetime(tabelaNegocios[col], utc=True, errors='coerce')

        if 'closed_at' in tabelaNegocios.columns and 'updated_at' in tabelaNegocios.columns:
            condicao_won_sem_data = (tabelaNegocios['status'] == 'won') & (tabelaNegocios['closed_at'].isna())
            tabelaNegocios.loc[condicao_won_sem_data, 'closed_at'] = tabelaNegocios.loc[condicao_won_sem_data, 'updated_at']

        for col in ['closed_at', 'expected_close_date']:
            if col in tabelaNegocios.columns:
                tabelaNegocios[col] = tabelaNegocios[col].dt.tz_convert('America/Sao_Paulo').dt.strftime('%Y-%m-%d %H:%M:%S')

        for col in ['total_price', 'one_time_price', 'recurrence_price']:
            if col in tabelaNegocios.columns:
                tabelaNegocios[col] = pd.to_numeric(tabelaNegocios[col], errors='coerce').fillna(0.0)

        def extrair_motivo_limpo(valor_bruto):
            if pd.isna(valor_bruto) or not valor_bruto: return "Não Informado"
            if isinstance(valor_bruto, str) and '{' in valor_bruto:
                try: valor_bruto = ast.literal_eval(valor_bruto)
                except: pass
            if isinstance(valor_bruto, dict):
                motivo = valor_bruto.get('motivo-da-perda')
                return str(motivo).strip() if motivo and str(motivo).strip().lower() != 'none' else "Não Informado"
            return str(valor_bruto).strip()

        if 'custom_fields_motivo_da_perda' in tabelaNegocios.columns:
            tabelaNegocios['motivo_da_perda'] = tabelaNegocios['custom_fields_motivo_da_perda'].apply(lambda x: extrair_motivo_limpo(x) if isinstance(x, str) and '{' in x else x)
        elif 'custom_fields' in tabelaNegocios.columns:
            tabelaNegocios['motivo_da_perda'] = tabelaNegocios['custom_fields'].apply(extrair_motivo_limpo)
        else:
            tabelaNegocios['motivo_da_perda'] = 'Não Informado'

        tabelaNegocios['motivo_da_perda'] = tabelaNegocios['motivo_da_perda'].fillna('Não Informado').replace(['', 'None', 'nan', 'NaN', 'None.'], 'Não Informado')

        # === INCLUSÃO DA NOVA COLUNA ===
        colunasUteis = ['id', 'name', 'status', 'total_price', 'one_time_price', 'recurrence_price', 'expected_close_date', 'closed_at', 'pipeline_id', 'stage_id', 'owner_id', 'organization_id', 'lost_reason_id', 'rating', 'custom_fields_tipo_de_contrato', 'motivo_da_perda', 'custom_fields_proposta_entregue_ao_cliente', 'created_at', 'updated_at']
        colunasPresentes = [coluna for coluna in colunasUteis if coluna in tabelaNegocios.columns]

        tabelaFiltrada = tabelaNegocios[colunasPresentes]

        # === RENOMEANDO A COLUNA PARA A CAMADA SILVER ===
        if 'custom_fields_proposta_entregue_ao_cliente' in tabelaFiltrada.columns:
            tabelaFiltrada = tabelaFiltrada.rename(columns={'custom_fields_proposta_entregue_ao_cliente': 'proposta_entregue_ao_cliente'})

        tabelaPadronizada = self.padronizarDataframe(tabelaFiltrada)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)

        listaNegociosLimpos = self.normalizarInteiros(tabelaDeduplicada.to_dict(orient='records'))

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

    def processarStages(self, dadosBronze):
        tabelaStages = pd.DataFrame(dadosBronze)
        if tabelaStages.empty:
            return []
        tabelaStages.dropna(subset=['id'], inplace=True)
        if 'order' in tabelaStages.columns:
            tabelaStages.rename(columns={'order': 'stage_order'}, inplace=True)
        colunasUteis = ['id', 'pipeline_id', 'name', 'stage_order', 'created_at', 'updated_at']
        colunasPresentes = [coluna for coluna in colunasUteis if coluna in tabelaStages.columns]
        tabelaStages = tabelaStages[colunasPresentes]
        tabelaPadronizada = self.padronizarDataframe(tabelaStages)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)
        listaDicionarios = tabelaDeduplicada.to_dict(orient='records')
        return self.normalizarInteiros(listaDicionarios)

    def processarTasks(self, dadosBronze):
        tabelaTasks = pd.DataFrame(dadosBronze)
        if tabelaTasks.empty:
            return []
        tabelaTasks.dropna(subset=['id'], inplace=True)
        if 'owner_ids' in tabelaTasks.columns:
            tabelaTasks['owner_ids'] = tabelaTasks['owner_ids'].astype(str).str.replace('"', '').replace('nan', np.nan).replace('None', np.nan)
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
