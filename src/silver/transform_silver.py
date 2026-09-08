import ast
import json
import logging
import re

import numpy as np
import pandas as pd


class TransformSilver:
    """
    Camada de processamento e sanitização de dados da arquitetura Medallion.

    Esta classe atua de forma orquestrada, sendo o transform oficial do ETL.
    Recebe os dados brutos (Bronze), aplica regras gerais de sanitização
    (compatibilidade com o antigo 'Transform') e, em seguida, direciona para
    os métodos específicos que moldam os dados tanto para o schema 'silver'
    quanto para o schema 'privado'.
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.ID_FUNIL_PRIVADO = "68b741e7b98f0b001b5c8d75"

    # =========================================================================
    # UTILITÁRIOS GERAIS DE SANITIZAÇÃO (Herança da camada Bronze/Transform)
    # =========================================================================

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
        tabelaDados.replace(r"^\s*$", np.nan, regex=True, inplace=True)
        tabelaDados.replace({np.nan: None}, inplace=True)

        for col in tabelaDados.columns:
            if pd.api.types.is_datetime64_any_dtype(tabelaDados[col]):
                tabelaDados[col] = (
                    tabelaDados[col]
                    .dt.strftime("%Y-%m-%d %H:%M:%S")
                    .replace({np.nan: None})
                )
        return tabelaDados

    def deduplicarInteligente(self, tabelaDados, chave="id"):
        tabelaDados["non_null_count"] = tabelaDados.notna().sum(axis=1)
        tabelaLimpa = tabelaDados.sort_values(
            "non_null_count", ascending=False
        ).drop_duplicates(subset=[chave], keep="first")
        return tabelaLimpa.drop(columns=["non_null_count"])

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
                    registro[chave] = valor.strftime("%Y-%m-%d %H:%M:%S")
        return listaRegistros

    def extrair_motivo_limpo(self, valor_bruto):
        if pd.isna(valor_bruto) or not valor_bruto:
            return "Não Informado"
        if isinstance(valor_bruto, str) and "{" in valor_bruto:
            try:
                valor_bruto = ast.literal_eval(valor_bruto)
            except:
                pass
        if isinstance(valor_bruto, dict):
            motivo = valor_bruto.get("motivo-da-perda")
            return (
                str(motivo).strip()
                if motivo and str(motivo).strip().lower() != "none"
                else "Não Informado"
            )
        return str(valor_bruto).strip()

    def limpar_array_rd(self, valor):
        if pd.isna(valor) or valor in [None, "", "NaN", "nan"]:
            return None
        valor_str = str(valor).strip()
        if valor_str.startswith("[") and valor_str.endswith("]"):
            try:
                lista = ast.literal_eval(valor_str)
                if isinstance(lista, list):
                    return ", ".join([str(item) for item in lista if item])
            except Exception:
                limpo = re.sub(r'[\[\]"\']', "", valor_str)
                return limpo.strip()
        return valor_str

    # =========================================================================
    # MÉTODOS DE PROCESSAMENTO (SCHEMA SILVER GERAL)
    # =========================================================================

    def processarContatos(self, dadosBronze):
        tabelaContatos = pd.DataFrame(dadosBronze)
        if tabelaContatos.empty:
            return []
        tabelaContatos["name"] = tabelaContatos["name"].apply(self.formatarNomeProprio)
        tabelaContatos["email_principal"] = tabelaContatos["emails"].apply(
            lambda valor: self.extrairChaveJson(valor, "email")
        )
        tabelaContatos["telefone_principal"] = tabelaContatos["phones"].apply(
            lambda valor: self.extrairChaveJson(valor, "phone")
        )
        colunasUteis = [
            "id",
            "name",
            "job_title",
            "email_principal",
            "telefone_principal",
            "organization_id",
            "context_origin",
            "created_at",
            "updated_at",
        ]
        colunasPresentes = [
            col for col in colunasUteis if col in tabelaContatos.columns
        ]
        tabelaContatos = tabelaContatos[colunasPresentes]

        tabelaPadronizada = self.padronizarDataframe(tabelaContatos)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)
        listaDicionarios = tabelaDeduplicada.to_dict(orient="records")
        return self.normalizarInteiros(listaDicionarios)

    def processarNegocios(self, dadosBronze, mapaEtapasAtuais=None):
        """
        Processa todos os negócios e retorna os dados moldados para o SCHEMA SILVER,
        além do histórico de etapas.
        """
        tabelaNegocios = pd.DataFrame(dadosBronze)
        if tabelaNegocios.empty:
            return [], []

        tabelaNegocios.dropna(subset=["id"], inplace=True)

        if "status" in tabelaNegocios.columns:
            tabelaNegocios["status"] = (
                tabelaNegocios["status"].astype(str).str.lower().str.strip()
            )

        for col in ["closed_at", "expected_close_date", "updated_at"]:
            if col in tabelaNegocios.columns:
                tabelaNegocios[col] = pd.to_datetime(
                    tabelaNegocios[col], utc=True, errors="coerce"
                )

        if (
            "closed_at" in tabelaNegocios.columns
            and "updated_at" in tabelaNegocios.columns
        ):
            condicao_won_sem_data = (tabelaNegocios["status"] == "won") & (
                tabelaNegocios["closed_at"].isna()
            )
            tabelaNegocios.loc[condicao_won_sem_data, "closed_at"] = tabelaNegocios.loc[
                condicao_won_sem_data, "updated_at"
            ]

        for col in ["closed_at", "expected_close_date"]:
            if col in tabelaNegocios.columns:
                tabelaNegocios[col] = (
                    tabelaNegocios[col]
                    .dt.tz_convert("America/Sao_Paulo")
                    .dt.strftime("%Y-%m-%d %H:%M:%S")
                )

        for col in ["total_price", "one_time_price", "recurrence_price"]:
            if col in tabelaNegocios.columns:
                tabelaNegocios[col] = pd.to_numeric(
                    tabelaNegocios[col], errors="coerce"
                ).fillna(0.0)

        # Lógica de extração de motivo de perda padrão do Silver
        if "custom_fields_motivo_da_perda" in tabelaNegocios.columns:
            tabelaNegocios["motivo_da_perda"] = tabelaNegocios[
                "custom_fields_motivo_da_perda"
            ].apply(
                lambda x: (
                    self.extrair_motivo_limpo(x)
                    if isinstance(x, str) and "{" in x
                    else x
                )
            )
        elif "custom_fields" in tabelaNegocios.columns:
            tabelaNegocios["motivo_da_perda"] = tabelaNegocios["custom_fields"].apply(
                self.extrair_motivo_limpo
            )
        else:
            tabelaNegocios["motivo_da_perda"] = "Não Informado"

        tabelaNegocios["motivo_da_perda"] = (
            tabelaNegocios["motivo_da_perda"]
            .fillna("Não Informado")
            .replace(["", "None", "nan", "NaN", "None."], "Não Informado")
        )

        colunasUteis = [
            "id",
            "name",
            "status",
            "total_price",
            "one_time_price",
            "recurrence_price",
            "expected_close_date",
            "closed_at",
            "pipeline_id",
            "stage_id",
            "owner_id",
            "organization_id",
            "lost_reason_id",
            "rating",
            "custom_fields_tipo_de_contrato",
            "motivo_da_perda",
            "custom_fields_proposta_entregue_ao_cliente",
            "custom_fields_data_de_entrega_da_proposta",
            "created_at",
            "updated_at",
        ]

        colunasPresentes = [
            col for col in colunasUteis if col in tabelaNegocios.columns
        ]
        tabelaFiltrada = tabelaNegocios[colunasPresentes]

        if "custom_fields_proposta_entregue_ao_cliente" in tabelaFiltrada.columns:
            tabelaFiltrada = tabelaFiltrada.rename(
                columns={
                    "custom_fields_proposta_entregue_ao_cliente": "proposta_entregue_ao_cliente"
                }
            )

        if "custom_fields_data_de_entrega_da_proposta" in tabelaFiltrada.columns:
            tabelaFiltrada = tabelaFiltrada.rename(
                columns={
                    "custom_fields_data_de_entrega_da_proposta": "data_de_entrega_da_proposta"
                }
            )

        if "data_de_entrega_da_proposta" in tabelaFiltrada.columns:
            tabelaFiltrada["data_de_entrega_da_proposta"] = pd.to_datetime(
                tabelaFiltrada["data_de_entrega_da_proposta"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")

        tabelaPadronizada = self.padronizarDataframe(tabelaFiltrada)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)

        listaNegociosLimpos = self.normalizarInteiros(
            tabelaDeduplicada.to_dict(orient="records")
        )

        listaHistorico = []
        if mapaEtapasAtuais is not None:
            from datetime import datetime, timezone

            momentoAtual = datetime.now(timezone.utc).isoformat()
            for negocio in listaNegociosLimpos:
                idNegocio = str(negocio.get("id"))
                etapaNova = str(negocio.get("stage_id"))
                etapaAntiga = str(mapaEtapasAtuais.get(idNegocio))
                if etapaAntiga != "None" and etapaAntiga != etapaNova:
                    listaHistorico.append(
                        {
                            "deal_id": idNegocio,
                            "old_stage_id": etapaAntiga,
                            "new_stage_id": etapaNova,
                            "changed_at": momentoAtual,
                        }
                    )

        return listaNegociosLimpos, listaHistorico

    # =========================================================================
    # MÉTODO DE PROCESSAMENTO ESPECÍFICO (SCHEMA PRIVADO)
    # =========================================================================

    def processarNegociosPrivado(self, dadosBronze):
        """
        Processa os negócios de forma rigorosa e exclusiva para o SCHEMA PRIVADO.
        Aplica limpeza estrita nos arrays do RD e limita à malha fina de colunas homologadas.
        """
        tabelaNegocios = pd.DataFrame(dadosBronze)
        if tabelaNegocios.empty:
            return []

        tabelaNegocios.dropna(subset=["id"], inplace=True)

        # Filtro estrito do funil privado
        if "pipeline_id" in tabelaNegocios.columns:
            tabelaNegocios = tabelaNegocios[
                tabelaNegocios["pipeline_id"] == self.ID_FUNIL_PRIVADO
            ]

        if tabelaNegocios.empty:
            return []

        # Substitui hífens nos nomes por underscores
        tabelaNegocios.columns = [
            col.replace("-", "_") for col in tabelaNegocios.columns
        ]

        if "status" in tabelaNegocios.columns:
            tabelaNegocios["status"] = (
                tabelaNegocios["status"].astype(str).str.lower().str.strip()
            )

        for col in ["closed_at", "expected_close_date", "created_at", "updated_at"]:
            if col in tabelaNegocios.columns:
                tabelaNegocios[col] = pd.to_datetime(
                    tabelaNegocios[col], utc=True, errors="coerce"
                )

        if (
            "closed_at" in tabelaNegocios.columns
            and "updated_at" in tabelaNegocios.columns
        ):
            condicao_won_sem_data = (tabelaNegocios["status"] == "won") & (
                tabelaNegocios["closed_at"].isna()
            )
            tabelaNegocios.loc[condicao_won_sem_data, "closed_at"] = tabelaNegocios.loc[
                condicao_won_sem_data, "updated_at"
            ]

        for col in ["closed_at", "created_at", "updated_at"]:
            if col in tabelaNegocios.columns:
                tabelaNegocios[col] = (
                    tabelaNegocios[col]
                    .dt.tz_convert("America/Sao_Paulo")
                    .dt.strftime("%Y-%m-%d %H:%M:%S")
                )

        if "expected_close_date" in tabelaNegocios.columns:
            tabelaNegocios["expected_close_date"] = tabelaNegocios[
                "expected_close_date"
            ].dt.strftime("%Y-%m-%d")

        for col in ["total_price", "one_time_price", "recurrence_price"]:
            if col in tabelaNegocios.columns:
                tabelaNegocios[col] = pd.to_numeric(
                    tabelaNegocios[col], errors="coerce"
                ).fillna(0.0)

        if "custom_fields_motivo_da_perda" in tabelaNegocios.columns:
            tabelaNegocios["motivo_da_perda"] = tabelaNegocios[
                "custom_fields_motivo_da_perda"
            ].apply(
                lambda x: (
                    self.extrair_motivo_limpo(x)
                    if isinstance(x, str) and "{" in x
                    else x
                )
            )
        elif "custom_fields" in tabelaNegocios.columns:
            tabelaNegocios["motivo_da_perda"] = tabelaNegocios["custom_fields"].apply(
                self.extrair_motivo_limpo
            )
        else:
            tabelaNegocios["motivo_da_perda"] = "Não Informado"

        tabelaNegocios["motivo_da_perda"] = (
            tabelaNegocios["motivo_da_perda"]
            .fillna("Não Informado")
            .replace(["", "None", "nan", "NaN", "None."], "Não Informado")
        )

        if "custom_fields_proposta_entregue_ao_cliente" in tabelaNegocios.columns:
            tabelaNegocios = tabelaNegocios.rename(
                columns={
                    "custom_fields_proposta_entregue_ao_cliente": "proposta_entregue_ao_cliente"
                }
            )
        if "custom_fields_data_de_entrega_da_proposta" in tabelaNegocios.columns:
            tabelaNegocios = tabelaNegocios.rename(
                columns={
                    "custom_fields_data_de_entrega_da_proposta": "data_de_entrega_da_proposta"
                }
            )
        if "data_de_entrega_da_proposta" in tabelaNegocios.columns:
            tabelaNegocios["data_de_entrega_da_proposta"] = pd.to_datetime(
                tabelaNegocios["data_de_entrega_da_proposta"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")

        colunas_limpeza_array = [
            col
            for col in tabelaNegocios.columns
            if col.startswith("custom_fields_") or col == "proposta_entregue_ao_cliente"
        ]
        for col in colunas_limpeza_array:
            tabelaNegocios[col] = tabelaNegocios[col].apply(self.limpar_array_rd)

        colunas_homologadas = [
            "id",
            "name",
            "recurrence_price",
            "one_time_price",
            "total_price",
            "expected_close_date",
            "closed_at",
            "rating",
            "status",
            "pipeline_id",
            "stage_id",
            "owner_id",
            "source_id",
            "organization_id",
            "lost_reason_id",
            "contact_ids",
            "motivo_da_perda",
            "proposta_entregue_ao_cliente",
            "data_de_entrega_da_proposta",
            "custom_fields_tipo_de_contrato",
            "custom_fields_descricao",
            "custom_fields_audio_e_video",
            "custom_fields_prazo_do_contrato",
            "custom_fields_margem",
            "custom_fields_tipo_de_orcamento",
            "custom_fields_disciplinas_envolvidas",
            "custom_fields_engenheiro_responsavel",
            "custom_fields_entrega_ao_gn",
            "created_at",
            "updated_at",
        ]

        colunas_presentes = [
            col for col in colunas_homologadas if col in tabelaNegocios.columns
        ]
        tabelaFiltrada = tabelaNegocios[colunas_presentes]

        tabelaPadronizada = self.padronizarDataframe(tabelaFiltrada)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)

        listaNegociosLimpos = self.normalizarInteiros(
            tabelaDeduplicada.to_dict(orient="records")
        )
        return listaNegociosLimpos

    # =========================================================================
    # DEMAIS MÉTODOS (SCHEMA SILVER GERAL)
    # =========================================================================

    def processarOrganizacoes(self, dadosBronze):
        tabelaOrganizacoes = pd.DataFrame(dadosBronze)
        if tabelaOrganizacoes.empty:
            return []
        tabelaOrganizacoes.dropna(subset=["id"], inplace=True)
        colunasUteis = [
            "id",
            "name",
            "owner_id",
            "custom_fields_cidade",
            "custom_fields_estado",
            "custom_fields_razao_social",
            "custom_fields_cnpj",
            "created_at",
            "updated_at",
        ]
        colunasPresentes = [
            col for col in colunasUteis if col in tabelaOrganizacoes.columns
        ]
        tabelaOrganizacoes = tabelaOrganizacoes[colunasPresentes]

        if "custom_fields_cnpj" in tabelaOrganizacoes.columns:
            tabelaOrganizacoes = tabelaOrganizacoes.rename(
                columns={"custom_fields_cnpj": "cnpj"}
            )

        tabelaPadronizada = self.padronizarDataframe(tabelaOrganizacoes)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)
        listaDicionarios = tabelaDeduplicada.to_dict(orient="records")
        return self.normalizarInteiros(listaDicionarios)

    def processarPipelines(self, dadosBronze):
        tabelaPipelines = pd.DataFrame(dadosBronze)
        if tabelaPipelines.empty:
            return []
        tabelaPipelines.dropna(subset=["id"], inplace=True)
        colunasUteis = ["id", "name", "created_at", "updated_at"]
        colunasPresentes = [
            col for col in colunasUteis if col in tabelaPipelines.columns
        ]
        tabelaPipelines = tabelaPipelines[colunasPresentes]
        tabelaPadronizada = self.padronizarDataframe(tabelaPipelines)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)
        listaDicionarios = tabelaDeduplicada.to_dict(orient="records")
        return self.normalizarInteiros(listaDicionarios)

    def processarStages(self, dadosBronze):
        tabelaStages = pd.DataFrame(dadosBronze)
        if tabelaStages.empty:
            return []
        tabelaStages.dropna(subset=["id"], inplace=True)
        if "order" in tabelaStages.columns:
            tabelaStages.rename(columns={"order": "stage_order"}, inplace=True)
        colunasUteis = [
            "id",
            "pipeline_id",
            "name",
            "stage_order",
            "created_at",
            "updated_at",
        ]
        colunasPresentes = [col for col in colunasUteis if col in tabelaStages.columns]
        tabelaStages = tabelaStages[colunasPresentes]
        tabelaPadronizada = self.padronizarDataframe(tabelaStages)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)
        listaDicionarios = tabelaDeduplicada.to_dict(orient="records")
        return self.normalizarInteiros(listaDicionarios)

    def processarTasks(self, dadosBronze):
        tabelaTasks = pd.DataFrame(dadosBronze)
        if tabelaTasks.empty:
            return []
        tabelaTasks.dropna(subset=["id"], inplace=True)
        if "owner_ids" in tabelaTasks.columns:
            tabelaTasks["owner_ids"] = (
                tabelaTasks["owner_ids"]
                .astype(str)
                .str.replace('"', "")
                .replace("nan", np.nan)
                .replace("None", np.nan)
            )
        colunasUteis = [
            "id",
            "name",
            "type",
            "status",
            "deal_id",
            "owner_ids",
            "due_date",
            "completed_at",
            "completed_by_id",
            "created_by_id",
            "description",
            "created_at",
            "updated_at",
        ]
        colunasPresentes = [col for col in colunasUteis if col in tabelaTasks.columns]
        tabelaTasks = tabelaTasks[colunasPresentes]
        tabelaPadronizada = self.padronizarDataframe(tabelaTasks)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)
        listaDicionarios = tabelaDeduplicada.to_dict(orient="records")
        return self.normalizarInteiros(listaDicionarios)

    def processarTeams(self, dadosBronze):
        tabelaTeams = pd.DataFrame(dadosBronze)
        if tabelaTeams.empty:
            return []
        tabelaTeams.dropna(subset=["id"], inplace=True)
        colunasUteis = ["id", "name", "created_at", "updated_at"]
        colunasPresentes = [col for col in colunasUteis if col in tabelaTeams.columns]
        tabelaTeams = tabelaTeams[colunasPresentes]
        tabelaPadronizada = self.padronizarDataframe(tabelaTeams)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)
        listaDicionarios = tabelaDeduplicada.to_dict(orient="records")
        return self.normalizarInteiros(listaDicionarios)

    def processarUsers(self, dadosBronze):
        tabelaUsers = pd.DataFrame(dadosBronze)
        if tabelaUsers.empty:
            return []
        tabelaUsers.dropna(subset=["id"], inplace=True)
        colunasUteis = ["id", "name", "email", "created_at", "updated_at"]
        colunasPresentes = [col for col in colunasUteis if col in tabelaUsers.columns]
        tabelaUsers = tabelaUsers[colunasPresentes]
        tabelaPadronizada = self.padronizarDataframe(tabelaUsers)
        tabelaDeduplicada = self.deduplicarInteligente(tabelaPadronizada)
        listaDicionarios = tabelaDeduplicada.to_dict(orient="records")
        return self.normalizarInteiros(listaDicionarios)
