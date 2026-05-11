-- ====================================================================================
-- CAMADA GOLD
-- ====================================================================================
CREATE SCHEMA IF NOT EXISTS gold;
GRANT USAGE ON SCHEMA gold TO anon, authenticated, service_role;

-- ====================================================================================
-- 1. DROP CASCADE
-- Sempre apaga as views antigas antes de recriar para evitar conflito de tipagem
-- ====================================================================================
DROP MATERIALIZED VIEW IF EXISTS gold.mv_kpis_gerais CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.mv_pipeline_ponderado CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.mv_vendas_mensais_yoy CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.mv_top_clientes_periodo CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.mv_vendas_gestor_periodo CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.mv_pipeline_funil CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.mv_segmentacao_produto CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.mv_performance_gestor CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.mv_carteira_clientes CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.mv_negocios_estagnados CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.mv_motivos_perda CASCADE;
DROP MATERIALIZED VIEW IF EXISTS gold.rd_deals_historico CASCADE;

-- ====================================================================================
-- PÁGINA 1: VISÃO GERAL DE VENDAS & METAS
-- ====================================================================================

-- Gráficos: Faixas YTD, MTD, Win Rate, e Gauges de Meta Anual/Mensal
CREATE MATERIALIZED VIEW gold.mv_kpis_gerais AS
SELECT 
    COALESCE(p.name, 'Sem Pipeline') AS pipeline_nome,
    -- Métricas YTD (Ano Atual)
    COUNT(d.id) FILTER (WHERE d.status = 'won' AND EXTRACT(YEAR FROM d.closed_at) = EXTRACT(YEAR FROM CURRENT_DATE)) AS qtd_ganhos_ytd,
    COALESCE(SUM(d.total_price) FILTER (WHERE d.status = 'won' AND EXTRACT(YEAR FROM d.closed_at) = EXTRACT(YEAR FROM CURRENT_DATE)), 0) AS valor_ganho_ytd,
    COALESCE(AVG(d.total_price) FILTER (WHERE d.status = 'won' AND EXTRACT(YEAR FROM d.closed_at) = EXTRACT(YEAR FROM CURRENT_DATE)), 0) AS ticket_medio_ytd,
    -- Métricas MTD (Mês Atual)
    COUNT(d.id) FILTER (WHERE d.status = 'won' AND EXTRACT(YEAR FROM d.closed_at) = EXTRACT(YEAR FROM CURRENT_DATE) AND EXTRACT(MONTH FROM d.closed_at) = EXTRACT(MONTH FROM CURRENT_DATE)) AS qtd_ganhos_mtd,
    COALESCE(SUM(d.total_price) FILTER (WHERE d.status = 'won' AND EXTRACT(YEAR FROM d.closed_at) = EXTRACT(YEAR FROM CURRENT_DATE) AND EXTRACT(MONTH FROM d.closed_at) = EXTRACT(MONTH FROM CURRENT_DATE)), 0) AS valor_ganho_mtd,
    COALESCE(AVG(d.total_price) FILTER (WHERE d.status = 'won' AND EXTRACT(YEAR FROM d.closed_at) = EXTRACT(YEAR FROM CURRENT_DATE) AND EXTRACT(MONTH FROM d.closed_at) = EXTRACT(MONTH FROM CURRENT_DATE)), 0) AS ticket_medio_mtd,
    -- Nova regra de Win Rate: Negócios Ganhos no Ano / Propostas do Ano
    COUNT(d.id) FILTER (WHERE EXTRACT(YEAR FROM d.created_at) = EXTRACT(YEAR FROM CURRENT_DATE)) AS total_propostas_ytd,
    ROUND((COUNT(d.id) FILTER (WHERE d.status = 'won' AND EXTRACT(YEAR FROM d.closed_at) = EXTRACT(YEAR FROM CURRENT_DATE))::NUMERIC / 
    NULLIF(COUNT(d.id) FILTER (WHERE EXTRACT(YEAR FROM d.created_at) = EXTRACT(YEAR FROM CURRENT_DATE)), 0)) * 100, 2) AS win_rate_ytd
FROM silver.deals d LEFT JOIN silver.pipelines p ON d.pipeline_id = p.id 
GROUP BY p.name;

-- Gráficos: Valor Total do Pipeline e Pipeline Ponderado (Com exclusão do On-Hold Privado)
CREATE MATERIALIZED VIEW gold.mv_pipeline_ponderado AS
SELECT 
    COALESCE(p.name, 'Sem Pipeline') AS pipeline_nome,
    COUNT(d.id) AS qtd_aberto,
    COALESCE(SUM(d.total_price), 0) AS valor_pipeline_bruto,
    COALESCE(SUM(d.total_price * CASE 
            WHEN s.name ILIKE '%Pré-Venda%' THEN 0.05
            WHEN s.name ILIKE '%Proposta%' THEN 0.10
            WHEN s.name ILIKE '%Análise%' THEN 0.20
            WHEN s.name ILIKE '%On-hold%' THEN 0.10
            WHEN s.name ILIKE '%Negociação%' THEN 0.33
            WHEN s.name ILIKE '%Pedido%' THEN 0.90
            ELSE 0 
        END
    ), 0) AS valor_pipeline_ponderado
FROM silver.deals d 
LEFT JOIN silver.pipelines p ON d.pipeline_id = p.id
LEFT JOIN silver.stages s ON d.stage_id = s.id
WHERE d.status NOT IN ('won', 'lost') 
  AND s.name NOT ILIKE '%Qualificação Técnica%'
  AND NOT (p.name ILIKE '%PRIVADO%' AND s.name ILIKE '%On-hold%') -- Regra de exclusão
GROUP BY p.name;

-- Gráficos: Top 15 Clientes (LineChart YTD e MTD)
CREATE MATERIALIZED VIEW gold.mv_top_clientes_periodo AS
SELECT 
    COALESCE(p.name, 'Sem Pipeline') AS pipeline_nome,
    o.name AS empresa_nome,
    COALESCE(SUM(d.total_price) FILTER (WHERE EXTRACT(YEAR FROM d.closed_at) = EXTRACT(YEAR FROM CURRENT_DATE)), 0) AS valor_ytd,
    COALESCE(SUM(d.total_price) FILTER (WHERE EXTRACT(YEAR FROM d.closed_at) = EXTRACT(YEAR FROM CURRENT_DATE) AND EXTRACT(MONTH FROM d.closed_at) = EXTRACT(MONTH FROM CURRENT_DATE)), 0) AS valor_mtd
FROM silver.deals d 
LEFT JOIN silver.pipelines p ON d.pipeline_id = p.id
LEFT JOIN silver.organizations o ON d.organization_id = o.id
WHERE d.status = 'won'
GROUP BY p.name, o.name;

-- Gráficos: Vendas por Gerente (LineChart YTD e MTD)
CREATE MATERIALIZED VIEW gold.mv_vendas_gestor_periodo AS
SELECT 
    COALESCE(p.name, 'Sem Pipeline') AS pipeline_nome,
    u.name AS gestor_nome,
    COALESCE(SUM(d.total_price) FILTER (WHERE EXTRACT(YEAR FROM d.closed_at) = EXTRACT(YEAR FROM CURRENT_DATE)), 0) AS valor_ytd,
    COALESCE(SUM(d.total_price) FILTER (WHERE EXTRACT(YEAR FROM d.closed_at) = EXTRACT(YEAR FROM CURRENT_DATE) AND EXTRACT(MONTH FROM d.closed_at) = EXTRACT(MONTH FROM CURRENT_DATE)), 0) AS valor_mtd
FROM silver.deals d 
LEFT JOIN silver.pipelines p ON d.pipeline_id = p.id
LEFT JOIN silver.users u ON d.owner_id = u.id
WHERE d.status = 'won'
GROUP BY p.name, u.name;

-- Gráfico: Evolução Mensal de Vendas YoY (Gráfico de barras detalhado)
CREATE MATERIALIZED VIEW gold.mv_vendas_mensais_yoy AS
SELECT 
    COALESCE(p.name, 'Sem Pipeline') AS pipeline_nome, 
    EXTRACT(YEAR FROM d.closed_at) AS ano, 
    EXTRACT(MONTH FROM d.closed_at) AS mes, 
    COALESCE(SUM(d.total_price), 0) AS receita_total,
    COALESCE(SUM(d.recurrence_price), 0) AS receita_recorrente,
    COALESCE(SUM(d.one_time_price), 0) AS receita_unica,
    COUNT(d.id) AS qtd_negocios
FROM silver.deals d LEFT JOIN silver.pipelines p ON d.pipeline_id = p.id 
WHERE d.status = 'won' AND d.closed_at IS NOT NULL 
GROUP BY p.name, ano, mes ORDER BY ano, mes;


-- ====================================================================================
-- PÁGINA 2: PIPELINE & SEGMENTAÇÃO
-- ====================================================================================

-- Gráficos: Os 4 Funis por Setor (Design Trapézio em "V")
CREATE MATERIALIZED VIEW gold.mv_pipeline_funil AS
SELECT 
    COALESCE(p.name, 'Sem Pipeline') AS pipeline_nome, 
    s.name AS etapa_nome, 
    s.stage_order AS ordem, 
    COUNT(d.id) AS total_negocios, 
    COALESCE(SUM(d.total_price), 0) AS valor_total
FROM silver.stages s 
LEFT JOIN silver.pipelines p ON s.pipeline_id = p.id 
LEFT JOIN silver.deals d ON s.id = d.stage_id AND d.status NOT IN ('won', 'lost')
WHERE s.name NOT ILIKE '%Qualificação Técnica%' 
GROUP BY p.name, s.name, s.stage_order ORDER BY p.name, s.stage_order;


-- ====================================================================================
-- PÁGINAS 3 E 4: GERENTES & ANÁLISE INDIVIDUAL
-- ====================================================================================

-- Gráficos: Barras Horizontal, Ranking de Oportunidades, Tempo Médio e Win Rate do Gestor
CREATE MATERIALIZED VIEW gold.mv_performance_gestor AS
SELECT 
    COALESCE(p.name, 'Sem Pipeline') AS pipeline_nome, 
    u.name AS gestor_nome,
    COUNT(d.id) FILTER (WHERE EXTRACT(YEAR FROM d.created_at) = EXTRACT(YEAR FROM CURRENT_DATE)) AS total_oportunidades_ytd, 
    COALESCE(SUM(d.total_price) FILTER (WHERE EXTRACT(YEAR FROM d.created_at) = EXTRACT(YEAR FROM CURRENT_DATE)), 0) AS valor_propostas_ytd, 
    COUNT(d.id) FILTER (WHERE d.status = 'won' AND EXTRACT(YEAR FROM d.closed_at) = EXTRACT(YEAR FROM CURRENT_DATE)) AS negocios_ganhos_ytd,
    COUNT(d.id) FILTER (WHERE d.status = 'lost' AND EXTRACT(YEAR FROM d.closed_at) = EXTRACT(YEAR FROM CURRENT_DATE)) AS negocios_perdidos_ytd,
    COALESCE(SUM(d.total_price) FILTER (WHERE d.status = 'won' AND EXTRACT(YEAR FROM d.closed_at) = EXTRACT(YEAR FROM CURRENT_DATE)), 0) AS valor_total_ganho_ytd,
    -- Ciclo de Vendas (Prazo Médio no Pipeline)
    ROUND(AVG(EXTRACT(EPOCH FROM (d.closed_at - d.created_at))/86400) FILTER (WHERE d.status = 'won')::NUMERIC, 1) AS dias_medios_fechamento
FROM silver.deals d 
LEFT JOIN silver.users u ON d.owner_id = u.id 
LEFT JOIN silver.pipelines p ON d.pipeline_id = p.id 
GROUP BY p.name, u.name;

-- Gráfico: Tabela de Carteira do Gerente (Página 4 - Detalhada)
CREATE MATERIALIZED VIEW gold.mv_carteira_clientes AS
SELECT 
    u.name AS gestor_nome,
    o.name AS cliente_nome,
    p.name AS pipeline_nome,
    COUNT(d.id) FILTER (WHERE EXTRACT(YEAR FROM d.created_at) = EXTRACT(YEAR FROM CURRENT_DATE)) AS oportunidades_2025,
    COUNT(d.id) FILTER (WHERE d.status NOT IN ('won', 'lost')) AS oportunidades_atuais,
    MAX(COALESCE(d.updated_at, d.created_at)) AS ultima_movimentacao,
    EXTRACT(DAY FROM CURRENT_DATE - MAX(COALESCE(d.updated_at, d.created_at))) AS dias_sem_movimentacao
FROM silver.deals d
LEFT JOIN silver.users u ON d.owner_id = u.id
LEFT JOIN silver.organizations o ON d.organization_id = o.id
LEFT JOIN silver.pipelines p ON d.pipeline_id = p.id
GROUP BY u.name, o.name, p.name;


-- ====================================================================================
-- PÁGINA 5: ALERTAS E COMPLIANCE
-- ====================================================================================

-- Gráfico: Tabela de Negócios Exigindo Atenção (> 15 dias sem interação)
CREATE MATERIALIZED VIEW gold.mv_negocios_estagnados AS
SELECT 
    COALESCE(p.name, 'Sem Pipeline') AS pipeline_nome, 
    d.id, d.name AS negocio_nome, o.name AS empresa_nome, d.total_price AS valor, 
    EXTRACT(DAY FROM CURRENT_DATE - COALESCE(d.updated_at, d.created_at)) AS dias_sem_interacao
FROM silver.deals d 
LEFT JOIN silver.organizations o ON d.organization_id = o.id 
LEFT JOIN silver.pipelines p ON d.pipeline_id = p.id 
WHERE d.status NOT IN ('won', 'lost') 
ORDER BY dias_sem_interacao DESC;

-- Gráfico: Motivos de Perda Normalizados Semanticamente
CREATE MATERIALIZED VIEW gold.mv_motivos_perda AS
WITH base AS (
    SELECT 
        COALESCE(p.name, 'Sem Pipeline') AS pipeline_nome,
        TRIM(LOWER(COALESCE(NULLIF(TRIM(d.motivo_da_perda), ''), 'Não Informado'))) AS motivo_raw,
        d.id, d.total_price
    FROM silver.deals d LEFT JOIN silver.pipelines p ON d.pipeline_id = p.id
    WHERE d.status = 'lost'
),
normalizado AS (
    SELECT pipeline_nome, id, total_price,
        CASE
            WHEN motivo_raw LIKE '%preço%' OR motivo_raw LIKE '%preco%' OR motivo_raw LIKE '%budget%' OR motivo_raw LIKE '%valor%' OR motivo_raw LIKE '%concorrencia%' OR motivo_raw LIKE '%menor investimento%' THEN 'Preço'
            WHEN motivo_raw LIKE '%duplic%' THEN 'Duplicidade'
            WHEN motivo_raw LIKE '%desist%' OR motivo_raw LIKE '%cancel%' OR motivo_raw LIKE '%encerrou%' OR motivo_raw LIKE '%encerrado%' THEN 'Cliente Desistiu'
            WHEN motivo_raw LIKE '%declin%' THEN 'Declínio'
            WHEN motivo_raw LIKE '%sem recurso%' OR motivo_raw LIKE '%financeir%' THEN 'Sem Recursos Financeiros'
            WHEN motivo_raw LIKE '%não foi escolhido%' OR motivo_raw LIKE '%nao foi escolhido%' OR motivo_raw LIKE '%perdeu%' OR motivo_raw LIKE '%concorrente%' THEN 'Perda para Concorrente'
            WHEN motivo_raw LIKE '%não conseguimos%' OR motivo_raw LIKE '%nao conseguimos%' OR motivo_raw LIKE '%não entregar%' OR motivo_raw LIKE '%nao entregar%' OR motivo_raw LIKE '%demora%' THEN 'Falha Interna'
            WHEN motivo_raw LIKE '%solução%' OR motivo_raw LIKE '%solucao%' THEN 'Solução'
            WHEN motivo_raw LIKE '%prazo%' THEN 'Prazo'
            WHEN motivo_raw LIKE '%relacionamento%' THEN 'Relacionamento'
            WHEN motivo_raw LIKE '%não perdemos%' OR motivo_raw LIKE '%nao perdemos%' THEN 'Não Perdemos'
            WHEN motivo_raw LIKE '%não informado%' OR motivo_raw LIKE '%nao informado%' THEN 'Não Informado'
            ELSE 'Outros'
        END AS motivo
    FROM base
)
SELECT pipeline_nome, motivo, COUNT(id) AS qtd_negocios, COALESCE(SUM(total_price), 0) AS valor_perdido
FROM normalizado GROUP BY pipeline_nome, motivo ORDER BY qtd_negocios DESC;


-- ====================================================================================
-- VIEWS DE HISTÓRICO & AUDITORIA
-- ====================================================================================

-- Gráfico: Histórico de Alterações de Etapas (Pode ser usado no perfil do gestor)
CREATE MATERIALIZED VIEW gold.rd_deals_historico AS
SELECT 
    h.id AS historico_id, h.deal_id AS negocio_id,
    COALESCE(d.name, 'Negócio Excluído') AS negocio_nome,
    COALESCE(p.name, 'Sem Pipeline') AS pipeline_nome,
    COALESCE(s_old.name, 'Criação / Sem Etapa') AS etapa_anterior_nome,
    COALESCE(s_new.name, 'Sem Etapa Nova') AS etapa_nova_nome,
    h.changed_at AS data_mudanca
FROM silver.deals_historico h
LEFT JOIN silver.deals d ON h.deal_id = d.id
LEFT JOIN silver.pipelines p ON d.pipeline_id = p.id
LEFT JOIN silver.stages s_old ON h.old_stage_id = s_old.id
LEFT JOIN silver.stages s_new ON h.new_stage_id = s_new.id
ORDER BY h.changed_at DESC;


-- ====================================================================================
-- PERMISSÕES E FUNÇÃO DE ATUALIZAÇÃO (REFRESH)
-- ====================================================================================
GRANT ALL ON ALL TABLES IN SCHEMA gold TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.atualizar_camada_gold() RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW gold.mv_kpis_gerais;
    REFRESH MATERIALIZED VIEW gold.mv_pipeline_ponderado;
    REFRESH MATERIALIZED VIEW gold.mv_vendas_mensais_yoy;
    REFRESH MATERIALIZED VIEW gold.mv_top_clientes_periodo;
    REFRESH MATERIALIZED VIEW gold.mv_vendas_gestor_periodo;
    REFRESH MATERIALIZED VIEW gold.mv_pipeline_funil;
    REFRESH MATERIALIZED VIEW gold.mv_segmentacao_produto;
    REFRESH MATERIALIZED VIEW gold.mv_performance_gestor;
    REFRESH MATERIALIZED VIEW gold.mv_carteira_clientes;
    REFRESH MATERIALIZED VIEW gold.mv_negocios_estagnados;
    REFRESH MATERIALIZED VIEW gold.mv_motivos_perda;
    REFRESH MATERIALIZED VIEW gold.rd_deals_historico;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Força a API do Supabase a enxergar as novas views instantaneamente
NOTIFY pgrst, 'reload schema';