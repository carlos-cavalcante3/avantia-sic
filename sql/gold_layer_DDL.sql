CREATE SCHEMA IF NOT EXISTS gold;
GRANT USAGE ON SCHEMA gold TO anon, authenticated, service_role;

CREATE MATERIALIZED VIEW gold.mv_pipeline_funil AS
SELECT 
    p.name AS pipeline_nome,
    s.name AS etapa_nome,
    s.stage_order AS ordem,
    COUNT(d.id) AS total_negocios,
    COALESCE(SUM(d.total_price), 0) AS valor_total
FROM silver.stages s
LEFT JOIN silver.pipelines p ON s.pipeline_id = p.id
LEFT JOIN silver.deals d ON s.id = d.stage_id AND d.status NOT IN ('won', 'lost')
GROUP BY p.name, s.name, s.stage_order
ORDER BY p.name, s.stage_order;

CREATE MATERIALIZED VIEW gold.mv_kpis_gerais AS
SELECT 
    COUNT(id) AS total_negocios_ganhos, 
    COALESCE(SUM(total_price), 0) AS valor_total_ganho, 
    COALESCE(AVG(total_price), 0) AS ticket_medio
FROM silver.deals 
WHERE status = 'won' AND EXTRACT(YEAR FROM closed_at) = EXTRACT(YEAR FROM CURRENT_DATE);

CREATE MATERIALIZED VIEW gold.mv_negocios_estagnados AS
SELECT 
    d.id, d.name AS negocio_nome, o.name AS empresa_nome, d.total_price AS valor, 
    EXTRACT(DAY FROM CURRENT_DATE - COALESCE(d.updated_at, d.created_at)) AS dias_sem_interacao
FROM silver.deals d
LEFT JOIN silver.organizations o ON d.organization_id = o.id
WHERE d.status NOT IN ('won', 'lost')
ORDER BY dias_sem_interacao DESC;

CREATE MATERIALIZED VIEW gold.mv_performance_gestor AS
SELECT 
    u.name AS gestor_nome,
    COUNT(d.id) AS total_negocios,
    COUNT(d.id) FILTER (WHERE d.status = 'won') AS negocios_ganhos,
    COALESCE(SUM(d.total_price) FILTER (WHERE d.status = 'won'), 0) AS valor_total_ganho,
    ROUND((COUNT(d.id) FILTER (WHERE d.status = 'won')::NUMERIC / NULLIF(COUNT(d.id) FILTER (WHERE d.status IN ('won', 'lost')), 0)) * 100, 2) AS win_rate
FROM silver.deals d
LEFT JOIN silver.users u ON d.owner_id = u.id
GROUP BY u.name;

CREATE MATERIALIZED VIEW gold.mv_motivos_perda AS
SELECT 
    COALESCE(NULLIF(TRIM(motivo_da_perda), ''), 'Não Informado') AS motivo,
    COUNT(id) AS qtd_negocios,
    COALESCE(SUM(total_price), 0) AS valor_perdido
FROM silver.deals
WHERE status = 'lost'
GROUP BY motivo
ORDER BY qtd_negocios DESC;

CREATE MATERIALIZED VIEW gold.mv_vendas_mensais_yoy AS
SELECT EXTRACT(YEAR FROM closed_at) AS ano, EXTRACT(MONTH FROM closed_at) AS mes, COALESCE(SUM(total_price), 0) AS receita_total
FROM silver.deals WHERE status = 'won' AND closed_at IS NOT NULL
GROUP BY ano, mes ORDER BY ano, mes;

CREATE MATERIALIZED VIEW gold.mv_composicao_receita AS
SELECT EXTRACT(YEAR FROM closed_at) AS ano, SUM(one_time_price) AS receita_unica, SUM(recurrence_price) AS receita_recorrente
FROM silver.deals WHERE status = 'won' GROUP BY ano;

CREATE MATERIALIZED VIEW gold.mv_ciclo_vendas AS
SELECT u.name AS gestor, ROUND(AVG(EXTRACT(EPOCH FROM (d.closed_at - d.created_at))/86400)::NUMERIC, 1) AS dias_medios_fechamento
FROM silver.deals d LEFT JOIN silver.users u ON d.owner_id = u.id
WHERE d.status = 'won' GROUP BY u.name;

GRANT ALL ON ALL TABLES IN SCHEMA gold TO anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.atualizar_camada_gold() RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW gold.mv_pipeline_funil;
    REFRESH MATERIALIZED VIEW gold.mv_kpis_gerais;
    REFRESH MATERIALIZED VIEW gold.mv_negocios_estagnados;
    REFRESH MATERIALIZED VIEW gold.mv_performance_gestor;
    REFRESH MATERIALIZED VIEW gold.mv_motivos_perda;
    REFRESH MATERIALIZED VIEW gold.mv_vendas_mensais_yoy;
    REFRESH MATERIALIZED VIEW gold.mv_composicao_receita;
    REFRESH MATERIALIZED VIEW gold.mv_ciclo_vendas;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;