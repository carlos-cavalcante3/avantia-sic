CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.deals CASCADE;
DROP TABLE IF EXISTS silver.deals_historico CASCADE;
DROP TABLE IF EXISTS silver.contacts CASCADE;
DROP TABLE IF EXISTS silver.organizations CASCADE;
DROP TABLE IF EXISTS silver.pipelines CASCADE;
DROP TABLE IF EXISTS silver.stages CASCADE;
DROP TABLE IF EXISTS silver.tasks CASCADE;
DROP TABLE IF EXISTS silver.teams CASCADE;
DROP TABLE IF EXISTS silver.users CASCADE;

CREATE TABLE silver.contacts (
    id TEXT PRIMARY KEY,
    name TEXT,
    job_title TEXT,
    email_principal TEXT,
    telefone_principal TEXT,
    organization_id TEXT,
    context_origin TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE silver.organizations (
    id TEXT PRIMARY KEY,
    name TEXT,
    owner_id TEXT,
    custom_fields_cidade TEXT,
    custom_fields_estado TEXT,
    custom_fields_razao_social TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE silver.deals (
    id TEXT PRIMARY KEY,
    name TEXT,
    status TEXT,
    total_price NUMERIC,
    one_time_price NUMERIC,
    recurrence_price NUMERIC,
    expected_close_date TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    pipeline_id TEXT,
    stage_id TEXT,
    owner_id TEXT,
    organization_id TEXT,
    lost_reason_id TEXT,
    rating INTEGER,
    custom_fields_tipo_de_contrato TEXT,
    motivo_da_perda TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE silver.deals_historico (
    id SERIAL PRIMARY KEY,
    deal_id TEXT,
    old_stage_id TEXT,
    new_stage_id TEXT,
    changed_at TIMESTAMPTZ
);

CREATE TABLE silver.pipelines (
    id TEXT PRIMARY KEY,
    name TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE silver.stages (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT,
    name TEXT,
    stage_order INTEGER,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE silver.tasks (
    id TEXT PRIMARY KEY,
    name TEXT,
    type TEXT,
    status TEXT,
    deal_id TEXT,
    owner_ids TEXT,
    due_date TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    completed_by_id TEXT,
    created_by_id TEXT,
    description TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE silver.teams (
    id TEXT PRIMARY KEY,
    name TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE silver.users (
    id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO anon, authenticated, service_role;