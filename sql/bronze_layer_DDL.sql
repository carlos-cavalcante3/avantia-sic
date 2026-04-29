CREATE SCHEMA IF NOT EXISTS bronze;

GRANT USAGE ON SCHEMA bronze TO anon, authenticated, service_role;

DROP TABLE IF EXISTS bronze.deals CASCADE;
DROP TABLE IF EXISTS bronze.contacts CASCADE;
DROP TABLE IF EXISTS bronze.organizations CASCADE;
DROP TABLE IF EXISTS bronze.pipelines CASCADE;
DROP TABLE IF EXISTS bronze.stages CASCADE;
DROP TABLE IF EXISTS bronze.tasks CASCADE;
DROP TABLE IF EXISTS bronze.teams CASCADE;
DROP TABLE IF EXISTS bronze.users CASCADE;

CREATE TABLE bronze.users (
    id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    phone TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE bronze.pipelines (
    id TEXT PRIMARY KEY,
    name TEXT,
    "order" INTEGER,
    stage_ids JSONB,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE bronze.stages (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT,
    name TEXT,
    "order" INTEGER,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE bronze.teams (
    id TEXT PRIMARY KEY,
    name TEXT,
    user_ids JSONB,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE bronze.organizations (
    id TEXT PRIMARY KEY,
    name TEXT,
    owner_id TEXT,
    url TEXT,
    description TEXT,
    segment_ids JSONB,
    follower_ids JSONB,
    custom_fields_cidade TEXT,
    custom_fields_cnpj TEXT,
    custom_fields_e_mail TEXT,
    custom_fields_endereco TEXT,
    custom_fields_estado TEXT,
    custom_fields_inscricao_estadual TEXT,
    custom_fields_nome_fantasia TEXT,
    custom_fields_origem TEXT,
    custom_fields_razao_social TEXT,
    custom_fields_telefone TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE bronze.contacts (
    id TEXT PRIMARY KEY,
    name TEXT,
    job_title TEXT,
    emails JSONB,
    phones JSONB,
    organization_id TEXT,
    context_origin TEXT,
    social_profiles JSONB,
    legal_bases JSONB,
    custom_fields_celular TEXT,
    custom_fields_sobrenome TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE bronze.deals (
    id TEXT PRIMARY KEY,
    name TEXT,
    status TEXT,
    total_price NUMERIC,
    recurrence_price NUMERIC,
    one_time_price NUMERIC,
    expected_close_date TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    pipeline_id TEXT,
    stage_id TEXT,
    owner_id TEXT,
    organization_id TEXT,
    lost_reason_id TEXT,
    source_id TEXT,
    campaign_id TEXT,
    contact_ids JSONB,
    rating INTEGER,
    custom_fields TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE bronze.tasks (
    id TEXT PRIMARY KEY,
    name TEXT,
    type TEXT,
    status TEXT,
    deal_id TEXT,
    owner_ids JSONB,
    due_date TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    completed_by_id TEXT,
    created_by_id TEXT,
    description TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO anon, authenticated, service_role;