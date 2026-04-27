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
    custom_fields_audio_e_video TEXT,
    custom_fields_data_da_criacao TIMESTAMPTZ,
    custom_fields_data_de_fechamento TIMESTAMPTZ,
    custom_fields_data_de_origem_da_proposta TIMESTAMPTZ,
    custom_fields_descricao TEXT,
    custom_fields_margem TEXT,
    custom_fields_motivo_da_perda TEXT,
    custom_fields_prazo_do_contrato TEXT,
    custom_fields_proposta_entregue_ao_cliente TEXT,
    custom_fields_registro_de_oportunidade TEXT,
    custom_fields_tipo_de_contrato TEXT,
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

CREATE INDEX idx_deals_updated_at ON bronze.deals(updated_at);
CREATE INDEX idx_deals_organization_id ON bronze.deals(organization_id);
CREATE INDEX idx_deals_pipeline_id ON bronze.deals(pipeline_id);
CREATE INDEX idx_ontacts_updated_at ON bronze.contacts(updated_at);
CREATE INDEX idx_contacts_organization_id ON bronze.contacts(organization_id);
CREATE INDEX idx_organizations_updated_at ON bronze.organizations(updated_at);
CREATE INDEX idx_tasks_updated_at ON bronze.tasks(updated_at);
CREATE INDEX idx_tasks_deal_id ON bronze.tasks(deal_id);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO anon, authenticated, service_role;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze TO anon, authenticated, service_role;
GRANT ALL PRIVILEGES ON ALL ROUTINES IN SCHEMA bronze TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON SEQUENCES TO anon, authenticated, service_role;