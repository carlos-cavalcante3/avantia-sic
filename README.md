# AVANTIA-SIC  
### ETL RD Station CRM → Supabase | Medallion Architecture (Bronze + Silver)

## Overview

**Propósito:** Automatizar a ingestão contínua, resiliente e padronizada dos dados do RD Station CRM para um Data Warehouse no Supabase, seguindo arquitetura Medallion (`bronze` → `silver` → `gold`).

A camada **Bronze** preserva os dados originais como fonte oficial de verdade (*source of truth*), enquanto a camada **Silver** entrega dados limpos, padronizados e prontos para consumo analítico em BI, dashboards e futuras modelagens Gold.

O projeto foi construído com foco em idempotência, rastreabilidade, autonomia operacional e confiabilidade para ambientes produtivos.

**Owner:** Carlos Cavalcante / Time Comercial  
**Contato:** carlos.cavalcante@avantia.com.br  
**Última atualização:** 11-05-2026

## Data Flow

RD Station CRM V2 (API REST)  
→ Python ETL (Extract, Transform, Load)  
→ Supabase PostgreSQL (`schema bronze`)  
→ Processamento Analítico (`schema silver`)  
→ Camada Gold → Materialized Views
→ Frontend próprio no Lovable

---

## Project Structure

```

avantia-sic/
├── data/
│   └── raw/
├── logs/
├── sql/
│   ├── bronze_layer_DDL.sql
│   └── silver_layer_DDL.sql
│   └── gold_layer_DDL.sql
├── src/
│   ├── __init__.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── pipeline.py
│   ├── pipeline_silver.py
│   └── silver/
│       ├── __init__.py
│       ├── load_silver.py
│       ├── transform_silver.py
│       └── validation.py
├── tests/
│   ├── conftest.py
│   ├── test_env.py
│   ├── test_extract.py
│   ├── test_load.py
│   ├── test_pipeline.py
│   └── test_transform.py
├── .github/
│   └── workflows/
│       └── etl.yml
├── .env
├── .gitignore
├── pytest.ini
├── README.md
├── requirements.txt
└── run.py

```

## Sources

| Source | Tipo | Refresh | Notas |
|---|---|---|---|
| `deals` | API REST | Batch | Negócios e oportunidades comerciais |
| `contacts` | API REST | Batch | Leads e contatos cadastrados |
| `organizations` | API REST | Batch | Empresas e contas |
| `users` | API REST | Batch | Usuários responsáveis |
| `pipelines` | API REST | Batch | Estrutura de funis |
| `tasks` | API REST | Batch | Atividades e tarefas |
| `teams` | API REST | Batch | Estrutura de equipes |

**Observações importantes:**
- Paginação dinâmica via `links.next`
- Controle contra loops infinitos de paginação
- Renovação automática de tokens OAuth2
- Retry automático para falhas temporárias e rate limit
- Backup local imutável de todas as extrações

## Main Features

- Carga totalmente idempotente com `upsert` por chave primária (`id`)
- Renovação automática de credenciais sem intervenção manual
- Deduplicação inteligente por densidade de informação
- Alinhamento dinâmico com schema físico do banco
- Validação volumétrica entre Bronze e Silver
- Backup raw (local) para auditoria e rastreabilidade
- Execução automatizada diária com GitHub Actions
- Logs completos para monitoramento operacional
- Atualização automática das Views e Analytics do Lovable 

## Transformations

### Bronze Layer

- Ingestão raw preservando a estrutura original da API
- Backup CSV local por execução
- Conversão mínima para compatibilidade com carga SQL
- Remoção segura de inconsistências de tipagem (`NaN`, `NaT`, `inf`)
- Deduplicação em memória antes da carga
- Upsert transacional em lotes

### Silver Layer

- Flatten de estruturas JSON aninhadas
- Padronização de nomenclatura em `snake_case`
- Extração de campos analíticos derivados
- Normalização de datas para padrão analítico (`TIMESTAMPTZ`)
- Padronização de nomes próprios
- Limpeza estrutural de listas e objetos serializados
- Deduplicação por qualidade de preenchimento
- Tipagem consistente para consumo em BI

## Schedule

- **Frequência:** Batch diário
- **Execução automática:** GitHub Actions via Cron
- **Horário:** 09:00 UTC (06:00 BRT)
- **Dependência principal:** Disponibilidade da API RD Station CRM V2
- **Destino final:** Dashboards analíticos e consumo pela camada Gold

A pipeline garante que os dados estejam atualizados antes do início do expediente operacional.

## Data Quality

### Controles aplicados

- Prevenção de duplicidade por PK (`id`)
- Auditoria de volumetria entre schemas
- Logs persistidos por execução
- Controle de falhas críticas e alertas operacionais
- Garantia de integridade antes da carga final

### Testes automatizados

A suíte `pytest` valida:

- Variáveis de ambiente
- Conectividade com Supabase
- Extração da API
- Regras de transformação
- Integridade da pipeline
- Fluxo de carga

## Runbook

### Falhas comuns e mitigação

### 1. Rate Limit da API (429)

Tratamento automático.

A pipeline lê o `Retry-After`, pausa a execução e retoma o processamento sem intervenção manual.

### 2. Access Token expirado (401)

Tratamento automático.

O sistema utiliza o `refresh_token`, gera novas credenciais, atualiza o `.env` e continua a execução.

### 3. Refresh Token inválido

Tratamento manual necessário.

O sistema registra alerta crítico em log.

**Ação:** gerar novo token no RD Station e atualizar o `.env`.

### 4. Novos campos no CRM (Schema Drift)

Tratamento parcial.

A pipeline não quebra, mas novas colunas são ignoradas até criação explícita no banco.

**Ação:** executar `ALTER TABLE` no schema correspondente.

### 5. Divergência volumétrica Bronze vs Silver

Tratamento por validação.

Caso a perda ultrapasse o limite aceitável, o processo registra alerta para investigação.

**Ação:** revisar transformação da entidade afetada.

## Execution

### Instalação

```

pip install -r requirements.txt

```

### Execução manual
```

python run.py

```

### Execução de testes

```

pytest

```


## Próximos Passos
- Finalizar atualizações dos dashboards de acordo com a necessidade do Time
- Procurar por falhas do sistema 
