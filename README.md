# Pipeline ETL: Avantia-sic (RD Station CRM → Supabase)

## Overview
**Propósito:** Automatizar a ingestão contínua, resiliente e padronizada de todos os dados da camada comercial do RD Station CRM para a camada Bronze do Data Warehouse no Supabase. O objetivo é criar a base de verdade estruturada para suportar a futura modelagem Silver/Gold e a construção de Dashboards de Analytics e BI para o time Comercial.

**Owner:** Carlos Cavalcante / Time Comercial  
**Contato:** carlos.cavalcante@avantia.com.br  
**Última atualização:** 20-04-2026  

## Data Flow
RD Station CRM V2 (API REST) → Python (Extract, Transform, Load) → Supabase (PostgreSQL - Schema `bronze`)  → Transformações (Schema `silver`)

## Project Structure
```
avantia-sic/
├── data/
│   └── raw/
├── logs/
├── sql/
│   ├── bronze_layer_DDL.sql
│   └── silver_layer_DDL.sql
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
│   ├── __pycache__/
│   ├── conftest.py
│   ├── test_env.py
│   ├── test_extract.py
│   ├── test_load.py
│   ├── test_pipeline.py
│   └── test_transform.py
├── .env
├── .gitignore
├── pytest.ini
├── README.md
├── requirements.txt
└── run.py
```
## Sources
| Source | Tipo | Refresh | Notas |
|--------|------|---------|-------|
| `deals` | API REST | Batch | Oportunidades comerciais e funil |
| `contacts` | API REST | Batch | Contatos e leads capturados |
| `organizations` | API REST | Batch | Empresas e contas |
| `users`, `pipelines`, `tasks`, `teams` | API REST | Batch | Metadados e estruturação do CRM |

*Nota: Todas as conexões suportam paginação dinâmica (`links.next`), correção automática de rotas da API, fallback contra loops infinitos e renovação automática de tokens OAuth2 via `.env`.*

## Transformations
1. Backup Raw (Extracao): Download seguro e paginado de todos os registros da API. Salva cópia local imutável em `data/raw/nome_recurso_dd-mm-yyyy_hhmmss.csv`.
2. Flatten & Snake_Case: Achatamento automático de dicionários aninhados (ex: `custom_fields`) gerando colunas derivativas independentes. Renomeia todas as colunas para o padrão `snake_case` (substituindo `-` por `_`).
3. Simplificação de Arrays: Listas simples (ex: `['Não', 'Sim']`) são convertidas em strings formatadas separadas por vírgula (`"Não, Sim"`) para facilitar queries analíticas futuras.
4. Timezones & Clean Floats: Conversão inteligente de datas BR para ISO 8601 (`YYYY-MM-DDTHH:MM:SSZ`) em formato UTC. Bloqueio absoluto de falhas de tipagem do Pandas, convertendo `NaN`, `NaT` e `inf` para `None` (JSON compliant).
5. Alinhamento de Schema Dinâmico: Introspecção do schema físico via OpenAPI do Supabase em tempo real. Colunas dinâmicas que chegam da API, mas não existem na tabela SQL, são removidas em memória antes do envio.
6. Deduplicação em Memória: Varredura por chave primária (`id`). Em caso de duplicidade na mesma extração, o registro é atualizado baseado no critério de `updated_at` (mais recente) ou densidade de dados (menos valores nulos).
7. Upsert Resiliente: Envio particionado (Lotes de 500) usando instrução `upsert` com `on_conflict="id"`, garantindo total idempotência da carga.

## Schedule
- **Frequência:** Batch Diário / Custom (via Cron / Orquestrador)
- **Dependências:** Disponibilidade da API do RD Station CRM V2
- **Downstream:** Transformação Silver (Views/Tables SQL no Supabase), Atualização de Dashboards

## Data Quality
- **Prevenção de Duplicidade:** Aplicação rigorosa de Unicidade por PK (`id`) antes do envio e garantida via `on_conflict` no banco.
- **Auditoria e Logs:** Logs completos salvos localmente (`logs/pipeline_YYYY-MM-DD_HHMMSS.log`) e exibidos via terminal (`StreamHandler`).
- **Testes Automatizados:** Cobertura de caixa preta operando via `pytest` validando (.env, Conexão com Banco, Extração, Transformação e Integridade da Pipeline).

## Runbook
### Falhas comuns e mitigação
1. **Rate Limit da API (429):** Tratado automaticamente. A pipeline lê o `Retry-After`, pausa o processamento (`time.sleep`) e retoma sozinha.
2. **Access Token Expirado (401):** Tratado automaticamente. O sistema renova o token via `refresh_token`, reescreve o arquivo `.env` sem interrupção humana e continua o fluxo.
3. **Token de Renovação Inválido:** Emite alerta CRÍTICO nos logs (`REFRESH_TOKEN INVALIDO`). Ação necessária: Gerar novo token manual no RD Station e substituir no `.env`.
4. **Schema Drift (Novos campos no CRM):** Tratado parcialmente. A pipeline não quebra se um campo novo aparecer no CRM; o campo é filtrado no `load.py`. Para ingestão na Bronze, a coluna deve ser explicitamente criada no banco via instrução `ALTER TABLE`.
5. **Loops Infinitos de Paginação:** Tratado automaticamente. Um rastreador baseado em *Set* protege o loop e quebra a requisição se a API repetir a última página.
