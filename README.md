# Avantia SIC - Data Pipeline (RD Station CRM)
### ETL RD Station CRM → Supabase | Medallion Architecture

![Python](https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge&logo=python)
![Supabase](https://img.shields.io/badge/Supabase-Database-4ECB71?style=for-the-badge&logo=supabase)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Relational-316192?style=for-the-badge&logo=postgresql)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-Automation-2088FF?style=for-the-badge&logo=github-actions)

> Repositório responsável pela extração, transformação e carga (ETL) de dados comerciais do **RD Station CRM** para o banco de dados analítico no **Supabase (PostgreSQL)**, alimentando os dashboards de inteligência comercial da Avantia.

---

## 📊 Dashboards de Inteligência Comercial

<img width="1878" height="973" alt="Image" src="https://github.com/user-attachments/assets/0f0e03f7-8c60-4d87-b959-7216aa27b5a0" />

---

## 🏗️ Arquitetura de Dados (Medallion Architecture)

O projeto implementa a **Arquitetura Medalhão** para garantir qualidade, resiliência e histórico dos dados:

1. **🥉 Camada Bronze (`sql/bronze_layer_DDL.sql`):** 
   - Recebe os dados brutos (Raw) diretamente das requisições paginadas da API do RD Station CRM.
   - Formato original preservado (JSON/Dicionários convertidos para colunas base).
2. **🥈 Camada Silver (`sql/silver_layer_DDL.sql`):** 
   - Dados limpos, tipados e padronizados.
   - Tratamento de nulos, conversão de datas (timezone) e achatamento (flatten) de estruturas aninhadas.
   - Aplicação de chaves primárias e relacionamentos.
3. **🥇 Camada Gold (`sql/gold_layer_DDL.sql`):** 
   - Modelagem dimensional e `MATERIALIZED VIEWS`.
   - Tabelas agregadas e KPIs de negócios (taxa de conversão, funil de vendas, ticket médio) otimizadas para leitura rápida pelo dashboard.

---

## ⚙️ Fluxo de Autenticação e Rotação de Tokens (OAuth2)

Um dos maiores desafios técnicos do projeto é a **manutenção da sessão da API do RD Station**.
- O sistema utiliza **Refresh Token Rotation**.
- A tabela `silver.api_auth` armazena o `access_token` e `refresh_token` atuais.
- **Antes de qualquer extração:** O pipeline (`src/extract.py`) lê o token do Supabase, aciona o endpoint de autenticação do RD Station, obtém novas chaves, e faz um **UPSERT no Supabase usando a `service_role` key** para garantir a persistência antes de continuar. Isso evita que o pipeline "quebre" em execuções automatizadas.

---

## 📂 Estrutura do Repositório

```text
avantia-sic/
├── .github/workflows/
│   └── etl.yml              # Orquestração da pipeline via GitHub Actions
├── sql/
│   ├── bronze_layer_DDL.sql # DDLs da camada bruta
│   ├── silver_layer_DDL.sql # DDLs da camada limpa + Tabela api_auth
│   └── gold_layer_DDL.sql   # Materialized Views e agregações
├── src/
│   ├── extract.py           # Conexão com API RD CRM e extração em paginação
│   ├── load.py              # Operações de Upsert/Insert no Supabase
│   ├── transform.py         # Regras de negócio e limpeza usando Pandas
│   ├── pipeline.py          # Orquestrador principal do ETL
│   └── silver/              # Processamento específico da camada Silver
├── tests/                   # Suíte de testes unitários (pytest)
├── requirements.txt         # Dependências do projeto (pandas, supabase, requests)
├── pytest.ini               # Configurações do Pytest
└── README.md                # Documentação atual
```

---

## 🚀 Como Executar Localmente

### 1. Pré-requisitos
- Python 3.10+
- Conta no Supabase configurada com as DDLs da pasta `sql/` executadas.
- Credenciais de API do RD Station CRM V2.

### 2. Instalação

```bash
# Clone o repositório
git clone https://github.com/carlos-cavalcante3/avantia-sic.git
cd avantia-sic

# Crie e ative um ambiente virtual
python -m venv venv
source venv/bin/activate  # No Windows: venv\Scripts ctivate

# Instale as dependências
pip install -r requirements.txt
```

### 3. Variáveis de Ambiente (`.env`)
Crie um arquivo `.env` na raiz do projeto com as seguintes credenciais:

```env
# Banco de Dados (Supabase)
SUPABASE_URL=sua_url_do_supabase
SUPABASE_KEY=sua_service_role_key_aqui  # IMPORTANTE: Use a service_role para gravar tokens!

# RD Station CRM
RD_CLIENT_ID=seu_client_id
RD_CLIENT_SECRET=seu_client_secret
```

### 4. Executando o Pipeline Completo

Para iniciar a carga Batch manualmente:
```bash
python run.py
```
*(Certifique-se de que a tabela `silver.api_auth` possui um `refresh_token` válido antes da primeira execução).*

---

## 🛠️ Orquestração (GitHub Actions)

A pipeline é executada automaticamente através do GitHub Actions (`etl.yml`), configurada para:
- Rodar por agendamento (CRON) para atualizações recorrentes.
- Bloquear execuções simultâneas (`concurrency`) para evitar colisão na rotação de tokens OAuth2.

## 🔮 Roadmap e Próximos Passos
- [ ] **Migração para Streaming / Near Real-Time:** Substituir rotinas de *Pull* (paginação batch) por *Push* (Webhooks do RD Station processados via Supabase Edge Functions).
- [ ] Conversão de `Materialized Views` para `Views` regulares para suportar atualização e leitura de dados em tempo real na camada analítica.
