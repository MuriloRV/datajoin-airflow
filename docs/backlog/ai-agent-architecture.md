# Arquitetura do agente de IA (text-to-SQL via metadata dbt)

**Status:** backlog (plano arquitetural — não começou)
**Origem:** discussão 2026-05-15 sobre onde encaixar agente de IA no stack atual (portal + airflow + DW) pra responder perguntas de cliente via SQL no warehouse.

## Princípio guia

A infra atual (datajoin-app + datajoin-airflow + datajoin-vm-hostinger-prod) já tem vizinho natural pra cada peça do agente. **Sem repo novo, sem service novo** — só módulos novos dentro do que existe e um DB extra na mesma instância Postgres.

## Decisão em uma tabela

| Componente | Onde | Por quê |
|---|---|---|
| Pipeline `.md` + embedding | `datajoin-airflow` (DAG nova + tasks) | Roda colado no `dbt build`. Lê `target/manifest.json` direto. Já é orquestração. |
| pgvector (knowledge base) | DB **novo `agent_kb`** no Postgres do `datajoin-app` | Mesma instância dos outros DBs do app, sem container novo. |
| Código do agente (tools, prompts, LLM calls) | `datajoin-app` como módulo `agent/` (mesma API) | Já tem auth, tenant config, acesso ao warehouse. Streaming via FastAPI. |
| Histórico de conversa | DB `platform_metadata` (Alembic do portal) | Conversa é metadata do portal: quem perguntou, qual tenant, quando. |
| UI de chat | `datajoin-app/portal/` (Next.js existente) | Mesma autenticação, mesma sessão. |

## Por que NÃO separar em repo/service novo agora

Tentação clássica: "AI parece outro beast, deixa num repo só dele". Custo real disso, na escala atual:

- Auth duplicada (ou OIDC entre serviços)
- Pipeline de deploy paralelo
- Cada `execute_query` vira HTTP cross-service (latência + erro a mais)
- LLM API key precisa estar em 2 ambientes
- Mudança em schema de tenant força contrato API ao invés de import direto

Regra: **só extrai serviço quando tiver forcing function** — diferente cadência de release, time separado, ou workload que não cabe junto (GPU worker, etc).

## Detalhe por componente

### 1. Postgres do `datajoin-app` ganha mais um DB

Estrutura final da instância:

```
postgres (mesma instancia, na VM Hostinger)
├── platform_metadata   (role: platform)         ← API + telemetria + chat history
├── warehouse           (role: dw_admin)         ← DW dos tenants
└── agent_kb            (role: agent_kb_admin)   ← NOVO: pgvector + .md content
```

**Provisionar** via append no `docker/postgres/init.sh` do `datajoin-app`:

```bash
CREATE ROLE agent_kb_admin LOGIN PASSWORD '${AGENT_KB_PASSWORD}';
CREATE DATABASE agent_kb OWNER agent_kb_admin;
\c agent_kb
CREATE EXTENSION IF NOT EXISTS vector;
```

**Isolamento por tenant via schema** (mesma simetria que `luminea_staging`/`luminea_curated`):

```
agent_kb
├── kb_luminea
│   ├── model_embeddings    (model_name, content_md, embedding vector(1536), updated_at, content_hash)
│   └── glossary_embeddings
├── kb_<outro_tenant>
│   └── ...
```

Schema por tenant > tabela única com `tenant_id`:
- Simetria com warehouse
- Drop limpo se um tenant churnar
- Permissões granulares (`GRANT USAGE ON SCHEMA kb_luminea TO agent_role_luminea`)

**Por que DB dedicado e não juntar em platform_metadata:**
- Alembic head fica imune a migrations do agente
- Backups separados — `agent_kb` é regenerável (re-roda DAG), `platform_metadata` não é
- Permissão limpa: role do agente só vê `agent_kb`, não acessa tenants/users do portal por acidente

**Por que não no warehouse:**
- DW é dado do cliente (compliance, LGPD). Misturar embeddings de metadata polui esse domínio
- DW pode ser dump-restored, recriado, particionado — embeddings teriam que migrar junto

### 2. `datajoin-airflow` ganha pipeline de geração

DAG nova `<tenant>__agent_docs`, encadeada com a `curated`:

```
[Cosmos: dbt build curated]
    ↓
[dbt docs generate]            # cria target/catalog.json + target/manifest.json
    ↓
[manifest_to_agent_catalog.py] # gera 2 artefatos por modelo:
                               #   - <model>.md     (RAG / contexto do LLM)
                               #   - <model>.json   (validação programática)
                               # + _index.md global (camada barata, no system prompt)
    ↓
[embedder.py]                  # upsert no pgvector com hash check (só re-embedda o que mudou)
```

**Por que `.md` E `.json` em paralelo:**
- `.md` é o contexto que o LLM consome (descrições ricas, exemplos, lineage humano)
- `.json` é o que o **validador de SQL** precisa: estrutura tipada com colunas, tipos exatos, joins permitidos, PK/FK. Sem ele, o validador teria que parsear markdown — frágil. Mesma fonte (`manifest.json` + `catalog.json`), dois formatos otimizados pra consumidores diferentes.

Exemplo do `.json`:

```json
{
  "model_name": "fct_vendas",
  "relation_name": "luminea_curated.fct_vendas",
  "description": "Fato de vendas consolidando CA + CRM.",
  "columns": {
    "venda_id":     {"type": "uuid",          "primary_key": true,  "nullable": false},
    "cliente_id":   {"type": "uuid",          "foreign_key": true,  "nullable": false},
    "tipo_pessoa":  {"type": "varchar(2)",    "accepted_values": ["pf", "pj"]},
    "valor_total":  {"type": "numeric(18,2)", "nullable": false},
    "data_venda":   {"type": "timestamp",     "nullable": false}
  },
  "allowed_joins": [
    {"to": "dim_clientes", "left_key": "cliente_id", "right_key": "cliente_id", "relationship": "many_to_one"}
  ],
  "upstream": ["stg_conta_azul__vendas", "stg_crm__oportunidades"]
}
```

Estrutura no repo:

```
datajoin-airflow/
├── plugins/
│   └── agent_docs/                              # módulo novo
│       ├── manifest_to_agent_catalog.py         # parser manifest+catalog -> .md + .json
│       ├── md_writer.py                         # render do .md
│       ├── json_writer.py                       # render do .json estruturado
│       ├── index_builder.py                     # gera o _index.md (camada barata)
│       └── embedder.py                          # chama embeddings + upsert no pgvector
├── dags/
│   └── luminea__agent_docs.py                   # 1 DAG por tenant (mesmo pattern dos ETLs)
└── dbt/projects/luminea/agent_docs/             # output (.gitignore — regenerado)
    ├── models/<model>.md
    ├── models/<model>.json
    └── _index.md
```

A DAG `luminea__agent_docs` conecta no `agent_kb` via Airflow Connection nova: `agent_kb_default` (host `postgres`, db `agent_kb`, user `agent_kb_admin`).

**Hash check é crítico:** upsert por hash do `.md` — só re-embedda o que mudou. Embedding tem custo (`text-embedding-3-small` ≈ $0.02/M tokens) e dbt regen é frequente. Sem hash check, paga embedding toda noite à toa.

### 3. `datajoin-app` ganha módulo `agent/`

Layout:

```
datajoin-app/
└── api/
    └── app/
        ├── tenants/                        # existente
        ├── platform_telemetry/             # existente
        └── agent/                          # NOVO
            ├── routes.py                   # POST /api/agent/chat (streaming SSE)
            ├── tools/
            │   ├── list_tables.py
            │   ├── get_table.py             # le .md (contexto pro LLM)
            │   ├── search_tables.py         # query pgvector
            │   ├── explain_lineage.py
            │   └── execute_sql.py           # entrada de execucao — passa pelo pipeline sql/
            ├── sql/                         # camada CRITICA — guardas do warehouse
            │   ├── parser.py                # sqlglot — parse + AST inspection
            │   ├── validator.py             # le .json do catalog: tabelas existem?
            │   │                            # colunas existem? joins permitidos?
            │   ├── policies.py              # SELECT-only, no multi-statement, row limit,
            │   │                            # timeout, allowlist de schemas, no DDL/DML
            │   └── executor.py              # roda no warehouse via role read-only
            ├── prompts/
            │   └── system.md
            ├── conversation.py              # CRUD em agent_conversations + agent_messages + agent_query_runs
            └── llm_client.py                # wrapper Anthropic SDK
```

**Protocolo de execução de SQL** (não é "agente chama execute_sql direto"):

```
LLM propoe SQL
    ↓
sql/parser.py        ← parse via sqlglot, rejeita se nao for SELECT
    ↓
sql/validator.py     ← cruza com <model>.json: tabelas/colunas/joins existem e sao permitidos?
    ↓
sql/policies.py      ← aplica row limit (ex: LIMIT 1000 forcado), timeout (ex: 15s)
    ↓
sql/executor.py      ← roda com role tenant-scoped READ-ONLY
    ↓
se erro em qualquer etapa → retorna mensagem estruturada pro LLM, que tenta de novo
```

**Segurança do `executor.py`** — não-negociáveis:

| Camada | O que faz |
|---|---|
| Role do banco | `<tenant>_agent_reader` — `GRANT SELECT` apenas nos schemas do tenant; sem `INSERT/UPDATE/DELETE/DDL` |
| Parser (sqlglot) | Rejeita se não for `SELECT`; rejeita múltiplas statements; rejeita CTEs com side-effects |
| Validator | Tabela/coluna citada na SQL existe em `<model>.json` daquele tenant? Senão, erro estruturado |
| Allowlist | Schemas permitidos vêm de config (`{tenant}_curated`, `{tenant}_delivery`). `{tenant}_raw` e `{tenant}_staging` bloqueados por default |
| Row limit | Força `LIMIT N` no AST (não confia em texto); N configurável (default 1000) |
| Statement timeout | `SET LOCAL statement_timeout = 15000` antes da query |
| Log | Toda SQL gerada + status + latência → `agent_query_runs` |

`sqlglot` é a peça chave: parser de SQL multi-dialect que dá AST manipulável. Permite inspeção e rewrites (forçar `LIMIT`, validar refs) sem regex frágil.

**Conexões novas no `datajoin-app`** (env vars):
- `METADATA_DB_URL` — já existe
- `DW_DEFAULT_*` — já existe (executa SQL no warehouse, mas com role específica do tenant pra não ter blanket SELECT permission)
- `AGENT_KB_DB_URL` — NOVA (lê embeddings)
- `ANTHROPIC_API_KEY` — NOVA

**Tabelas novas em `platform_metadata`** (Alembic) — **4 tabelas desde o dia 1**, não só conversas. Custo de adicionar agora é zero; custo de NÃO ter histórico de SQL gerada desde o começo é alto (perde dataset crítico de debug + treino):

```sql
agent_conversations (
    id, tenant_id, user_id, started_at, summary
)
agent_messages (
    id, conversation_id, role, content, tools_called jsonb, created_at
)
agent_query_runs (
    id, conversation_id, message_id, tenant_id, user_id,
    generated_sql,              -- o que o LLM propos
    validated_sql,              -- o que efetivamente foi pro warehouse (com LIMIT etc)
    tables_used jsonb,          -- extraido pelo sqlglot
    execution_status,           -- ok | parse_error | validator_error | policy_error | timeout | runtime_error
    row_count, latency_ms, error_message,
    created_at
)
agent_feedback (
    id, message_id, rating,     -- thumbs up/down ou 1-5
    user_comment, created_at
)
```

Sem isso, no dia que precisar entender "por que o agente errou na pergunta X do cliente Y na semana passada", você não tem dados. Com isso, dá pra fazer dashboards, retrain offline, e medir taxa de validador_error vs runtime_error pra saber onde o agente está fraco.

### 4. Portal Next.js — só uma página nova

Nada estrutural. `/chat` route, componente de input, SSE consumer pra streaming. Reusa o auth/session existente. ~1-2 dias de trabalho quando chegar a hora.

## Fluxo end-to-end

1. **Build noturno (Airflow):** ETL → dbt build → `dbt docs generate` → `manifest_to_md.py` produz `.md` por modelo + `_index.md` → `embedder.py` upserta embeddings em `agent_kb.kb_luminea.model_embeddings`
2. **Usuário faz pergunta no portal:** POST `/api/agent/chat` com `{tenant_id, message}`
3. **API valida tenant + abre conversa em `platform_metadata.agent_conversations`**
4. **Loop do agente** (FastAPI + Anthropic SDK com tool use):
   - Tool `search_tables` → query pgvector em `agent_kb.kb_luminea` → top-5 modelos
   - Tool `get_table` → lê `.md` do mesmo schema → retorna conteúdo
   - Tool `execute_sql` → roda no `warehouse` com role tenant-scoped → retorna rows
5. **Stream da resposta via SSE** pro portal
6. **Persiste mensagens em `platform_metadata.agent_messages`** (auditoria + memória futura)

## Estratégia de retrieval (token-econômica)

Quando o `models/` do tenant crescer, **nunca passar o `manifest.json` cru**. Camadas:

### Camada 1: Índice fixo no system prompt (~2-5K tokens)
Um `_index.md` agrupado por tag/domínio, com 1 linha por modelo. Cabe inteiro no system prompt. Agente lê e já sabe **onde buscar** sem ver detalhe.

```markdown
## Financeiro
- `fct_movimentacoes_financeiras` — Movimentações de caixa consolidadas
- `dim_categorias_dre` — Árvore de categorias do DRE

## Vendas
- `fct_vendas` — Fato de vendas (Conta Azul + comissão do CRM)
- `dim_clientes` — Clientes consolidados (CA + CRM, deduplicados)
```

### Camada 2: Detalhe por modelo (sob demanda)
`.md` completo com descrição + colunas + tipos + exemplos. ~2 KB por modelo. Só carrega quando o agente pede.

### Camada 3: Tools (4 functions)

```
list_tables(domain: str | null) -> string          # retorna o índice (Camada 1)
get_table(name: str) -> string                     # retorna o .md (Camada 2)
search_tables(query: str, top_k: int = 5) -> list  # busca semântica (pgvector)
explain_lineage(table: str, depth: int = 1) -> list  # upstream/downstream do manifest
```

### Compressão extra
- **Colunas por importância:** marca no `_schema.yml` com `meta.agent_priority: primary|secondary|audit`. Default só retorna primárias. Corta 40-60% dos tokens por modelo.
- **Catálogo de métricas + glossário tenant-level:** dois níveis numa única estrutura, gerada junto com os `.md` de modelo:

  **`_glossary.md`** — termos de negócio (tradução):
  ```yaml
  terms:
    - name: "pessoa física"
      synonyms: ["PF", "cliente PF"]
      expression: "tipo_pessoa = 'pf'"
    - name: "cliente ativo"
      expression: "comprou nos ultimos 90 dias"
  ```

  **`_metrics.json`** — métricas executáveis (definição → SQL fragment):
  ```json
  {
    "vendas_aprovadas": {
      "description": "Quantidade de vendas com status aprovada",
      "model": "fct_vendas",
      "expression": "COUNT(DISTINCT venda_id)",
      "filters": ["status = 'aprovada'"],
      "date_column": "data_venda"
    },
    "ticket_medio": {
      "description": "Valor medio por venda",
      "model": "fct_vendas",
      "expression": "SUM(valor_total) / COUNT(DISTINCT venda_id)"
    }
  }
  ```

  Diferença é importante: **glossário** traduz termo ("PF" → filtro); **catálogo de métricas** é definição executável com expression + filters + date_column. Sem catálogo de métrica, o agente acerta a tabela mas erra a fórmula de negócio (ex: contar `venda_id` em vez de `DISTINCT venda_id`, esquecer `status = 'aprovada'`). Fonte pode ser `meta:` no `_schema.yml` do dbt ou YAML separado — script gera ambos.
- **Exemplos de uso no `.md`** dos fact tables centrais (2-3 queries-exemplo por tabela). ~300 tokens cada, ROI alto.
- **Cache em conversation state:** se já fetched, não fetcha de novo.
- **Lineage barata:** `explain_lineage` antes de gerar SQL com JOIN — evita JOIN inventado.

## Pré-processamento dos artefatos dbt

Fontes de verdade:
- **`manifest.json`** (gerado por `dbt compile/run/test`): tudo do YAML resolvido + lineage + tags + materialização
- **`catalog.json`** (gerado por `dbt docs generate`): adiciona tipos REAIS do warehouse via introspect

**Por que `.md` e não `manifest.json` cru:**

| | manifest.json cru | .md gerado |
|---|---|---|
| Tamanho | 5–20 MB | ~2 KB por modelo |
| Tokens no contexto | Estoura janela rápido | Cabe vários no contexto |
| RAG embedding | Ruim — vetores ruidosos | Excelente — chunks semanticamente coesos |
| Diff revisável | Ilegível | Legível, code review do "knowledge base" |

Formato do `.md` por modelo:

```markdown
# luminea_curated.fct_vendas

**Descrição:** Fato de vendas consolidando Conta Azul + comissão do CRM.
**Materialização:** table
**Atualizado por:** DAG `luminea__curated` (Cosmos)
**Fontes upstream:**
- `stg_conta_azul__vendas`
- `stg_crm__oportunidades`

## Colunas

| coluna | tipo | descrição | tests |
|---|---|---|---|
| `venda_id` | uuid | ID único da venda | not_null, unique |
| `cliente_id` | uuid | FK pra dim_clientes.cliente_id | not_null |
| `tipo_pessoa` | varchar(2) | `pf` ou `pj` | accepted_values: pf, pj |
| `valor_total` | numeric(18,2) | Valor bruto em BRL | not_null |

## Exemplos de uso

\`\`\`sql
SELECT tipo_pessoa, SUM(valor_total)
FROM luminea_curated.fct_vendas
WHERE data_venda >= NOW() - INTERVAL '1 month'
GROUP BY tipo_pessoa
\`\`\`
```

## Resumo dos repos depois da implementação

```
datajoin-app/          ← portal + API + chat (agent module)
datajoin-airflow/      ← ETL + dbt + DAG agent_docs (gera + embedda)
datajoin-vm-hostinger/ ← infra (cloudflared, network, sysctl)
```

**Mesmos 3 repos.** Zero serviço novo. Um DB extra (mesma instância). Um módulo em cada repo existente.

## Quando reconsiderar a arquitetura

| Sinal | Mudança |
|---|---|
| LLM workload começa a derrubar latência da API do portal | Extrai agent pra worker assíncrono (mesmo repo, container separado) |
| Volume de embeddings ultrapassa ~10 GB | Migra `agent_kb` pra instância dedicada |
| Time cresce e dedica gente só pro agente | Pode justificar repo separado |
| Cliente quer rodar agente on-prem | Vira service standalone com API contract |

## Ordem de implementação sugerida

Refinada pra **reduzir risco** — agente NÃO toca warehouse até ter validador + audit completos. Ordem:

1. **Infra: `agent_kb` provisionado**
   - Role/db `agent_kb` no `init.sh` do `datajoin-app` + `CREATE EXTENSION vector;`
   - Tabelas do schema `kb_luminea` (model_embeddings, glossary_embeddings, metric_catalog)
   - Sem código ainda. Smoke test: connect via psql + insert dummy embedding.

2. **Geração de artefatos no Airflow (sem execução de SQL)**
   - Módulo `plugins/agent_docs/` com `manifest_to_agent_catalog.py` (gera `.md` + `.json` por modelo)
   - `_index.md` agrupado por tag
   - DAG `luminea__agent_docs` rodando manualmente
   - **Output validado por humano**: abre os `.md`/`.json` gerados, confere descrições, ajusta `meta:` no dbt onde faltou contexto

3. **Embedding + retrieval (sem agente ainda)**
   - `embedder.py` com hash check + upsert no pgvector
   - 1 endpoint isolado `/api/agent/_debug/search?q=...` retorna top-K modelos
   - Testa com perguntas reais que o time já tem em mente

4. **Tabelas de auditoria em `platform_metadata`**
   - Alembic migration com as 4 tabelas (`agent_conversations`, `agent_messages`, `agent_query_runs`, `agent_feedback`)
   - Mesmo sem agente, prepara o terreno

5. **Geração de SQL SEM execução (preview mode)**
   - Endpoint `/api/agent/preview` que recebe pergunta, retorna **a SQL proposta + tabelas usadas + custo estimado**, NÃO executa
   - Loop: LLM com tools de knowledge (`search_tables`, `get_table`) — sem `execute_sql` ainda
   - Mostra a SQL gerada no frontend ou via curl pra revisão humana
   - Fase crítica: aqui você descobre o que o agente erra ANTES dele tocar warehouse

6. **Camada `sql/` — parser + validator + policies**
   - `sqlglot` parseando, rejeitando não-SELECT, multi-statement, etc
   - Validator cruzando com `.json` por modelo (tabela existe? coluna existe? join permitido?)
   - Policies: row limit forçado no AST, statement_timeout, allowlist de schemas
   - Testes unitários cobrindo tentativas de SQL injection e bypass

7. **Role read-only no warehouse + execução protegida**
   - `<tenant>_agent_reader` no Postgres, `GRANT SELECT` apenas em `<tenant>_curated` e `<tenant>_delivery`
   - `executor.py` usa essa role exclusivamente
   - Endpoint `/api/agent/chat` agora completo (tool `execute_sql` habilitada)
   - Toda execução loga em `agent_query_runs`

8. **Portal `/chat` UI**
   - Next.js consumindo SSE
   - Componente de feedback (thumbs up/down) populando `agent_feedback`

9. **Catálogo de métricas + glossário**
   - `_metrics.json` por tenant a partir de `meta:` do dbt ou YAML dedicado
   - Tool nova `get_metric_definition(name)` no agente
   - Roll-out quando começar a ver erro de fórmula de negócio nos `agent_query_runs`

10. **Otimizações on-demand**
    - `meta.agent_priority: primary|secondary|audit` nas colunas (corta tokens)
    - Cache em conversation state
    - Re-embedding diferencial por hash
    - Pgvector indexes (HNSW/IVFFlat) quando o índice ficar > 50K vetores

**Pontos de NÃO-volta** marcados:
- Após passo 5, time já consegue revisar SQL gerada → confiança calibrada
- Passo 7 só depois de 5 e 6 passarem em testes — antes disso, agente NÃO toca warehouse

## Decisões revisitadas (e o que ficou de fora)

Conscientemente **não adotado**, mas considerado:

- **Schema único com `tenant_id` em vez de schema por tenant** — mantemos schema por tenant por simetria com o warehouse (`luminea_staging`, `luminea_curated`, ...). Padrão consistente vale mais que economia de DDL dinâmico.
- **Abstração de LLM provider (interface genérica Anthropic/OpenAI)** — YAGNI. Vai com `anthropic` SDK direto. Refator pra interface multi-provider leva 1h quando houver razão concreta (custo, qualidade, fallback).

## TL;DR

- **Mesma instância Postgres, DB novo `agent_kb`** com pgvector + schema por tenant
- **Gera `.md` e embeddings no Airflow** (DAG colado no dbt build)
- **Código do agente no `datajoin-app`** como módulo, mesma API, FastAPI streaming
- **Histórico de conversa no `platform_metadata`** (Alembic do portal)
- **Zero repo novo, zero service novo**
