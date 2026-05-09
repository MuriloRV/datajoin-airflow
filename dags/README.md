# DAGs do Airflow — convenção de naming + tags

## Naming: `<tenant_slug>__<purpose>`

Double underscore como separador entre prefixo do tenant e propósito da DAG. Casa com a convenção de raw tables (`<source>__<table>`).

```
acme__example_etl
acme__dbt_build
luminea__shopify_etl
finhub__b3_quotes_etl
```

Regras:

- `tenant_slug` é o slug snake_case do tenant em `tenants.slug` (mig 0024 — 3..24 chars, validado por CHECK no banco). Sem abreviação: `padaria_bela_vista__pdv_etl`, não `padaria__pdv_etl`.
- `purpose` é o que a DAG faz, em snake_case: `example_etl`, `dbt_build`, `b3_quotes_etl`, `risk_alerts_automation`. Curto, descritivo.
- Mesmo `purpose` aparece em múltiplos tenants (`luminea__shopify_etl`, `acme__shopify_etl`) — isso é desejado.

Validação na DAG: `assert dag_id.startswith(f"{TENANT_SLUG}__")` no topo do arquivo (defesa contra typo).

## Tags obrigatórias

```python
tags=["tenant:acme", "kind:etl"]
```

- `tenant:<slug>` — sempre. A UI do Airflow tem filtro nativo por tag — vale ouro pra suporte filtrar tudo de um cliente.
- `kind:<etl|dbt|automation|alert>` — categoria operacional. Padroniza vocabulário pra dashboards/observabilidade.
- Tags adicionais opcionais: `source:shopify`, `freq:hourly`, `tier:dedicated` — sem regra estrita, mas siga o padrão `chave:valor` (snake_case).

## Organização no filesystem

Pasta por tenant: `dags/<tenant_slug>/<purpose>.py`. Adotada quando o terceiro tenant entrou em produção e o flat passou a atrapalhar suporte ("acha tudo do ACME"). DAG ID continua `<slug>__<purpose>` (não muda só o filesystem).

```
dags/
├── README.md
├── _factories/                 # futuro — geram N DAGs por tenant
├── acme/
│   ├── example_etl.py          # dag_id: acme__example_etl
│   └── dbt_build.py            # dag_id: acme__dbt_build
├── luminea/
│   ├── conta_azul.py           # dag_id: luminea__conta_azul_etl
│   └── (outras)
└── ...
```

Convenção:

- **Subpasta = slug do tenant** (snake_case, igual `tenants.slug`).
- **Filename = purpose** (sem repetir o slug).
- **dag_id continua `<slug>__<purpose>`** — assertivo no topo do arquivo.

Conectores (lib reusável de API client) ficam em `plugins/connectors/<source>/`. Cada conector é isolado dos outros (ver `plugins/connectors/__init__.py`).

## Factory pattern (futuro)

Quando o mesmo `purpose` rodar pra 5+ tenants com a mesma lógica, vale criar uma factory:

```python
# dags/factories/shopify_etl.py
from app.api_client import get_tenants_with_integration

for tenant in get_tenants_with_integration("shopify"):
    create_dag(
        dag_id=f"{tenant.slug}__shopify_etl",
        schedule="0 2 * * *",
        tags=[f"tenant:{tenant.slug}", "kind:etl", "source:shopify"],
        ...
    )
```

Trade-off: factory é "mágica" — debug mais difícil, e erro na factory derruba múltiplas DAGs ao mesmo tempo. Vale quando o purpose é genuinamente o mesmo entre tenants. Pra DAGs custom (cliente fez request específico), arquivo dedicado é OK — mantém o naming `<slug>__<purpose_custom>`.

## Suspender DAGs de tenant suspenso

`tenants.status='suspended'` deve refletir em DAGs pausadas. Hoje feito por API call (`POST /tenants/{tid}/pipelines/{airflow_dag_id}/pause` — endpoint existente). Em factory pattern, basta a factory pular tenants suspensos no loop e o DagBag remove a DAG.

## Templating

DAG é Python — não tem placeholder mágico. Slug do tenant fica como constante no topo (`TENANT_SLUG = "acme"`) ou injetado pela factory. Nunca interpolar slug que venha de input externo (não tem como vir, mas a regra fica).

## Pegadinhas observadas

1. **`max_active_runs=1` em DAGs dbt** — evita 2 instâncias gravando no mesmo `target/`. Já está em `acme__dbt_build`.
2. **`catchup=False`** em todas — não queremos backfill automático (rodar 30 dias retroativos quando alguém ativa a DAG).
3. **Telemetria via `TelemetryContext.from_airflow(context, tenant_id=...)`** — sempre. As pipeline_runs/pipeline_tasks aparecem no portal por causa disso.
