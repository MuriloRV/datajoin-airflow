# dbt — convenção de camadas

Cada tenant tem **4 schemas/datasets** no DW, sempre nessa ordem:

| Camada | Schema | O que vive aqui | Persistência |
|---|---|---|---|
| Raw | `<slug>_raw` | Replicas brutas dos conectores (Shopify, Meta Ads, ERP). Schema dos sources. | Schema persiste; **tabelas são truncadas pela DAG** no início do próximo run |
| Staging | `<slug>_staging` | Limpeza, casting, dedup, renomeação. Modelos `stg_*` do dbt. | Schema persiste; tabelas truncadas no início do próximo run (igual raw) |
| Curated | `<slug>_curated` | Modelos de negócio: fatos e dims. `fct_*`, `dim_*`. | Persiste entre runs (dbt incremental ou table) |
| Delivery | `<slug>_delivery` | Marts finais consumidos por dashboards e exports. Agregados, denormalizados. | Persiste entre runs |

`<slug>` = `tenants.slug` (snake_case ASCII, 3..24 chars — mig 0024 + `CHECK ck_tenants_slug_format`).

## Como os schemas chegam lá

- **Provisão inicial:** ao vincular um tenant a um DW (`POST /tenants/{tid}/warehouses`) ou via `make seed`, o backend roda `provision_layered_schemas(dw, slug)` em `app/services/warehouse/provisioner.py` e executa `CREATE SCHEMA IF NOT EXISTS x4` no DW. Idempotente.
- **Recuperação:** `POST /tenants/{tid}/warehouses/{wh_id}/reprovision` (admin) re-executa o mesmo CREATE SCHEMA. Use quando o DW foi recreado, quando a provisão inicial falhou parcialmente, ou pra confirmar estado.

## Padrão dbt (por projeto)

Cada projeto dbt declara `+schema:` por pasta de modelo. Exemplo:

```yaml
# dbt_project.yml
models:
  <projeto>:
    staging:
      +materialized: view
      +schema: staging       # vira <slug>_staging via macro abaixo
    curated:
      +materialized: table
      +schema: curated       # vira <slug>_curated
    delivery:
      +materialized: table
      +schema: delivery      # vira <slug>_delivery
```

Macro `generate_schema_name` (em `macros/generate_schema_name.sql`) prefixa com `<slug>_`:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set tenant = var('tenant_slug') -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ tenant }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

Invocação:

```bash
dbt run --vars '{tenant_slug: acme}' --target postgres
```

> Projeto de referência atual: `dbt/projects/luminea/` (3 camadas medalhão + Cosmos). Novos projetos seguem essa convenção.

## Raw / staging são "ephemeral em dado", não em schema

Confusão comum: o **schema** persiste, só as **tabelas** são limpas a cada run. Razões:

- DAGs do Airflow rodam `TRUNCATE` ou `DROP TABLE` em `<slug>_raw.*` e `<slug>_staging.*` no início do pipeline, antes da extração e do `dbt build`.
- Schema persistente evita custo de `CREATE SCHEMA` em todo run + permite que dbt sources resolva `{{ source(...) }}` sem dança de DDL.
- O **watermark de extração incremental** (cursor "última atualização lida do Shopify") **não pode** viver em raw — vai pro metadata Postgres em tabela própria. (Não modelado ainda; será adicionado quando ingerirmos dado real.)

## O que NÃO fazer

- **Não materializar staging como `table`** — a regra é `view` (ou `ephemeral` se não precisa debug). Tabela em staging só polui storage.
- **Não criar tabela em raw via dbt** — raw é da ingest layer, não do dbt. `{{ source(...) }}` só lê.
- **Não expor raw/staging pro cliente** — só `curated` e `delivery` aparecem em dashboards/exports. Filtro está em `CLIENT_VISIBLE_LAYERS` (`app/services/warehouse/naming.py`).
- **Não criar 5ª camada** — se aparecer um caso novo (ex: `_archive`), discutir antes. Convenção curta = manutenção barata.
