# datajoin-airflow

Repositório de **Airflow + DAGs + plugins (conectores ETL) + dbt projects** da plataforma datajoin.

> **API + Portal + Postgres** vivem no repositório irmão [`datajoin-app`](https://github.com/MuriloRV/datajoin-app). Os 2 composes compartilham a network docker `dj_network` e o Postgres provisionado pelo `datajoin-app` (que cria o db `airflow` consumido aqui).

## Dois caminhos: dev local vs prod

| | Dev (local) | Prod (Hostinger VM) |
|---|---|---|
| Como roda | `docker compose -f docker-compose.yml up` | `docker compose -f compose.prod.yml up` |
| Onde mora | sua máquina | VM Hostinger KVM 4 (16 GB) |
| DAGs/plugins | volume mount (hot reload) | baked na imagem |
| Imagem | build local | `ghcr.io/MuriloRV/datajoin-airflow/airflow:<tag>` (CI builda+pusha) |
| Postgres | container do `datajoin-app` | container Docker do `datajoin-app` na MESMA VM (`dj_network`) |
| UI | `localhost:8080` (porta exposta) | `airflow.datajoin.cloud` via Cloudflare Tunnel (sem porta exposta) |
| Logs | named volume `airflow_logs` | named volume `airflow_logs` (acesso via `docker compose logs`) |
| Pipeline | NÃO entra no pipeline | `.github/workflows/deploy.yml` (push em main → SSH → compose up) |

> O `docker-compose.yml` é **dev-only**. O pipeline de deploy usa `compose.prod.yml`.

## Pré-requisitos (dev)

- Docker Desktop com Compose v2
- Repo `datajoin-app` clonado ao lado e com o `postgres` rodando

## Setup local (primeira vez)

```bash
# 1. sobe o postgres do datajoin-app (cria a rede dj_network e provisiona o db `airflow`)
cd ../datajoin-app
docker compose up -d postgres

# 2. cria .env do airflow a partir do template
cd ../datajoin-airflow
cp .env.example .env

# 3. sobe os 4 services do Airflow (init -> apiserver + dag-processor + scheduler)
docker compose up -d --build
```

UI do Airflow: http://localhost:8080 (admin / admin).

## Estrutura

```
datajoin-airflow/
├── dags/                              # DAGs por tenant (luminea__conta_azul_etl, etc)
├── plugins/                           # conectores ETL + utilitários compartilhados
│   └── connectors/
│       └── conta_azul/                # 24 entidades, 6 incrementais via watermark
├── dbt/                               # projetos dbt por tenant (staging + marts)
├── scripts/
│   └── contaazul_get_refresh_token.py # gera refresh_token pra colar na Connection do Airflow
├── docker/
│   ├── Dockerfile                     # apache/airflow:3.2.0-python3.13 + libpq + requirements + DAGs/plugins/dbt baked
│   └── requirements.txt               # libs Python adicionais (cosmos, conectores, etc)
├── docker-compose.yml                 # DEV-ONLY — 5 services Airflow (init/api/dag-processor/scheduler/triggerer)
├── compose.prod.yml                   # PROD — mesmos services com imagem ghcr + recursos limits, sem ports
├── .github/workflows/deploy.yml       # push em main -> build+push ghcr -> SSH na VM -> compose up
├── .env.example                       # FERNET_KEY, JWT_SECRET, SERVICE_TOKEN, ADMIN_PASSWORD ... (DEV)
└── .env.prod.example                  # template do .env de prod (renderizado pelo CI a partir de secrets)
```

## Como conversa com o `datajoin-app`

| Direção | Como | O quê |
|---|---|---|
| Airflow → Postgres | Rede docker `dj_network`, hostname `postgres:5432` | metadata do Airflow (db `airflow`, user `airflow`) + DW (db `warehouse`, user `dw_admin`) |
| Airflow → API datajoin | HTTP `http://api:8000` (rede compartilhada) com header `X-Service-Token` | reportar telemetria de runs, ler config de tenant, etc |
| API datajoin → Airflow | HTTP `http://airflow-apiserver:8080` (rede compartilhada) | trigger DAG, ler status, gerenciar Connections |

Auth de service-to-service é via `SERVICE_TOKEN` (estático em dev). Em prod: substituir por OIDC/mTLS.

## Airflow 3.x — pontos importantes

- **`api-server` substitui `webserver`** (UI + REST API unificados na mesma porta 8080).
- **`dag-processor` é processo dedicado** — saiu do scheduler. Necessário pra DAGs serem parseadas.
- **Execution API com JWT compartilhado** — `AIRFLOW__API_AUTH__JWT_SECRET` precisa ser igual em apiserver e scheduler. Task workers assinam tokens com ela pra reportar status.
- **`SimpleAuthManager` é dev-only.** Em prod usar FAB ou auth externo. Em dev, escrevemos manualmente o file `simple_auth_manager_passwords.json.generated` pra ter `admin/admin` previsível.
- **Cosmos chama `dbt parse` no import** da DAG — cacheado depois, mas o primeiro import pode bater 30s. Por isso o `DAG_FILE_PROCESSOR_TIMEOUT=120`.

## Conectores ETL

Cada conector vive em `plugins/connectors/<nome>/`. Padrão estabelecido pelo conector Conta Azul (24 entidades, 6 incrementais por watermark, raw → staging → curated):

- `client.py` — OAuth + retries + paginação
- `extractors/` — 1 classe por entidade, registry pattern em `__init__.py`
- `loader.py` — genérico, resolve via registry (sem if-else por entidade)
- `schemas.py` — Pydantic com `extra="allow"` + raw jsonb fallback (absorve schema drift)
- `README.md` + `PENDENCIAS_PROD.md` por conector

## Comandos úteis

```bash
docker compose logs -f --tail=100             # tail de todos os services Airflow
docker compose logs -f airflow-scheduler      # só scheduler
docker compose exec airflow-apiserver bash    # shell num container
docker compose down                           # para (mantém volumes)
docker compose down -v                        # DESTRUTIVO: apaga logs do Airflow
```

Pra resetar o metadata do Airflow (apaga histórico de DAG runs):

```bash
# garante que o postgres do datajoin-app está rodando
docker compose -f ../datajoin-app/docker-compose.yml exec postgres \
  psql -U postgres -c "DROP DATABASE airflow; CREATE DATABASE airflow OWNER airflow;"
docker compose up -d airflow-init  # roda `airflow db migrate` de novo
```

## Princípios

1. **Conectores 100% custom em Python.** Nada de Fivetran/Airbyte.
2. **Idempotência via UPSERT** (chave `id` na raw) + `extra="allow"` nos schemas Pydantic.
3. **Schema drift tolerável** — raw jsonb fallback absorve campos novos da API.
4. **Watermarks centralizados** — extractors leem/escrevem em `etl_watermarks` (tabela do datajoin-app) via API platform_telemetry.
5. **dbt rodado via Cosmos** — projetos por tenant em `dbt/projects/`, integrado com Airflow.

## Deploy em produção (Hostinger VM)

Push em `main` → workflow `.github/workflows/deploy.yml`:

1. Builda `docker/Dockerfile` (DAGs/plugins/dbt baked) e pusha pra `ghcr.io/MuriloRV/datajoin-airflow/airflow:sha-<commit>`
2. SSH na VM (`deploy@$VM_HOST`), materializa `/opt/datajoin/airflow/.env` a partir do secret `AIRFLOW_DOTENV` + injeta `IMAGE_TAG`
3. `docker compose pull` → `docker compose run --rm airflow-init` (= `airflow db migrate` + escreve auth file) → `docker compose up -d --no-deps --force-recreate --wait` (4 services principais)
4. Health check via `dj_network` em `http://airflow-apiserver:8080/api/v2/version`
5. `docker image prune -af` (rollback continua possível via re-pull do ghcr)

### UI pública via Cloudflare Tunnel

A UI roda **sem porta exposta no host** — chega via CF tunnel. Pra rotear `airflow.datajoin.cloud`:

1. Painel Zero Trust → Networks → Tunnels → `datajoin-app-prod` → **Public Hostnames** → **Add a public hostname**
2. Subdomain: `airflow`, Domain: `datajoin.cloud`
3. Service: HTTP → URL `airflow-apiserver:8080`
4. Save (DNS propaga em ~30s)

O `cloudflared` já está na `dj_network`, então resolve `airflow-apiserver` por DNS interno do Docker.

### Secrets do GitHub Actions

| Secret | Valor |
|---|---|
| `DEPLOY_SSH_KEY` | Private key (ed25519) com acesso ao user `deploy` na VM (mesma usada pelo `datajoin-app`) |
| `VM_HOST` | IP/DNS da VM Hostinger |
| `AIRFLOW_DOTENV` | Conteúdo completo do `.env` de prod (ver `.env.prod.example`) |

### Recursos (compose.prod.yml — limits ≈ 6.3 GB; sobra pro datajoin-app)

| Service | Memory limit | CPU limit |
|---|---|---|
| airflow-apiserver | 768Mi | 1 |
| airflow-scheduler | 4Gi | 2 |
| airflow-dag-processor | 1Gi | 1 |
| airflow-triggerer | 512Mi | 0.5 |

`PARALLELISM=8`, `MAX_ACTIVE_RUNS_PER_DAG=1`, timezone `America/Sao_Paulo`. Ajuste em `compose.prod.yml`.

### Acesso aos logs em prod

Como é named volume Docker (sem hostPath), o tail é via container:

```bash
ssh deploy@<vm>
docker compose -f /opt/datajoin/airflow/compose.yml logs -f --tail=200 airflow-scheduler
docker compose -f /opt/datajoin/airflow/compose.yml exec airflow-scheduler \
  tail -f /opt/airflow/logs/dag_id=<dag>/run_id=<run>/task_id=<task>/attempt=1.log
```
