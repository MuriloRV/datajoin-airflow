# Deploy — Airflow no k3s (Hostinger VM)

Stack: Airflow 3.2 em k3s single-node, Helm Chart oficial. Postgres metadata mora no **Docker do datajoin-app** (mesma VM); logs vão pra `/var/log/datajoin-airflow` no host.

> **`docker-compose.yml` da raiz é DEV-ONLY.** O pipeline (`.github/workflows/deploy.yml`) NÃO o usa — só builda `docker/Dockerfile` e importa no containerd do k3s.

## Topologia na VM

```
VM Hostinger KVM 4 (4 vCPU, 16 GB)
├── Docker (datajoin-app)               ~5 GB
│   └── postgres:5432  ←──────────┐
└── k3s + Helm (datajoin-airflow)  │   ~8–10 GB
    ├── api-server  (NodePort 30080)
    ├── scheduler   (LocalExecutor — tasks rodam aqui)
    ├── dag-processor
    ├── triggerer
    └── Service `dj-postgres` ────┘  (Endpoints → IP do host)
```

## Setup inicial na VM (1 vez)

```bash
ssh ubuntu@<VM_IP>
git clone https://github.com/<seu_user>/datajoin-airflow.git
cd datajoin-airflow

# 1. Instala k3s + helm + docker e cria /var/log/datajoin-airflow
sudo bash deployment/scripts/setup-k3s.sh

# 2. Cria o .env.prod com seus valores reais
cp deployment/.env.prod.example deployment/.env.prod
$EDITOR deployment/.env.prod
```

> O `datajoin-app` precisa estar rodando ANTES do primeiro deploy do Airflow — o `init.sh` dele cria o database `airflow` e o user `airflow` no Postgres. Confira que `POSTGRES_PASSWORD` no `.env.prod` bate com o do `datajoin-app`.

## Deploy

```bash
# Manual:
bash deployment/scripts/deploy.sh                 # tag = latest
bash deployment/scripts/deploy.sh 7f3a912         # outra tag (ex: git sha)

# Automatizado: push em main → GitHub Actions roda o deploy.sh na VM via SSH
```

UI: `http://<VM_IP>:30080` (admin/admin).

## Operação

```bash
bash deployment/scripts/status.sh       # release + pods + svc + eventos
bash deployment/scripts/uninstall.sh    # remove o release (mantém PV/PVC)
bash deployment/scripts/uninstall.sh --purge   # limpa TUDO no k8s (logs no host ficam)

# Logs:
tail -f /var/log/datajoin-airflow/dag_id=<dag>/run_id=<run>/task_id=<task>/attempt=1.log
```

## Recursos (limits ≈ 6.3 GB; sobra ~6 GB pro datajoin-app)

| Componente | Request | Limit |
|---|---|---|
| api-server | 384Mi / 250m | 768Mi / 1000m |
| scheduler | 2Gi / 1000m | 4Gi / 2000m |
| dag-processor | 512Mi / 250m | 1Gi / 1000m |
| triggerer | 256Mi / 100m | 512Mi / 500m |

`AIRFLOW__CORE__PARALLELISM=8`, `MAX_ACTIVE_RUNS_PER_DAG=2`. Ajuste em `values/base.yaml` se precisar mais throughput.

## Como funciona o link com o Postgres do Docker

`setup-postgres-svc.sh` cria um `Service` ClusterIP **sem selector** + um `Endpoints` manual apontando pro IP do node (= IP da VM). Pods do k3s alcançam o host via esse IP, e o Postgres do Docker está exposto na 5432 do host. Resultado: dentro do cluster, `dj-postgres.airflow.svc.cluster.local:5432` resolve pra Postgres do datajoin-app.

Se a VM trocar de IP (raro, mas Hostinger pode fazer em remanejamento), basta rodar o deploy de novo — o script descobre o IP novo automaticamente.

## Como funciona o volume de logs

`setup-logs-volume.sh` cria um `PersistentVolume` com `hostPath: /var/log/datajoin-airflow` e um `PVC` `airflow-logs-pvc`. O chart monta esse PVC nos 4 pods (`logs.persistence.existingClaim`). Como é single-node, ReadWriteOnce funciona — todos os pods rodam no mesmo node.

Pra ver logs por fora do container, é só `tail -f` no path do host. Pra rotacionar, use `logrotate` na VM (não tem nada do Airflow gerenciando isso).

## Secrets do GitHub Actions

Configure em `Settings → Secrets and variables → Actions`:

```
HOSTINGER_HOST              # IP ou DNS da VM
HOSTINGER_USER              # ex: ubuntu
HOSTINGER_PORT              # ex: 22
HOSTINGER_SSH_KEY           # private key (ed25519) com acesso à VM

POSTGRES_HOST_IP            # vazio = autodescoberta
POSTGRES_PORT               # 5432
POSTGRES_USER               # airflow
POSTGRES_PASSWORD           # mesmo do datajoin-app
POSTGRES_DB                 # airflow

AIRFLOW_FERNET_KEY
AIRFLOW_SECRET_KEY
AIRFLOW_JWT_SECRET

SERVICE_TOKEN               # mesmo do datajoin-app
PLATFORM_API_URL            # http://<host_interno>:8000

DW_DB_USER
DW_DB_PASSWORD
DW_DB_NAME

AIRFLOW_LOGS_DIR            # /var/log/datajoin-airflow
```

## Troubleshooting

- **`QueuePool limit reached` no api-server**: aumente `sql_alchemy_pool_size`/`max_overflow` em `values/base.yaml` (`config.database`). Postgres do Docker tem `max_connections=100` por padrão.
- **`Could not connect to dj-postgres`**: rode `bash deployment/scripts/setup-postgres-svc.sh` pra recriar Service+Endpoints. Confira `kubectl get endpoints dj-postgres -n airflow` — `ENDPOINTS` deve mostrar o IP da VM.
- **`PermissionError` em `/opt/airflow/logs`**: `sudo chown -R 50000:0 /var/log/datajoin-airflow`.
- **DAG nova não aparece**: lembra que DAGs são baked na imagem. Sem rebuild, o pod não enxerga. Rode `deploy.sh` de novo.
