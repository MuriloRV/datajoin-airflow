"""DAG luminea__conta_azul_etl — pipeline completo Conta Azul.

Pipeline em 3 etapas sequenciais:
    1. extract_entity_to_raw     : Python (mapped task)  -> luminea_raw.conta_azul__*
    2. dbt_staging_curated       : Cosmos                -> luminea_staging + luminea_curated
    3. dbt_delivery              : Cosmos                -> luminea_delivery

Visibility com Cosmos: cada model dbt vira 1 task individual no Graph
view do Airflow. Tests viram tasks separadas. Lineage do dbt
(`{{ ref('...') }}`, `{{ source('...') }}`) vira lineage do Airflow —
paralelismo automatico onde nao ha dependencia.

Selecao de entidades por tenant: a constante `ENTITIES` define quais
entidades a DAG extrai. Mudanca = PR no codigo (operador/admin), nao
config no portal — decisao consciente pra evitar drift entre tenants.
Linhas comentadas dao visibilidade do que existe e esta desabilitado.

Esta DAG e' DECLARATIVA — so config, constantes e a orquestracao.
Toda logica de extract/load vive em
`plugins/connectors/conta_azul/loader.py` + `extractors/`.
Modelagem dbt em `dbt/projects/luminea/models/`.

Versionamento da lib: ver plugins/connectors/conta_azul/README.md.
"""
from __future__ import annotations

import os
from datetime import datetime
from functools import partial

# Side effect: silencia deprecations da Cosmos 1.14.x x Airflow 3.x.
# Tem que vir ANTES do import de `cosmos` pra os filtros estarem
# instalados antes do shim de compat emitir os warnings.
import platform_warnings  # noqa: F401
from airflow.sdk import Asset, chain, dag, task
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)

from connectors.conta_azul import ContaAzulClient, load_entity_to_raw
from platform_telemetry import (
    TelemetryContext,
    dbt_model_callback,
    dw_connection,
    resolve_tenant_by_slug,
    sync_run_finalize,
    sync_run_start,
)

# ─────────────── Configs do tenant ───────────────
TENANT_SLUG = "luminea"
DAG_ID = f"{TENANT_SLUG}__conta_azul_etl"
RAW_SCHEMA = f"{TENANT_SLUG}_raw"
DBT_PROJECT_DIR = f"/opt/dbt/projects/{TENANT_SLUG}"
# Connection do Airflow com credenciais OAuth2 do Conta Azul.
# Convencao: <slug>__<source>. Criar via UI: Admin > Connections.
# Documentacao detalhada em plugins/connectors/conta_azul/README.md.
CONN_ID = f"{TENANT_SLUG}__conta_azul"
# Mock=True so quando explicito (dev/CI sem creds reais). Em prod nunca.
USE_MOCK = os.environ.get("CONTA_AZUL_MOCK", "0") == "1"

# Entidades extraidas neste tenant. Cada string e' chave do registry
# em connectors.conta_azul.extractors.EXTRACTORS — vira 1 task mapeada
# no Airflow + 1 tabela em raw `conta_azul__<entity>`.
#
# Adicionar/remover = editar este array + abrir PR. Ate o cliente pedir,
# manter linhas comentadas pra visibilidade do que esta disponivel.
ENTITIES: list[str] = [
    # Pessoas
    "pessoas",
    "conta_conectada",                     # info do tenant (1 row)
    # Catalogo
    "produtos",
    "produto_categorias",
    "produto_ncm",
    "produto_cest",
    "produto_unidades_medida",
    "produto_ecommerce_marcas",             # 0 rows sem integracao e-com
    "servicos",
    # Financeiro — master data
    "categorias_financeiras",
    "categorias_dre",
    "centros_de_custo",
    "contas_financeiras",
    # Financeiro — eventos
    "contas_a_receber",                     # incremental + window
    "contas_a_pagar",                       # incremental + window
    "transferencias",                       # window 5a
    "saldo_inicial",                        # window 1a
    "eventos_alteracoes",                   # CDC (incremental)
    # Comercial
    "vendedores",
    "vendas",                               # incremental
    "contratos",                            # window 5a
    # Fiscal
    "notas_servico",                        # NFS-e (window 15d)
    "notas_fiscais",                        # NFe (window 30d)
]

# Entities que dependem de raw populado de outras entities — rodam DEPOIS.
# Le IDs de raw.<source> e faz N+1 nos endpoints de detalhe.
# Inclui:
#   - pessoas_detalhe       <- pessoas
#   - parcelas_detalhe      <- contas_a_receber + contas_a_pagar
#   - vendas_detalhe        <- vendas
#   - vendas_itens          <- vendas
#   - notas_fiscais_itens   <- notas_fiscais (chave_acesso)
#   - contratos_detalhe     <- contratos
#   - produtos_detalhe      <- produtos
#   - servicos_detalhe      <- servicos
#   - saldo_atual           <- contas_financeiras (snapshot diario)
ENTITIES_DEPENDENT: list[str] = [
    "pessoas_detalhe",
    "parcelas_detalhe",
    "vendas_detalhe",
    "vendas_itens",
    "notas_fiscais_itens",
    "contratos_detalhe",
    "produtos_detalhe",
    "servicos_detalhe",
    "saldo_atual",
]

# Entities que dependem das DEPENDENTES (cadeia de 2 niveis de N+1).
# Inclui:
#   - baixas        <- parcelas_detalhe (parcela_id -> /parcelas/{id}/baixas)
#   - cobrancas     <- parcelas_detalhe + contas_a_receber (scan de cobranca_ids no jsonb)
ENTITIES_TRANSITIVE: list[str] = [
    "baixas",
    "cobrancas",
]

# Cosmos lê o profiles.yml do proprio projeto dbt — preserva o macro
# generate_schema_name e demais customizacoes do tenant. Alternativa
# (profile_mapping com Connection do Airflow) ficaria mais acoplada.
PROFILE_CONFIG = ProfileConfig(
    profile_name=TENANT_SLUG,
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
)

# Callback que reporta 1 JobRun por model dbt no DB datajoin. Tolerante
# a falhas (try/except interno + log warning) — telemetria nao derruba
# a DAG. Anexado em both success e failure pra capturar models que
# quebraram tambem.
_DBT_MODEL_CB = partial(dbt_model_callback, tenant_slug=TENANT_SLUG)

# Callbacks DAG-level pra publicar SyncRun (rollup). Tolerante a falhas
# internamente — telemetria nunca derruba a DAG.
_SYNC_FINALIZE_SUCCESS = partial(
    sync_run_finalize, tenant_slug=TENANT_SLUG, status="success"
)
_SYNC_FINALIZE_FAILED = partial(
    sync_run_finalize, tenant_slug=TENANT_SLUG, status="failed"
)


@dag(
    dag_id=DAG_ID,
    # Sem schedule no momento — disparo somente manual via portal/Airflow UI.
    # Quando reativar agendamento, voltar pra "0 4 * * *" (diario 4am UTC).
    schedule=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tenant:luminea", "kind:etl", "source:conta_azul"],
    doc_md=__doc__,
    on_success_callback=_SYNC_FINALIZE_SUCCESS,
    on_failure_callback=_SYNC_FINALIZE_FAILED,
)
def luminea__conta_azul_etl():
    # Primeira task: cria a SyncRun pai. Airflow 3.x nao tem hook estavel
    # de "DAG comecou" — fazer via task de abertura e' a forma robusta.
    @task(retries=2)
    def open_sync_run(**context) -> None:
        sync_run_start(context, tenant_slug=TENANT_SLUG)

    @task(retries=3, retry_exponential_backoff=True)
    def ensure_token_fresh() -> None:
        """Refresca proativamente o access_token do Conta Azul.

        Conta Azul rotaciona o refresh_token a cada chamada (uso unico).
        Se duas tasks paralelas tentassem refrescar simultaneamente, a
        segunda receberia invalid_grant — race condition. Esta task
        roda ANTES do fan-out paralelo, no unico subprocess que tem
        permissao pra refrescar (allow_refresh=True). Todas as tasks de
        extract herdam o AT fresco da Connection com allow_refresh=False
        — nunca tentam refrescar, falham loud se AT vencer no meio.

        Margem 50min: AT tem TTL 1h, entao apos refresh aqui sobra ~10min
        de folga mesmo se a DAG inteira rodar. DAGs > 1h precisam de
        ajuste (rodar essa task de novo, ou aumentar a margem).
        """
        client = ContaAzulClient.from_airflow_connection(
            CONN_ID, mock=USE_MOCK, allow_refresh=True
        )
        client.ensure_token_fresh(min_remaining_minutes=50)

    @task(
        retries=3,
        retry_exponential_backoff=True,
        # Cap concurrencia das tasks mapeadas: rate limit do Conta Azul
        # e' ~10 req/s. Com 6+ entities expandindo simultaneamente, hit
        # garantido. 2 paralelas deixam folga pra cada uma usar ate 5 req/s.
        max_active_tis_per_dag=2,
    )
    def extract_entity_to_raw(entity_name: str, **context) -> dict:
        """1) Extrai 1 entidade do Conta Azul e materializa em luminea_raw.

        Mapeada por entity_name via .expand(entity_name=ENTITIES) — Airflow
        cria N task instances paralelas (1 por entity). Failure isolada
        por task: se `vendas` falhar, `pessoas` ainda termina e dbt segue
        com o que tem.
        """
        import logging
        log = logging.getLogger("conta_azul.dag")
        log.info("[%s] resolving tenant %s", entity_name, TENANT_SLUG)
        tenant = resolve_tenant_by_slug(TENANT_SLUG)
        log.info("[%s] tenant resolved: id=%s", entity_name, tenant["id"])

        log.info("[%s] entering TelemetryContext (POST /job-runs status=running)", entity_name)
        with TelemetryContext.from_airflow(context, tenant_id=tenant["id"]) as tele:
            log.info("[%s] TelemetryContext OK; building ContaAzulClient (mock=%s)", entity_name, USE_MOCK)
            # Le credenciais OAuth2 da Connection `luminea__conta_azul`.
            # CONTA_AZUL_MOCK=1 forca mock pra dev local sem creds.
            # allow_refresh=False: tasks paralelas nao podem refrescar
            # (refresh_token e' uso unico — race garantida com 2 paralelas).
            # Refresh acontece APENAS na task ensure_token_fresh upstream.
            client = ContaAzulClient.from_airflow_connection(
                CONN_ID, mock=USE_MOCK, allow_refresh=False
            )
            log.info("[%s] client built; opening DW connection", entity_name)
            with dw_connection() as conn:
                log.info("[%s] DW conn OK; calling load_entity_to_raw", entity_name)
                rows = load_entity_to_raw(
                    client, conn, RAW_SCHEMA, entity_name,
                    tenant_slug=TENANT_SLUG,
                    run_id=context["run_id"],
                )
                log.info("[%s] load_entity_to_raw returned rows=%d", entity_name, rows)
            tele.add_rows_inserted(rows)
            log.info("[%s] returning from task with rows=%d", entity_name, rows)
            return {
                "tenant_id": tenant["id"],
                "entity": entity_name,
                "rows": rows,
            }

    # 2) Cosmos expande staging + curated em tasks individuais
    # (1 por model + 1 por test). Filtro via tags definidas no
    # dbt_project.yml. Callback reporta 1 JobRun por model.
    dbt_staging_curated = DbtTaskGroup(
        group_id="dbt_staging_curated",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=PROFILE_CONFIG,
        execution_config=ExecutionConfig(),  # local: usa dbt-core instalado no container
        render_config=RenderConfig(
            select=["tag:staging", "tag:curated"],
        ),
        default_args={
            "on_success_callback": _DBT_MODEL_CB,
            "on_failure_callback": _DBT_MODEL_CB,
        },
    )

    # 3) Delivery — agregados finais. Roda apenas apos staging+curated OK.
    dbt_delivery = DbtTaskGroup(
        group_id="dbt_delivery",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=PROFILE_CONFIG,
        execution_config=ExecutionConfig(),
        render_config=RenderConfig(
            select=["tag:delivery"],
        ),
        default_args={
            "on_success_callback": _DBT_MODEL_CB,
            "on_failure_callback": _DBT_MODEL_CB,
        },
    )

    open_sync = open_sync_run()
    token_fresh = ensure_token_fresh()
    extract = extract_entity_to_raw.expand(entity_name=ENTITIES)
    # Dependentes rodam APOS as primarias estarem em raw.
    extract_dependent = extract_entity_to_raw.override(
        task_id="extract_dependent_to_raw",
    ).expand(entity_name=ENTITIES_DEPENDENT)
    # Transitivas rodam APOS as dependentes (cadeia de 2 niveis de N+1):
    # ex: baixas le parcela_ids de raw.parcelas_detalhe.
    extract_transitive = extract_entity_to_raw.override(
        task_id="extract_transitive_to_raw",
    ).expand(entity_name=ENTITIES_TRANSITIVE)
    chain(
        open_sync,
        token_fresh,
        extract,
        extract_dependent,
        extract_transitive,
        dbt_staging_curated,
        dbt_delivery,
    )


luminea__conta_azul_etl()
