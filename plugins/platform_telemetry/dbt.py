"""Callback de telemetria pros models dbt rodados via Cosmos.

dbt e' tratado como ferramenta de modelagem dentro da DAG — cada model
vira 1 PipelineTask normal no backend, com `rows_updated` preenchido a
partir do `target/run_results.json` quando disponivel.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def _read_rows_affected(project_dir: str, model_name: str) -> int:
    """Le rows_affected do model em `<project_dir>/target/run_results.json`.

    Best-effort: Cosmos pode rodar dbt em diretorio temporario (sem deixar
    artefatos no project_dir) — nesse caso retorna 0 silenciosamente.
    """
    if not project_dir:
        return 0
    path = Path(project_dir) / "target" / "run_results.json"
    if not path.exists():
        return 0
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0
    for r in data.get("results", []):
        unique_id = r.get("unique_id", "")
        # unique_id formato: model.<project>.<model_name>
        if not unique_id.startswith("model."):
            continue
        if unique_id.rsplit(".", 1)[-1] != model_name:
            continue
        adapter_resp = r.get("adapter_response") or {}
        return int(adapter_resp.get("rows_affected") or 0)
    return 0


def dbt_model_callback(
    context, *, tenant_slug: str, project_dir: str | None = None
) -> None:
    """Callback Airflow pra criar 1 PipelineTask por model dbt.

    Anexar via `default_args.on_success_callback` e
    `default_args.on_failure_callback` num DbtTaskGroup do Cosmos:

        from functools import partial
        from platform_telemetry import dbt_model_callback

        cb = partial(dbt_model_callback, tenant_slug="luminea",
                     project_dir=DBT_PROJECT_DIR)
        DbtTaskGroup(
            ...,
            default_args={
                "on_success_callback": cb,
                "on_failure_callback": cb,
            },
        )

    Cria PipelineTask apenas pra tasks com sufixo `.run` (modelos) — tests
    e outras tasks internas sao ignoradas. Tolerante a falhas: log warning,
    nao quebra a DAG.
    """
    ti = context.get("task_instance")
    if ti is None:
        return
    # Cosmos cria duas variantes de airflow_task_id pra modelos:
    # - Com tests:    'dbt_staging_curated.dim_conta_azul__customers.run'
    # - Sem tests:    'dbt_delivery.conta_azul__customers_summary_run'
    airflow_task_id = ti.task_id
    if airflow_task_id.endswith(".run"):
        model_name = airflow_task_id.split(".")[-2]
    elif airflow_task_id.endswith("_run"):
        model_name = airflow_task_id.split(".")[-1].removesuffix("_run")
    else:
        return  # nao e' uma task de model (test, group internal, etc)

    try:
        from platform_telemetry.client import PlatformClient
        from platform_telemetry.context import _resolve_triggered_by
        from platform_telemetry.helpers import resolve_tenant_by_slug

        tenant = resolve_tenant_by_slug(tenant_slug)
        dag_run = context.get("dag_run")
        airflow_run_id = getattr(dag_run, "run_id", "unknown")
        airflow_dag_id = getattr(dag_run, "dag_id", None)

        # ti.state pode vir como TaskInstanceState enum em Airflow 3 — str() normaliza.
        state_str = str(ti.state).lower() if ti.state else "unknown"
        is_success = state_str == "success" or state_str.endswith(".success")

        rows_updated = _read_rows_affected(project_dir or "", model_name)

        payload = {
            "source": "dbt",
            "external_run_id": f"{airflow_run_id}::{airflow_task_id}",
            "airflow_dag_id": airflow_dag_id,
            "airflow_run_id": airflow_run_id,
            "airflow_task_id": airflow_task_id,
            "status": "success" if is_success else "failed",
            "triggered_by": _resolve_triggered_by(dag_run),
            "started_at": ti.start_date.isoformat() if ti.start_date else None,
            "finished_at": ti.end_date.isoformat() if ti.end_date else None,
            "rows_updated": rows_updated,
            "log_output": {"model": model_name},
        }

        client = PlatformClient()
        try:
            client.upsert_pipeline_task(tenant["id"], payload)
        finally:
            client.close()
    except Exception as e:  # noqa: BLE001
        # Telemetria nunca deve derrubar a DAG. Log e segue.
        logger.warning(
            "dbt_model_callback falhou pra task=%s tenant=%s: %s",
            ti.task_id, tenant_slug, e,
        )
