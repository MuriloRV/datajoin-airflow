"""Helpers para rodar dbt e extrair telemetria do run_results.json.

Decisão de design: rodamos dbt via subprocess (não via dbtRunner em-process),
para isolar o ambiente (sys.path, logging) e permitir fácil captura de stdout.
"""
from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass
class DbtRunSummary:
    models_run: int = 0
    models_error: int = 0
    tests_passed: int = 0
    tests_failed: int = 0
    tests_warn: int = 0
    tests_skipped: int = 0
    rows_affected: int = 0
    elapsed_seconds: float = 0.0
    failures: list[str] = None  # unique_ids que falharam

    def __post_init__(self) -> None:
        if self.failures is None:
            self.failures = []


def run_dbt(
    args: Iterable[str],
    *,
    project_dir: str,
    profiles_dir: str | None = None,
    target: str = "dev",
) -> subprocess.CompletedProcess:
    """Executa `dbt <args>` via subprocess e retorna o CompletedProcess.

    Não levanta exceção em falha — deixa o caller decidir com base no returncode.
    """
    cmd = ["dbt", "--no-use-colors", *list(args), "--project-dir", project_dir, "--target", target]
    if profiles_dir:
        cmd += ["--profiles-dir", profiles_dir]
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def parse_run_results(project_dir: str) -> DbtRunSummary:
    """Lê `target/run_results.json` e retorna contadores para telemetria."""
    path = Path(project_dir) / "target" / "run_results.json"
    if not path.exists():
        return DbtRunSummary()

    data = json.loads(path.read_text(encoding="utf-8"))
    summary = DbtRunSummary()
    summary.elapsed_seconds = float(data.get("elapsed_time") or 0.0)

    for r in data.get("results", []):
        unique_id = r.get("unique_id", "")
        status = r.get("status")
        rt = unique_id.split(".")[0] if unique_id else ""
        adapter_resp = r.get("adapter_response") or {}
        rows = adapter_resp.get("rows_affected") or 0

        if rt == "model":
            if status == "success":
                summary.models_run += 1
                summary.rows_affected += int(rows)
            else:
                summary.models_error += 1
                summary.failures.append(unique_id)
        elif rt == "test":
            # status: 'pass' | 'fail' | 'warn' | 'error' | 'skipped'
            if status == "pass":
                summary.tests_passed += 1
            elif status == "warn":
                summary.tests_warn += 1
            elif status == "skipped":
                summary.tests_skipped += 1
            else:
                summary.tests_failed += 1
                summary.failures.append(unique_id)

    return summary


def emit_dbt_metrics(tele, summary: DbtRunSummary) -> None:
    """Publica métricas padronizadas via um TelemetryContext ativo."""
    tele.add_rows_updated(summary.rows_affected)
    tele.emit("dbt_models_run", summary.models_run, "count")
    tele.emit("dbt_models_error", summary.models_error, "count")
    tele.emit("dbt_tests_passed", summary.tests_passed, "count")
    tele.emit("dbt_tests_failed", summary.tests_failed, "count")
    tele.emit("dbt_tests_warn", summary.tests_warn, "count")
    tele.emit("dbt_tests_skipped", summary.tests_skipped, "count")
    tele.emit("dbt_elapsed", summary.elapsed_seconds, "seconds")


def dbt_model_callback(context, *, tenant_slug: str) -> None:
    """Callback Airflow pra criar 1 JobRun por model dbt na API datajoin.

    Anexar via `default_args.on_success_callback` e
    `default_args.on_failure_callback` num DbtTaskGroup do Cosmos:

        from functools import partial
        from platform_telemetry import dbt_model_callback

        cb = partial(dbt_model_callback, tenant_slug="luminea")
        DbtTaskGroup(
            ...,
            default_args={
                "on_success_callback": cb,
                "on_failure_callback": cb,
            },
        )

    Cria JobRun apenas pra tasks com sufixo `.run` (modelos) — tests
    viram metric agregada noutra task wrapper se necessario. Tolerante
    a falhas: log warning, nao quebra a DAG.
    """
    import logging

    ti = context.get("task_instance")
    if ti is None:
        return
    # Cosmos cria duas variantes de task_id pra modelos:
    # - Com tests:    'dbt_staging_curated.dim_conta_azul__customers.run'
    # - Sem tests:    'dbt_delivery.conta_azul__customers_summary_run'
    # Filtramos ambos e extraimos o nome do model.
    task_id = ti.task_id
    if task_id.endswith(".run"):
        model_name = task_id.split(".")[-2]
    elif task_id.endswith("_run"):
        model_name = task_id.split(".")[-1].removesuffix("_run")
    else:
        return  # nao e' uma task de model (test, group internal, etc)

    try:
        from platform_telemetry.client import PlatformClient
        from platform_telemetry.helpers import resolve_tenant_by_slug

        tenant = resolve_tenant_by_slug(tenant_slug)
        dag_run = context.get("dag_run")
        run_id = getattr(dag_run, "run_id", "unknown")
        run_type = getattr(dag_run, "run_type", "unknown")
        triggered_by = getattr(dag_run, "triggering_user_name", None) or "scheduler"

        # Airflow 3.x RuntimeTaskInstance nao expoe .duration —
        # calculamos via end_date - start_date (segundos).
        duration_ms = 0
        if ti.start_date and ti.end_date:
            duration_ms = int(
                (ti.end_date - ti.start_date).total_seconds() * 1000
            )

        # ti.state pode vir como TaskInstanceState enum em Airflow 3.
        # Comparacao com string ainda funciona via __eq__ do enum.
        state_str = str(ti.state).lower() if ti.state else "unknown"
        is_success = state_str == "success" or state_str.endswith(".success")

        # Bug fix 2026-05-03: dag_id estava ausente, quebrava agrupamento
        # de SyncRun. dag_run_id agora vai como campo proprio (separado de
        # external_run_id) pra linkar com sync_runs via (dag_id, dag_run_id).
        dag_id = getattr(dag_run, "dag_id", None)
        payload = {
            "source": "dbt",
            "dag_id": dag_id,
            "dag_run_id": run_id,
            # Upsert key (source, external_run_id, task_id) — combinacao garante
            # 1 row por model por dag_run, idempotente em retry.
            "external_run_id": run_id,
            "task_id": ti.task_id,
            "status": "success" if is_success else "failed",
            "run_type": run_type,
            "triggered_by": str(triggered_by),
            "started_at": ti.start_date.isoformat() if ti.start_date else None,
            "finished_at": ti.end_date.isoformat() if ti.end_date else None,
            "duration_ms": duration_ms,
            # rows_inserted=0 ainda — preencher requer parsear
            # target/run_results.json (TODO Phase 2.x).
            "rows_inserted": 0,
            "log_output": {"model": model_name, "task": ti.task_id},
        }

        client = PlatformClient()
        try:
            client.upsert_job_run(tenant["id"], payload)
        finally:
            client.close()
    except Exception as e:  # noqa: BLE001
        # Telemetria nunca deve derrubar a DAG. Log e segue.
        logging.warning(
            "dbt_model_callback falhou pra task=%s tenant=%s: %s",
            ti.task_id, tenant_slug, e,
        )
