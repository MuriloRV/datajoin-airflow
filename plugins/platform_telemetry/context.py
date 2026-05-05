"""TelemetryContext — context manager que envolve uma task e publica job_run ao final."""
from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from platform_telemetry.client import PlatformClient


def _map_airflow_run_type(airflow_run_type: str | None) -> str:
    """Converte DagRun.run_type do Airflow para o enum da plataforma."""
    if not airflow_run_type:
        return "other"
    mapping = {
        "scheduled": "scheduled",
        "manual": "manual",
        "backfill": "backfill",
        "backfill_job": "backfill",
        "asset_triggered": "asset",
        "dataset_triggered": "asset",  # compat com nomenclatura antiga 2.x
    }
    return mapping.get(airflow_run_type, "other")


@dataclass
class TelemetryContext:
    tenant_id: str
    service_instance_id: str | None
    dag_id: str
    task_id: str
    run_id: str
    run_type: str
    # map_index: -1 para task nao-mapeada; 0..N-1 para mapped task instances
    # criadas via @task.expand(...). Incluido na chave de idempotencia
    # (external_run_id) pra evitar colisao quando varios items rodam em
    # paralelo com mesmo task_id.
    map_index: int = -1
    triggered_by: str | None = None
    client: PlatformClient = field(default_factory=PlatformClient)

    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    _started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    _extra_metrics: list[dict[str, Any]] = field(default_factory=list)

    # ─── Construtor idiomático a partir do context do Airflow ───
    @classmethod
    def from_airflow(
        cls,
        context: dict,
        tenant_id: str,
        service_instance_id: str | None = None,
    ) -> "TelemetryContext":
        dag_run = context["dag_run"]
        task_instance = context["task_instance"]
        airflow_run_type = getattr(dag_run, "run_type", None)
        triggered_by = None
        # Em 3.x: dag_run.triggered_by pode trazer o usuário que disparou manual.
        if airflow_run_type == "manual":
            triggered_by = getattr(dag_run, "triggered_by", None) or "airflow-ui"
        elif airflow_run_type == "scheduled":
            triggered_by = "airflow-scheduler"
        return cls(
            tenant_id=tenant_id,
            service_instance_id=service_instance_id,
            dag_id=task_instance.dag_id,
            task_id=task_instance.task_id,
            run_id=dag_run.run_id,
            run_type=_map_airflow_run_type(airflow_run_type),
            map_index=getattr(task_instance, "map_index", -1),
            triggered_by=triggered_by,
        )

    # ─── API para uso dentro da task ───
    def add_rows_inserted(self, n: int) -> None:
        self.rows_inserted += n

    def add_rows_updated(self, n: int) -> None:
        self.rows_updated += n

    def add_rows_deleted(self, n: int) -> None:
        self.rows_deleted += n

    def emit(self, metric: str, value: float | int | Decimal, unit: str) -> None:
        """Evento de uso arbitrário (bytes_processed, api_calls, etc)."""
        self._extra_metrics.append({"metric": metric, "value": float(value), "unit": unit})

    # ─── context manager ───
    def __enter__(self) -> "TelemetryContext":
        # Publica o job_run como 'running' imediatamente (visibilidade pro cliente).
        self._send(status="running", finished_at=None, error=None)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        finished_at = datetime.now(timezone.utc)
        if exc_type is None:
            self._send(status="success", finished_at=finished_at, error=None)
        else:
            err = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))[:4000]
            self._send(status="failed", finished_at=finished_at, error=err)
        self._flush_extra_metrics()
        self.client.close()
        return False  # nunca engole a exceção

    # ─── internal ───
    def _send(self, *, status: str, finished_at: datetime | None, error: str | None) -> None:
        # Mapped tasks: inclui [map_index] no external_run_id pra cada
        # instance ser unica. Tasks nao-mapeadas mantem formato original
        # (backwards compatible).
        suffix = f"[{self.map_index}]" if self.map_index >= 0 else ""
        external_run_id = f"{self.run_id}::{self.task_id}{suffix}"
        payload = {
            "source": "airflow",
            "external_run_id": external_run_id,
            # dag_run_id separado de external_run_id permite agrupamento
            # de JobRuns numa SyncRun via (tenant_id, dag_id, dag_run_id).
            # Adicionado 2026-05-03 (Phase 2 do master_release_plan).
            "dag_run_id": self.run_id,
            "service_instance_id": self.service_instance_id,
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "status": status,
            "run_type": self.run_type,
            "triggered_by": self.triggered_by,
            "started_at": self._started_at.isoformat(),
            "finished_at": finished_at.isoformat() if finished_at else None,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_deleted": self.rows_deleted,
            "error_message": error,
        }
        try:
            self.client.upsert_job_run(self.tenant_id, payload)
        except Exception:
            # Telemetria NUNCA deve quebrar a task. Só registra no log do Airflow.
            import logging

            logging.exception("telemetry upsert_job_run failed")

    def _flush_extra_metrics(self) -> None:
        for m in self._extra_metrics:
            try:
                self.client.emit_usage(self.tenant_id, m)
            except Exception:
                import logging

                logging.exception("telemetry emit_usage failed (metric=%s)", m.get("metric"))
