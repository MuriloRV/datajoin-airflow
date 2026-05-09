"""TelemetryContext — context manager que envolve uma task e publica pipeline_task ao final."""
from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
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


def _resolve_triggered_by(dag_run: Any) -> str | None:
    """Normaliza `triggered_by` consistente em todos os callbacks (DAG, task, dbt).

    Garante que o portal sempre veja "airflow-scheduler" / "airflow-ui" /
    nome do user — sem variantes como "scheduler" puro.
    """
    run_type = getattr(dag_run, "run_type", None)
    if run_type == "manual":
        return getattr(dag_run, "triggered_by", None) or "airflow-ui"
    if run_type == "scheduled":
        return "airflow-scheduler"
    return None


@dataclass
class TelemetryContext:
    tenant_id: str
    # Prefixo airflow_* deixa explicita a fronteira de integracao: esses ids
    # vem literalmente do orquestrador e sao gravados como `airflow_*` nas
    # tabelas pipeline_runs / pipeline_tasks do portal.
    airflow_dag_id: str
    airflow_task_id: str
    airflow_run_id: str
    run_type: str
    # map_index: -1 para task nao-mapeada; 0..N-1 para mapped task instances
    # criadas via @task.expand(...). Incluido na chave de idempotencia
    # (external_run_id) pra evitar colisao quando varios items rodam em
    # paralelo com mesmo airflow_task_id.
    map_index: int = -1
    triggered_by: str | None = None
    client: PlatformClient = field(default_factory=PlatformClient)

    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    _started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # ─── Construtor idiomático a partir do context do Airflow ───
    @classmethod
    def from_airflow(
        cls,
        context: dict,
        tenant_id: str,
    ) -> "TelemetryContext":
        dag_run = context["dag_run"]
        task_instance = context["task_instance"]
        return cls(
            tenant_id=tenant_id,
            airflow_dag_id=task_instance.dag_id,
            airflow_task_id=task_instance.task_id,
            airflow_run_id=dag_run.run_id,
            run_type=_map_airflow_run_type(getattr(dag_run, "run_type", None)),
            map_index=getattr(task_instance, "map_index", -1),
            triggered_by=_resolve_triggered_by(dag_run),
        )

    # ─── API para uso dentro da task ───
    def add_rows_inserted(self, n: int) -> None:
        self.rows_inserted += n

    def add_rows_updated(self, n: int) -> None:
        self.rows_updated += n

    def add_rows_deleted(self, n: int) -> None:
        self.rows_deleted += n

    # ─── context manager ───
    def __enter__(self) -> "TelemetryContext":
        # Publica a pipeline_task como 'running' imediatamente (visibilidade pro cliente).
        self._send(status="running", finished_at=None, error=None)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        finished_at = datetime.now(timezone.utc)
        if exc_type is None:
            self._send(status="success", finished_at=finished_at, error=None)
        else:
            err = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))[:4000]
            self._send(status="failed", finished_at=finished_at, error=err)
        self.client.close()
        return False  # nunca engole a exceção

    # ─── internal ───
    def _send(self, *, status: str, finished_at: datetime | None, error: str | None) -> None:
        # Mapped tasks: inclui [map_index] no external_run_id pra cada
        # instance ser unica. Tasks nao-mapeadas mantem formato original.
        suffix = f"[{self.map_index}]" if self.map_index >= 0 else ""
        external_run_id = f"{self.airflow_run_id}::{self.airflow_task_id}{suffix}"
        payload: dict[str, Any] = {
            "source": "airflow",
            "external_run_id": external_run_id,
            "airflow_dag_id": self.airflow_dag_id,
            "airflow_run_id": self.airflow_run_id,
            "airflow_task_id": self.airflow_task_id,
            "status": status,
            "triggered_by": self.triggered_by,
            "started_at": self._started_at.isoformat(),
            "finished_at": finished_at.isoformat() if finished_at else None,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_deleted": self.rows_deleted,
            "error_message": error,
        }
        try:
            self.client.upsert_pipeline_task(self.tenant_id, payload)
        except Exception:
            # Telemetria NUNCA deve quebrar a task. Só registra no log do Airflow.
            import logging

            logging.exception("telemetry upsert_pipeline_task failed")
