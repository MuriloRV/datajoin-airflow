"""Silencia deprecation warnings de bibliotecas 3rd-party que ainda não
adaptaram pra Airflow 3.x (notavelmente astronomer-cosmos 1.14.x).

Importar ANTES de qualquer import do Cosmos:

    import platform_warnings  # noqa: F401  -- side effect: filtra warnings
    from cosmos import DbtTaskGroup, ...

Por que este módulo existe:
- Cosmos 1.14.1 (latest em mai/2026) ainda usa `from airflow import DAG`,
  `airflow.exceptions.ParamValidationError`, `airflow.models.Variable.get`
  e `airflow.exceptions.AirflowSkipException` — tudo deprecated no Airflow
  3.x. O shim de compatibilidade emite `warnings.warn()` em cada import.
- Cosmos também emite `logger.warning()` toda vez que monta uma URI de
  Asset/Dataset (informativo: "URI standard mudou no Airflow 3.0.0").
  Em DAG com 10+ models dbt, vira centenas de linhas de ruído escondendo
  warnings reais nossos.
- Não dá pra fazer upgrade pra resolver — já estamos na latest.

Os filtros são por mensagem (regex específico) — NUNCA por categoria
solta — pra não mascarar deprecations futuras nossas. Quando Cosmos
publicar versão compatível com Airflow 3.x, basta remover este módulo
dos imports nas DAGs.
"""
from __future__ import annotations

import logging
import warnings

# Padrão: cada filtro casa com uma mensagem específica do shim de
# compat do Airflow 3.x. Filtrar por module=`cosmos\..*` nao funciona —
# a warning e' emitida de dentro do `airflow.__init__`, não do módulo
# que fez o import.
#
# Não restringimos por categoria porque o shim usa classes diferentes
# por mensagem (UserWarning, DeprecationWarning, DeprecatedImportWarning
# custom da Airflow). O default `category=Warning` cobre todas via
# subclassing — e o regex de mensagem garante que não silenciamos
# deprecations de outras origens.
_COSMOS_DEPRECATIONS = [
    r"Import 'DAG' directly from the airflow module is deprecated.*",
    r"airflow\.exceptions\.ParamValidationError is deprecated.*",
    r"airflow\.exceptions\.AirflowSkipException is deprecated.*",
    r"Using Variable\.get from `airflow\.models` is deprecated.*",
]

for _pattern in _COSMOS_DEPRECATIONS:
    warnings.filterwarnings("ignore", message=_pattern)


# Filtro de logging: o warning sobre Asset URI vem via logger.warning(),
# nao via warnings.warn() — entao warnings.filterwarnings nao alcanca.
# Filters em logging NAO se propagam dos pais pros filhos: precisam ser
# anexados no proprio logger que emite. logging.getLogger() e' idempotente
# (sempre retorna a mesma instancia por nome), entao anexar agora
# garante que quando cosmos.dataset importar, recebe o mesmo logger ja
# filtrado.
class _CosmosNoiseFilter(logging.Filter):
    """Suprime warnings informativos do Cosmos sobre Asset URI format."""

    _PATTERNS = (
        "Airflow 3.0.0 Asset (Dataset) URIs validation rules changed",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not any(p in msg for p in self._PATTERNS)


_filter = _CosmosNoiseFilter()
# Anexa nos loggers especificos que emitem o warning. Lista pequena e
# explicita — se Cosmos passar a emitir de outros lugares, adicione aqui.
for _name in ("cosmos.dataset",):
    logging.getLogger(_name).addFilter(_filter)
