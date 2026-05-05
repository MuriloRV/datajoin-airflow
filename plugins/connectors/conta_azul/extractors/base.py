"""Contract base pros extractors do Conta Azul.

Cada entidade da API (pessoas, produtos, vendas, ...) implementa um
`Extractor` que sabe:
- O nome canonico da entidade (= nome da tabela em raw + chave do registry).
- O schema pydantic da resposta da API.
- DDL da tabela raw e SQL de UPSERT (template com {schema}).
- Como paginar a API e iterar os items (`fetch`).
- Como serializar 1 item pra dict do UPSERT (`serialize_for_upsert`).

Loader e' generico: pega o extractor pelo nome no registry, executa DDL,
itera fetch, faz UPSERT batch. Sem if-else por entidade.

Convencao do `NAME`:
- Bate com o nome da tabela em raw (ex: "pessoas" -> "conta_azul__pessoas").
- Bate com o nome usado em ENTITIES na DAG.
- pt-BR (matches API) — segue regra "API names atravessam raw/staging/curated".
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Iterator

from pydantic import BaseModel


class Extractor(ABC):
    """Contract abstrato. Subclasses sao stateless — instanciaveis sem args.

    Atributos de classe (definidos por subclass):
        NAME: chave do registry (ex: "pessoas").
        SCHEMA: classe pydantic da resposta (ex: Pessoa).
        DDL: SQL de CREATE TABLE com placeholder `{schema}` pro schema raw.
        UPSERT_SQL: SQL de INSERT ... ON CONFLICT, tambem com `{schema}`.

        INCREMENTAL_FIELD: nome do atributo do schema usado pra calcular
            high-water (ex: "data_alteracao"). None = full refresh sempre,
            sem watermark. Quando setado, loader le watermark antes do
            fetch, passa como filtro, e grava novo watermark = max(field)
            apos o fetch.
        INCREMENTAL_PARAM: nome do query param que filtra "alterado desde"
            no endpoint (ex: "data_alteracao_de"). Loader passa
            INCREMENTAL_PARAM=<watermark_value> em fetch().
    """

    NAME: ClassVar[str]
    SCHEMA: ClassVar[type[BaseModel]]
    DDL: ClassVar[str]
    UPSERT_SQL: ClassVar[str]

    # Watermark/incremental: None pras subclasses que sao lookup/full refresh.
    INCREMENTAL_FIELD: ClassVar[str | None] = None
    INCREMENTAL_PARAM: ClassVar[str | None] = None

    @abstractmethod
    def fetch(
        self,
        client: Any,
        **kwargs: Any,
    ) -> Iterator[BaseModel]:
        """Pagina a API e yield 1 item por vez (streaming).

        Kwargs convencionais (loader passa todos, extractor pega o que precisa):
            watermark: dict gravado pelo run anterior. None na primeira run.
                Subclass com INCREMENTAL_FIELD lê `watermark["value"]` pra
                montar filtro incremental no endpoint.
            dw_conn: psycopg.Connection — extractors que precisam ler
                tabelas raw ja existentes (ex: pessoas_detalhe le IDs de
                raw.conta_azul__pessoas).
            schema_raw: nome do schema raw do tenant (ex 'luminea_raw').
            tenant_slug: pra extractors que gerenciam watermark proprio.
            run_id: idem.

        Yieldd-by-item permite que o loader insira em stream sem
        materializar tudo na memoria — relevante pra entidades com
        milhoes de linhas.
        """

    @abstractmethod
    def serialize_for_upsert(self, item: BaseModel) -> dict[str, Any]:
        """Converte 1 item do schema pra dict de params do UPSERT_SQL."""

    def watermark_value(self, item: BaseModel) -> Any:
        """Extrai o valor incremental de 1 item. Default: getattr(INCREMENTAL_FIELD).

        Subclass pode override pra logica custom (ex: combinar 2 campos).
        Retorna None se INCREMENTAL_FIELD nao setado.
        """
        if self.INCREMENTAL_FIELD is None:
            return None
        return getattr(item, self.INCREMENTAL_FIELD, None)
