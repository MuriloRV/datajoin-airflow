"""Extractor de pessoas (PF/PJ — clientes/fornecedores).

Endpoint API:  GET /v1/pessoas
Tabela raw:    <schema>.conta_azul__pessoas

Esta primeira versao mantem so colunas escalares — endereco/contato/
telefone aninhados ficam pra Fase 3 (precisa adicionar colunas JSONB
+ dbt staging com flatten via jsonb_array_elements).
"""
from __future__ import annotations

from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import Pessoa


class PessoasExtractor(Extractor):
    NAME = "pessoas"
    SCHEMA = Pessoa
    INCREMENTAL_FIELD = "data_alteracao"
    INCREMENTAL_PARAM = "data_alteracao_de"

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__pessoas" (
        id             UUID PRIMARY KEY,
        nome           TEXT NOT NULL,
        email          TEXT,
        documento      TEXT,
        tipo_pessoa    TEXT,
        telefone       TEXT,
        ativo          BOOLEAN,
        id_legado      BIGINT,
        uuid_legado    UUID,
        perfis         JSONB,
        data_criacao   TIMESTAMPTZ NOT NULL,
        data_alteracao TIMESTAMPTZ NOT NULL,
        loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__pessoas"
        (id, nome, email, documento, tipo_pessoa, telefone, ativo,
         id_legado, uuid_legado, perfis, data_criacao, data_alteracao)
    VALUES
        (%(id)s, %(nome)s, %(email)s, %(documento)s, %(tipo_pessoa)s,
         %(telefone)s, %(ativo)s, %(id_legado)s, %(uuid_legado)s,
         %(perfis)s::jsonb, %(data_criacao)s, %(data_alteracao)s)
    ON CONFLICT (id) DO UPDATE SET
        nome           = EXCLUDED.nome,
        email          = EXCLUDED.email,
        documento      = EXCLUDED.documento,
        tipo_pessoa    = EXCLUDED.tipo_pessoa,
        telefone       = EXCLUDED.telefone,
        ativo          = EXCLUDED.ativo,
        id_legado      = EXCLUDED.id_legado,
        uuid_legado    = EXCLUDED.uuid_legado,
        perfis         = EXCLUDED.perfis,
        data_alteracao = EXCLUDED.data_alteracao,
        loaded_at      = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[Pessoa]:
        """Pagina /v1/pessoas. API exige AMBOS data_alteracao_de + ate
        quando filtro temporal presente.

        Formato dos timestamps: ISO 8601 SEM microsegundos e SEM timezone
        (`YYYY-MM-DDTHH:MM:SS`). Outros formatos (com ms ou TZ Z) retornam
        500. Apenas date (`YYYY-MM-DD`) retorna 400.
        """
        from datetime import datetime
        from connectors.conta_azul.client import _mock_pessoas
        watermark = kwargs.get("watermark")
        extra_params = None
        if watermark and watermark.get("value"):
            # Strip microsegundos + timezone do watermark.
            wm = watermark["value"].split(".")[0].split("+")[0].rstrip("Z")
            extra_params = {
                "data_alteracao_de": wm,
                "data_alteracao_ate": datetime.utcnow().replace(microsecond=0).isoformat(),
            }
        yield from client.paginate_all(
            "/pessoas", Pessoa,
            extra_params=extra_params,
            mock_factory=_mock_pessoas,
        )

    def serialize_for_upsert(self, item: Pessoa) -> dict[str, Any]:
        import json
        from uuid import UUID
        # API usa zero-id/zero-uuid pra representar "sem legado".
        # Normaliza pra NULL — mais limpo em queries downstream.
        ZERO_UUID = UUID("00000000-0000-0000-0000-000000000000")
        uuid_legado = item.uuid_legado if item.uuid_legado and item.uuid_legado != ZERO_UUID else None
        return {
            "id": str(item.id),
            "nome": item.nome,
            "email": item.email,
            "documento": item.documento,
            "tipo_pessoa": item.tipo_pessoa,
            "telefone": item.telefone,
            "ativo": item.ativo,
            "id_legado": item.id_legado if item.id_legado else None,
            "uuid_legado": str(uuid_legado) if uuid_legado else None,
            "perfis": json.dumps(item.perfis) if item.perfis else None,
            "data_criacao": item.data_criacao,
            "data_alteracao": item.data_alteracao,
        }
