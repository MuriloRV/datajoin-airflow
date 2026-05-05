"""Extractor de detalhe de pessoa (com nested arrays).

Endpoint API:  GET /v1/pessoas/{id}  (1 request por pessoa — N+1)
Tabela raw:    <schema>.conta_azul__pessoas_detalhe

Padrao **diferente** dos outros extractors: nao busca a lista direto da
API (isso o `pessoas` ja faz). Em vez disso le os IDs ja extraidos da
tabela `conta_azul__pessoas` no DW e busca o detalhe de cada um.

**N+1 com watermark:**
- Watermark = data_alteracao da pessoa mais recente cujo detalhe foi
  buscado.
- Na primeira run: busca detalhe de TODAS as pessoas. Para 42 pessoas,
  ~10s. Para 10k pessoas, ~40min com throttle 250ms — ainda viavel
  rodando 1x ao inicio.
- Runs seguintes: busca apenas detalhes das pessoas alteradas desde
  o ultimo watermark (filtro feito em memoria depois de ler raw.pessoas).

Diferente do contract base: este extractor precisa de ACESSO AO DW pra
ler os IDs. O loader passa o `conn` quando o extractor declara
`REQUIRES_DW_READ = True`.
"""
from __future__ import annotations

import json
import logging
from typing import Any, ClassVar, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import PessoaDetalhe


log = logging.getLogger(__name__)


class PessoasDetalheExtractor(Extractor):
    NAME = "pessoas_detalhe"
    SCHEMA = PessoaDetalhe
    INCREMENTAL_FIELD = "data_alteracao"  # propagado de raw.pessoas
    INCREMENTAL_PARAM = None  # filtro feito local (nao via API)

    # Sinaliza pro loader passar a conexao DW como argumento extra.
    REQUIRES_DW_READ: ClassVar[bool] = True

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__pessoas_detalhe" (
        id                       UUID PRIMARY KEY,
        nome                     TEXT NOT NULL,
        documento                TEXT,
        email                    TEXT,
        telefone_comercial       TEXT,
        telefone_celular         TEXT,
        tipo_pessoa              TEXT,
        nome_empresa             TEXT,
        rg                       TEXT,
        data_nascimento          TEXT,
        optante_simples_nacional BOOLEAN,
        orgao_publico            BOOLEAN,
        observacao               TEXT,
        codigo                   TEXT,
        ativo                    BOOLEAN,
        enderecos                JSONB,
        outros_contatos          JSONB,
        inscricoes               JSONB,
        pessoas_legado           JSONB,
        perfis                   JSONB,
        raw                      JSONB,
        loaded_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__pessoas_detalhe"
        (id, nome, documento, email, telefone_comercial, telefone_celular,
         tipo_pessoa, nome_empresa, rg, data_nascimento,
         optante_simples_nacional, orgao_publico, observacao, codigo, ativo,
         enderecos, outros_contatos, inscricoes, pessoas_legado, perfis, raw)
    VALUES
        (%(id)s, %(nome)s, %(documento)s, %(email)s,
         %(telefone_comercial)s, %(telefone_celular)s, %(tipo_pessoa)s,
         %(nome_empresa)s, %(rg)s, %(data_nascimento)s,
         %(optante_simples_nacional)s, %(orgao_publico)s, %(observacao)s,
         %(codigo)s, %(ativo)s,
         %(enderecos)s::jsonb, %(outros_contatos)s::jsonb,
         %(inscricoes)s::jsonb, %(pessoas_legado)s::jsonb,
         %(perfis)s::jsonb, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        nome                     = EXCLUDED.nome,
        documento                = EXCLUDED.documento,
        email                    = EXCLUDED.email,
        telefone_comercial       = EXCLUDED.telefone_comercial,
        telefone_celular         = EXCLUDED.telefone_celular,
        tipo_pessoa              = EXCLUDED.tipo_pessoa,
        nome_empresa             = EXCLUDED.nome_empresa,
        rg                       = EXCLUDED.rg,
        data_nascimento          = EXCLUDED.data_nascimento,
        optante_simples_nacional = EXCLUDED.optante_simples_nacional,
        orgao_publico            = EXCLUDED.orgao_publico,
        observacao               = EXCLUDED.observacao,
        codigo                   = EXCLUDED.codigo,
        ativo                    = EXCLUDED.ativo,
        enderecos                = EXCLUDED.enderecos,
        outros_contatos          = EXCLUDED.outros_contatos,
        inscricoes               = EXCLUDED.inscricoes,
        pessoas_legado           = EXCLUDED.pessoas_legado,
        perfis                   = EXCLUDED.perfis,
        raw                      = EXCLUDED.raw,
        loaded_at                = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[PessoaDetalhe]:
        """Le IDs de raw.conta_azul__pessoas filtrando por watermark, busca detalhe.

        Watermark vem de raw.pessoas.data_alteracao — o detail endpoint nao
        retorna data_alteracao consistentemente, entao trackeamos durante
        o fetch (self._latest_data_alt) e o loader chama watermark_value()
        que devolve esse valor (ignorando o item).

        Args:
            client: ContaAzulClient.
            watermark: dict do etl_watermarks (key 'value' = ISO timestamp da
                ultima pessoa cujo detalhe foi buscado).
            dw_conn: psycopg.Connection — passado pelo loader.
            schema_raw: ex 'luminea_raw' — onde mora conta_azul__pessoas.
        """
        watermark = kwargs.get("watermark")
        dw_conn = kwargs.get("dw_conn")
        schema_raw = kwargs.get("schema_raw")

        # Reset do tracker a cada fetch (extractor reusado entre runs em
        # warm processes — sem reset, watermark "carregaria" entre runs).
        self._latest_data_alt = None

        if dw_conn is None or schema_raw is None:
            log.error("PessoasDetalheExtractor: faltando dw_conn ou schema_raw")
            return

        # Filtro: data_alteracao > watermark. Na primeira run, busca tudo.
        wm_value = watermark.get("value") if watermark else None
        with dw_conn.cursor() as cur:
            if wm_value:
                cur.execute(
                    f'SELECT id::text, data_alteracao FROM "{schema_raw}".conta_azul__pessoas '
                    f'WHERE data_alteracao > %s ORDER BY data_alteracao',
                    (wm_value,),
                )
            else:
                cur.execute(
                    f'SELECT id::text, data_alteracao FROM "{schema_raw}".conta_azul__pessoas '
                    f'ORDER BY data_alteracao'
                )
            rows = cur.fetchall()

        log.info(
            "PessoasDetalhe: %d pessoas pra buscar detalhe (watermark=%s)",
            len(rows), wm_value,
        )
        for pid, data_alt in rows:
            detalhe = client.get_pessoa_detalhe(pid)
            if detalhe is None:
                continue
            # Track maior data_alteracao visto pra gravar como watermark.
            # rows ja vem ORDER BY data_alteracao, entao o ultimo bem-sucedido
            # e' o max — mas como pode haver 404 intermediario, comparamos.
            if self._latest_data_alt is None or (
                data_alt is not None and data_alt > self._latest_data_alt
            ):
                self._latest_data_alt = data_alt
            yield detalhe

    def watermark_value(self, item: PessoaDetalhe) -> Any:
        """Watermark = max(data_alteracao) de raw.pessoas visto no fetch.

        Ignora `item` (detail endpoint nao traz data_alteracao consistente)
        e devolve o valor trackeado pelo fetch. Loader chama isto pra cada
        item; sempre retorna o mesmo max ate o final do fetch.
        """
        return getattr(self, "_latest_data_alt", None)

    def serialize_for_upsert(self, item: PessoaDetalhe) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json")
        return {
            "id": str(item.id),
            "nome": item.nome,
            "documento": item.documento,
            "email": item.email,
            "telefone_comercial": item.telefone_comercial,
            "telefone_celular": item.telefone_celular,
            "tipo_pessoa": item.tipo_pessoa,
            "nome_empresa": item.nome_empresa,
            "rg": item.rg,
            "data_nascimento": item.data_nascimento,
            "optante_simples_nacional": item.optante_simples_nacional,
            "orgao_publico": item.orgao_publico,
            "observacao": item.observacao,
            "codigo": item.codigo,
            "ativo": item.ativo,
            "enderecos": json.dumps(item.enderecos) if item.enderecos else None,
            "outros_contatos": json.dumps(item.outros_contatos) if item.outros_contatos else None,
            "inscricoes": json.dumps(item.inscricoes) if item.inscricoes else None,
            "pessoas_legado": json.dumps(item.pessoas_legado) if item.pessoas_legado else None,
            "perfis": json.dumps(item.perfis) if item.perfis else None,
            "raw": json.dumps(raw_dict, default=str),
        }
