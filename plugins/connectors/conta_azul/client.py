"""HTTP client da API do Conta Azul (v3 OAuth2).

Encapsula:
- OAuth2 com refresh_token rotativo (Conta Azul gera refresh novo a cada
  renovação — temos que persistir de volta na Connection).
- Retry com backoff exponencial em 429/5xx/timeout/network.
- Respeito ao header Retry-After em 429.
- Refresh automático em 401 + 1 retry da request original.
- Paginação no endpoint /v1/pessoas (offset-based: pagina + tamanho_pagina).

Tudo isolado neste arquivo — sem lib compartilhada com outros conectores.

Connection no Airflow esperada:
    Conn Id:   luminea__conta_azul     (convencao <slug>__<source>)
    Conn Type: HTTP
    Login:     <client_id>
    Password:  <client_secret>
    Extra:     {
        "refresh_token": "<refresh>",
        "access_token": "<atual>",                       (opcional, gera-se na 1a chamada)
        "access_token_expires_at": "2026-05-01T12:00Z"   (opcional, ISO8601)
    }

Doc oficial: https://developers.contaazul.com/auth
- Token endpoint: POST https://auth.contaazul.com/oauth2/token
- Auth do token:  Basic base64(client_id:client_secret) no header Authorization
- API base:       https://api-v2.contaazul.com/v1/
- API auth:       Bearer <access_token>
- access_token:   1 hora
- refresh_token:  2 semanas, USO UNICO (rotaciona a cada refresh)
- Rate limit:     ~50/min, ~10/s — 429 com Retry-After
- Endpoint usado: GET /v1/pessoas?pagina=N&tamanho_pagina=M
"""
from __future__ import annotations

import base64
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from uuid import uuid4

import httpx
from pydantic import BaseModel

from connectors.conta_azul.schemas import (
    CategoriaFinanceira,
    CentroDeCusto,
    ContaFinanceira,
    Contrato,
    EventoFinanceiro,
    NotaFiscal,
    NotaServico,
    Pessoa,
    PessoaDetalhe,
    Produto,
    Servico,
    Venda,
    Vendedor,
)


log = logging.getLogger(__name__)

# ───────── Endpoints oficiais (doc Conta Azul) ─────────
_TOKEN_URL = "https://auth.contaazul.com/oauth2/token"
_BASE_URL = "https://api-v2.contaazul.com/v1"

# ───────── Política de retry ─────────
# Cap: 8 tentativas com backoff exponencial base=2s, teto 120s.
# Total worst-case: 2+4+8+16+32+64+120+120 = 366s.
# Aumentamos o numero de tentativas e o teto pra absorver janelas longas
# de 429 da Conta Azul (ja vimos /servico ficar throttled por > 60s).
_MAX_ATTEMPTS = 8
_BACKOFF_BASE_S = 2
_BACKOFF_MAX_S = 120
_DEFAULT_TIMEOUT_S = 30

# Throttle entre requests pra ficar bem abaixo do limite (~10 req/s).
# 250ms = 4 req/s — folga grande, evita 429 mesmo em paginacao continua.
# Aplicado em TODA request (apos resposta), inclusive em runs paralelas
# do mesmo processo (sleep e' por chamada).
_THROTTLE_S = 0.25

# Headers que tratamos especialmente.
_RETRY_AFTER_HEADER = "Retry-After"


class ContaAzulAuthError(RuntimeError):
    """OAuth2 falhou — refresh_token expirado/revogado, credenciais erradas.
    Não retentável — admin precisa atualizar a Connection."""


class ContaAzulApiError(RuntimeError):
    """API retornou erro definitivo (4xx exceto 401/429). Stack trace inclui
    status code + body pra debug."""


class ContaAzulClient:
    """Cliente HTTP para a API do Conta Azul (uma instância por tenant).

    Uso tipico:
        client = ContaAzulClient.from_airflow_connection("luminea__conta_azul")
        for pessoa in client.list_pessoas():
            ...

    Em modo mock (dev/CI sem creds):
        client = ContaAzulClient(mock=True)
    """

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        refresh_token: str | None = None,
        access_token: str | None = None,
        access_token_expires_at: datetime | None = None,
        *,
        mock: bool = False,
        conn_id: str | None = None,
        allow_refresh: bool = True,
    ):
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._access_token = access_token
        self._access_token_expires_at = access_token_expires_at
        self._mock = mock
        # conn_id eh guardado pra persistir o novo refresh_token de volta
        # na Connection do Airflow apos cada refresh (token rotation).
        # Sem conn_id, refresh ainda funciona em memoria mas a proxima
        # execucao da DAG vai falhar — refresh_token e' uso unico.
        self._conn_id = conn_id
        # allow_refresh=False: client levanta ContaAzulAuthError em vez de
        # tentar refrescar. Usado nas tasks paralelas de extract pra evitar
        # race no refresh_token (uso unico) — apenas a task `ensure_token_fresh`
        # do inicio da DAG deve refrescar.
        self._allow_refresh = allow_refresh

        if not mock and not (client_id and client_secret and refresh_token):
            raise ValueError(
                "ContaAzulClient sem mock exige client_id, client_secret e "
                "refresh_token. Use from_airflow_connection() pra carregar "
                "automaticamente da Connection."
            )

    # ────────────────── Construtores ──────────────────

    @classmethod
    def from_airflow_connection(
        cls, conn_id: str, *, mock: bool = False, allow_refresh: bool = False
    ) -> "ContaAzulClient":
        """Constrói client a partir de uma Connection do Airflow.

        Em mock=True, ignora a Connection (útil pra dev local sem creds).

        `allow_refresh` controla se o client pode chamar /oauth2/token quando
        o access_token expirar:
          - False (default): tasks paralelas usam — levantam erro em vez de
            tentar refrescar (evita race no refresh_token de uso unico).
          - True: a task `ensure_token_fresh` no inicio da DAG usa — eh a
            UNICA que pode refrescar. Single subprocess, sem race.
        """
        if mock:
            return cls(mock=True, allow_refresh=allow_refresh)

        # Import lazy: rodar tests fora do Airflow não pode quebrar.
        from airflow.sdk import Connection  # type: ignore[import-not-found]

        conn = Connection.get(conn_id)
        extra = conn.extra_dejson if hasattr(conn, "extra_dejson") else {}

        expires_raw = extra.get("access_token_expires_at")
        expires_at: datetime | None = None
        if expires_raw:
            try:
                expires_at = datetime.fromisoformat(
                    expires_raw.replace("Z", "+00:00")
                )
            except ValueError:
                log.warning(
                    "access_token_expires_at inválido na Connection %s — ignorando",
                    conn_id,
                )

        return cls(
            client_id=conn.login,
            client_secret=conn.password,
            refresh_token=extra.get("refresh_token"),
            access_token=extra.get("access_token"),
            access_token_expires_at=expires_at,
            conn_id=conn_id,
            allow_refresh=allow_refresh,
        )

    # ────────────────── API pública ──────────────────

    def paginate_all(
        self,
        endpoint: str,
        schema: type[BaseModel],
        *,
        page_size: int = 100,
        extra_params: dict[str, Any] | None = None,
        mock_factory: "Callable[..., list[BaseModel]] | None" = None,
    ):
        """Itera todas as paginas de `endpoint`, yield item-a-item.

        Parada inteligente — necessaria porque a API do Conta Azul tem
        endpoints que IGNORAM o param `pagina` e retornam os mesmos itens
        sempre (ex: /servico). Se confiarmos so em "batch vazio", loop
        infinito. Estrategias combinadas:

        1. Se a resposta inclui `itens_totais` (ou `totalItems`), para
           quando ja yieldou >= total. Cobre 99% dos endpoints.
        2. Se `len(batch) < page_size`, pagina parcial = ultima. Para.
        3. Se `batch` vazia, terminator classico. Para.
        4. Hard cap de 1000 paginas — safeguard absoluto contra bugs.

        Cobertura: extractors so chamam isso e nao se preocupam com
        paginacao. Nao precisam loopar manualmente.
        """
        if self._mock and mock_factory:
            yield from mock_factory(page=1, page_size=page_size)
            return
        if self._mock:
            return

        yielded = 0
        total_expected: int | None = None
        _MAX_PAGES = 1000

        for page in range(1, _MAX_PAGES + 1):
            params = {"pagina": page, "tamanho_pagina": page_size}
            if extra_params:
                params.update(extra_params)
            data = self._request("GET", endpoint, params=params)

            # Captura totalizador na 1a pagina (para no proximo check).
            # Tres convencoes vistas em endpoints da API v3:
            #   1. flat raiz: `itens_totais` / `totalItems` (maioria)
            #   2. flat raiz: `total_itens` (/v1/venda/busca)
            #   3. nested: `paginacao.total_itens` (/notas-fiscais e /notas-fiscais-servico)
            if total_expected is None:
                for total_key in ("itens_totais", "totalItems", "total_itens", "total"):
                    if total_key in data:
                        total_expected = data[total_key]
                        log.info(
                            "Conta Azul %s: %s=%s na pagina 1",
                            endpoint, total_key, total_expected,
                        )
                        break
                # Fallback nested: envelope `{itens, paginacao: {total_itens}}`.
                if total_expected is None and isinstance(data.get("paginacao"), dict):
                    total_expected = data["paginacao"].get("total_itens")
                    if total_expected is not None:
                        log.info(
                            "Conta Azul %s: paginacao.total_itens=%s na pagina 1",
                            endpoint, total_expected,
                        )

            # Reaproveita logica de envelope detection do list_paginated.
            items = self._extract_items(endpoint, data)

            if not items:
                log.info(
                    "Conta Azul %s: pagina %d vazia (terminator) — total yielded=%d",
                    endpoint, page, yielded,
                )
                return

            for raw in items:
                yield schema.model_validate(raw)
                yielded += 1

            # Parada por totalizador: API pode ignorar `pagina` e ficar
            # devolvendo os mesmos itens (ex: /servico). Nao confiar so
            # em batch vazio.
            if total_expected is not None and yielded >= total_expected:
                log.info(
                    "Conta Azul %s: yielded %d >= total %d, parando",
                    endpoint, yielded, total_expected,
                )
                return

            # Pagina parcial = ultima pagina (convencao de muitas APIs).
            if len(items) < page_size:
                log.info(
                    "Conta Azul %s: pagina %d parcial (%d < %d) — ultima",
                    endpoint, page, len(items), page_size,
                )
                return

        log.warning(
            "Conta Azul %s: hit MAX_PAGES=%d cap (yielded=%d, total_expected=%s) — "
            "provavelmente API com bug de paginacao",
            endpoint, _MAX_PAGES, yielded, total_expected,
        )

    def list_simple(
        self,
        endpoint: str,
        schema: type[BaseModel],
        *,
        extra_params: dict[str, Any] | None = None,
    ):
        """GET endpoint que retorna LISTA direta (sem envelope/paginacao).

        Ex: /venda/vendedores -> [{"id":"...","nome":"..."}]
        """
        if self._mock:
            return
        data = self._request("GET", endpoint, params=extra_params or {})
        if not isinstance(data, list):
            log.warning(
                "Conta Azul %s: list_simple esperava list, recebeu %s",
                endpoint, type(data).__name__,
            )
            return
        for raw in data:
            yield schema.model_validate(raw)

    def _extract_items(self, endpoint: str, data: dict) -> list[dict]:
        """Detecta envelope (itens|items|dados|data) — extraido pra reuso."""
        for key in ("itens", "items", "dados", "data"):
            if key in data:
                return data[key] or []
        log.warning(
            "Conta Azul %s: envelope sem chave conhecida "
            "(itens|items|dados|data). Keys recebidas: %s",
            endpoint,
            list(data.keys()),
        )
        return []

    def list_paginated(
        self,
        endpoint: str,
        schema: type[BaseModel],
        *,
        page: int = 1,
        page_size: int = 100,
        extra_params: dict[str, Any] | None = None,
        mock_factory: "Callable[..., list[BaseModel]] | None" = None,
    ) -> list[BaseModel]:
        """Retorna 1 pagina de items parseados pelo schema fornecido.

        Generico — usado por todos os extractors. Caller (extractor) itera
        chamando page=1, 2, ... ate receber lista vazia.

        Args:
            endpoint: path relativo a `_BASE_URL` (ex: "/pessoas").
            schema: pydantic class pra parsear cada item.
            page: numero da pagina (1-based).
            page_size: tamanho da pagina. CUIDADO: alguns endpoints exigem
                valores especificos (ex: /produtos so aceita 10/20/50/100/
                200/500/1000). Defaults a 100 que funciona universalmente.
            extra_params: query params adicionais (ex: filtros incrementais).
            mock_factory: funcao opcional retornando lista mock de items
                (usada quando self._mock=True). Se None e mock=True,
                retorna [] (extractor sem mock implementado).
        """
        if self._mock:
            return mock_factory(page=page, page_size=page_size) if mock_factory else []

        params = {"pagina": page, "tamanho_pagina": page_size}
        if extra_params:
            params.update(extra_params)

        data = self._request("GET", endpoint, params=params)

        # API retorna envelope com chave variavel conforme endpoint:
        # - itens (PT)  -> /categorias, /pessoas, /servico, /conta-financeira, ...
        # - items (EN)  -> /produtos
        # - dados/data  -> defensivo (nao confirmado em uso)
        # Distingue 3 casos:
        #   1. chave presente com lista: pagina com dados — itera
        #   2. chave presente com null/[]: pagina vazia (terminator) — retorna []
        #   3. nenhuma chave conhecida: schema mudou — log warning + retorna []
        items = None
        found_key = False
        for key in ("itens", "items", "dados", "data"):
            if key in data:
                items = data[key] or []
                found_key = True
                break
        if not found_key:
            log.warning(
                "Conta Azul %s: envelope sem chave conhecida "
                "(itens|items|dados|data). Keys recebidas: %s",
                endpoint,
                list(data.keys()),
            )
            return []

        # Parsing tolerante: 1o item ja loga schema cru pra debug se algo
        # quebrar. Com extra="allow" nos schemas, campos desconhecidos
        # nao quebram — so faltantes obrigatorios sim.
        try:
            return [schema.model_validate(item) for item in items]
        except Exception:
            if items:
                log.error(
                    "Conta Azul %s: falha parseando %s. Schema cru do "
                    "primeiro item: keys=%s, sample=%s",
                    endpoint,
                    schema.__name__,
                    list(items[0].keys()) if isinstance(items[0], dict) else "?",
                    json.dumps(items[0], ensure_ascii=False)[:1500],
                )
            raise

    def list_pessoas(
        self, *, page: int = 1, page_size: int = 100
    ) -> list[Pessoa]:
        """Retorna 1 pagina de pessoas (PF/PJ). Endpoint: /v1/pessoas."""
        return self.list_paginated(
            "/pessoas", Pessoa,
            page=page, page_size=page_size,
            mock_factory=_mock_pessoas,
        )

    def list_categorias_financeiras(
        self, *, page: int = 1, page_size: int = 100
    ) -> list[CategoriaFinanceira]:
        """Categorias financeiras do DRE. Endpoint: /v1/categorias."""
        return self.list_paginated(
            "/categorias", CategoriaFinanceira,
            page=page, page_size=page_size,
        )

    def list_centros_de_custo(
        self, *, page: int = 1, page_size: int = 100
    ) -> list[CentroDeCusto]:
        """Centros de custo. Endpoint: /v1/centro-de-custo."""
        return self.list_paginated(
            "/centro-de-custo", CentroDeCusto,
            page=page, page_size=page_size,
        )

    def list_contas_financeiras(
        self, *, page: int = 1, page_size: int = 100
    ) -> list[ContaFinanceira]:
        """Contas financeiras (bancos/caixas/cartoes). Endpoint: /v1/conta-financeira."""
        return self.list_paginated(
            "/conta-financeira", ContaFinanceira,
            page=page, page_size=page_size,
        )

    def list_produtos(
        self, *, page: int = 1, page_size: int = 100
    ) -> list[Produto]:
        """Produtos. Endpoint: /v1/produtos (plural).

        ATENCAO: page_size restrito a 10|20|50|100|200|500|1000. Default
        100 e' valido. Outros valores retornam 400.
        """
        return self.list_paginated(
            "/produtos", Produto,
            page=page, page_size=page_size,
        )

    def list_servicos(
        self, *, page: int = 1, page_size: int = 100
    ) -> list[Servico]:
        """Servicos. Endpoint: /v1/servico (singular)."""
        return self.list_paginated(
            "/servico", Servico,
            page=page, page_size=page_size,
        )

    def list_contratos(
        self, *, data_inicio: str, data_fim: str,
        page: int = 1, page_size: int = 100,
    ) -> list[Contrato]:
        """Contratos. Endpoint: /v1/contratos.

        Requer data_inicio + data_fim (string YYYY-MM-DD). Sem esses params,
        retorna 400. Janela tipica: 5 anos retroativos a hoje.
        """
        return self.list_paginated(
            "/contratos", Contrato,
            page=page, page_size=page_size,
            extra_params={"data_inicio": data_inicio, "data_fim": data_fim},
        )

    def get_single(self, endpoint: str) -> dict | None:
        """GET endpoint que retorna 1 objeto (sem envelope, sem paginacao).

        Ex: /pessoas/conta-conectada -> {razao_social, documento, ...}.
        Retorna o dict cru — caller valida via pydantic.

        Em mock=True retorna None (caller skip-yields).
        404 retorna None (recurso nao existe). Outros erros HTTP raise.
        """
        if self._mock:
            return None
        try:
            return self._request("GET", endpoint)
        except ContaAzulApiError as e:
            if "404" in str(e):
                log.warning("Conta Azul: %s 404 — recurso nao existe", endpoint)
                return None
            raise

    def get_contrato_detalhe(self, contrato_id: str) -> dict | None:
        """GET /v1/contratos/{id}."""
        if self._mock:
            return None
        try:
            return self._request("GET", f"/contratos/{contrato_id}")
        except ContaAzulApiError as e:
            if "404" in str(e):
                return None
            raise

    def get_produto_detalhe(self, produto_id: str) -> dict | None:
        """GET /v1/produtos/{id}."""
        if self._mock:
            return None
        try:
            return self._request("GET", f"/produtos/{produto_id}")
        except ContaAzulApiError as e:
            if "404" in str(e):
                return None
            raise

    def get_servico_detalhe(self, servico_id: str) -> dict | None:
        """GET /v1/servicos/{id} (fallback /v1/servico/{id})."""
        if self._mock:
            return None
        try:
            return self._request("GET", f"/servicos/{servico_id}")
        except ContaAzulApiError as e:
            if "404" in str(e):
                try:
                    return self._request("GET", f"/servico/{servico_id}")
                except ContaAzulApiError as e2:
                    if "404" in str(e2):
                        return None
                    raise
            raise

    def get_saldo_atual(self, conta_financeira_id: str) -> dict | None:
        """GET /v1/conta-financeira/{id}/saldo-atual."""
        if self._mock:
            return None
        try:
            return self._request(
                "GET", f"/conta-financeira/{conta_financeira_id}/saldo-atual"
            )
        except ContaAzulApiError as e:
            if "404" in str(e):
                log.warning(
                    "Conta Azul: /conta-financeira/%s/saldo-atual 404 — ignorando",
                    conta_financeira_id,
                )
                return None
            raise

    def get_cobranca(self, id_cobranca: str) -> dict | None:
        """GET /v1/financeiro/eventos-financeiros/contas-a-receber/cobranca/{id}."""
        if self._mock:
            return None
        try:
            return self._request(
                "GET",
                f"/financeiro/eventos-financeiros/contas-a-receber/cobranca/{id_cobranca}",
            )
        except ContaAzulApiError as e:
            if "404" in str(e):
                return None
            raise

    def get_venda_detalhe(self, venda_id: str) -> dict | None:
        """GET /v1/vendas/{id} (singular alias /v1/venda/{id} pode tb funcionar).

        Doc oficial expoe /v1/vendas/{id}. Tentamos esse path primeiro,
        com fallback pra /v1/venda/{id} caso retorne 404 — mesma quirk
        de /vendas vs /venda/busca observada no extractor de vendas.
        """
        if self._mock:
            return None
        try:
            return self._request("GET", f"/vendas/{venda_id}")
        except ContaAzulApiError as e:
            if "404" in str(e):
                # Fallback singular — algumas tenants ainda respondem em /venda/{id}.
                try:
                    return self._request("GET", f"/venda/{venda_id}")
                except ContaAzulApiError as e2:
                    if "404" in str(e2):
                        log.warning(
                            "Conta Azul: /vendas/%s e /venda/%s 404 — ignorando",
                            venda_id, venda_id,
                        )
                        return None
                    raise
            raise

    def list_venda_itens(self, venda_id: str) -> list[dict]:
        """GET /v1/vendas/{id}/itens. Fallback singular /v1/venda/{id}/itens."""
        if self._mock:
            return []
        try:
            data = self._request("GET", f"/vendas/{venda_id}/itens")
        except ContaAzulApiError as e:
            if "404" in str(e):
                try:
                    data = self._request("GET", f"/venda/{venda_id}/itens")
                except ContaAzulApiError as e2:
                    if "404" in str(e2):
                        return []
                    raise
            else:
                raise
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return self._extract_items(f"/vendas/{venda_id}/itens", data)
        return []

    def get_nota_fiscal_detalhe(self, chave_acesso: str) -> dict | None:
        """GET /v1/notas-fiscais/{chave} — detalhe completo da NFe (com itens, transportadora, impostos)."""
        if self._mock:
            return None
        try:
            return self._request("GET", f"/notas-fiscais/{chave_acesso}")
        except ContaAzulApiError as e:
            if "404" in str(e):
                log.warning(
                    "Conta Azul: /notas-fiscais/%s 404 — ignorando",
                    chave_acesso,
                )
                return None
            raise

    def list_parcelas_de_evento(self, evento_id: str) -> list[dict]:
        """GET /v1/financeiro/eventos-financeiros/{id_evento}/parcelas.

        Retorna lista crua de parcelas associadas a 1 evento financeiro
        (conta_a_receber ou conta_a_pagar). Devolve dict cru (sem schema)
        — caller usa apenas pra extrair os IDs e em seguida chamar
        get_parcela_detalhe().

        404 -> []. Outros erros propagam.
        """
        if self._mock:
            return []
        try:
            data = self._request(
                "GET", f"/financeiro/eventos-financeiros/{evento_id}/parcelas"
            )
        except ContaAzulApiError as e:
            if "404" in str(e):
                log.warning(
                    "Conta Azul: /eventos-financeiros/%s/parcelas 404 — ignorando",
                    evento_id,
                )
                return []
            raise
        # Pode vir como envelope ou lista direta — normaliza.
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return self._extract_items(
                f"/eventos-financeiros/{evento_id}/parcelas", data
            )
        return []

    def get_parcela_detalhe(self, parcela_id: str) -> dict | None:
        """GET /v1/financeiro/eventos-financeiros/parcelas/{id}.

        Retorna dict cru — caller valida via pydantic ParcelaDetalhe.
        Endpoint critico: e' aqui que vem `data_pagamento`, juros, multa,
        valor liquido e forma_pagamento por parcela.
        """
        if self._mock:
            return None
        try:
            return self._request(
                "GET", f"/financeiro/eventos-financeiros/parcelas/{parcela_id}"
            )
        except ContaAzulApiError as e:
            if "404" in str(e):
                log.warning(
                    "Conta Azul: /parcelas/%s 404 — ignorando", parcela_id
                )
                return None
            raise

    def list_baixas_de_parcela(self, parcela_id: str) -> list[dict]:
        """GET /v1/financeiro/eventos-financeiros/parcelas/{id}/baixas.

        Lista as baixas (pagamentos parciais ou totais) realizadas em 1
        parcela. Granularidade fina — uma parcela pode ter N baixas.
        404/sem baixas -> [].
        """
        if self._mock:
            return []
        try:
            data = self._request(
                "GET",
                f"/financeiro/eventos-financeiros/parcelas/{parcela_id}/baixas",
            )
        except ContaAzulApiError as e:
            if "404" in str(e):
                return []
            raise
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return self._extract_items(
                f"/parcelas/{parcela_id}/baixas", data
            )
        return []

    def get_pessoa_detalhe(self, pessoa_id: str) -> PessoaDetalhe | None:
        """GET /v1/pessoas/{id} — detalhe completo com nested.

        Retorna None em 404 (pessoa apagada entre lista e detalhe — race).
        """
        if self._mock:
            return None
        try:
            data = self._request("GET", f"/pessoas/{pessoa_id}")
        except ContaAzulApiError as e:
            if "404" in str(e):
                log.warning("Conta Azul: /pessoas/%s 404 — ignorando", pessoa_id)
                return None
            raise
        return PessoaDetalhe.model_validate(data)

    # ────────────────── OAuth2 ──────────────────

    def _ensure_access_token(self) -> str:
        """Devolve o access_token atual; refresca se preciso E permitido.

        Margem de 60s pra evitar TOCTOU: token recém-renovado pode ainda
        estar válido na hora da request, mas expira no servidor antes da
        resposta voltar — e aí 401 acontece.

        Quando `allow_refresh=False` (tasks paralelas de extract): levanta
        erro em vez de refrescar — refresh deve ter rodado na task
        `ensure_token_fresh` no inicio da DAG. Se o AT expirou no meio do
        fan-out, eh sinal de DAG > TTL do AT (1h) ou Connection com tokens
        invalidos — falhar loud eh melhor que entrar em race.
        """
        if self._access_token and self._access_token_expires_at:
            if self._access_token_expires_at > datetime.now(
                timezone.utc
            ) + timedelta(seconds=60):
                return self._access_token

        if not self._allow_refresh:
            raise ContaAzulAuthError(
                "access_token expirou e este client foi construido com "
                "allow_refresh=False. Possiveis causas: (1) DAG run durou "
                f"mais que o TTL do AT (1h); (2) Connection {self._conn_id!r} "
                "esta com tokens invalidos. Refresh so eh permitido na task "
                "`ensure_token_fresh` no inicio da DAG."
            )

        self._refresh_access_token()
        assert self._access_token  # acabou de ser setado
        return self._access_token

    def ensure_token_fresh(self, min_remaining_minutes: int = 50) -> None:
        """Refresca o access_token se faltar menos que `min_remaining_minutes`.

        Idempotente. Designed pra ser chamado UMA UNICA VEZ no inicio da DAG
        (task `ensure_token_fresh`), antes do fan-out paralelo das tasks de
        extract. Garante que todas as tasks downstream pegam um AT valido
        sem precisarem refrescar — eliminando race no refresh_token (de uso
        unico) entre subprocesses paralelos.

        Margem default de 50min pro AT (TTL 1h): se a DAG rodar em ate ~50min
        apos esta task, AT continua valido pro fim. DAG mais longa precisa
        rodar esta task de novo no meio (ou aumentar a margem).

        Requer client construido com allow_refresh=True. Sem isso, levanta
        erro porque _ensure_access_token bloqueia.
        """
        if self._mock:
            return
        if not self._allow_refresh:
            raise ContaAzulAuthError(
                "ensure_token_fresh requer allow_refresh=True. Use "
                "ContaAzulClient.from_airflow_connection(conn_id, allow_refresh=True)."
            )
        if self._access_token and self._access_token_expires_at:
            margin = timedelta(minutes=min_remaining_minutes)
            now = datetime.now(timezone.utc)
            if self._access_token_expires_at > now + margin:
                remaining = self._access_token_expires_at - now
                log.info(
                    "Conta Azul: AT ainda valido por %ss — skip refresh proativo",
                    int(remaining.total_seconds()),
                )
                return
        log.info(
            "Conta Azul: refresh proativo no inicio da DAG (margem=%dmin)",
            min_remaining_minutes,
        )
        self._refresh_access_token()

    def _refresh_access_token(self) -> None:
        """POST oauth2/token com grant_type=refresh_token.

        Conta Azul rotaciona refresh_token a CADA renovação — o response
        traz um refresh_token NOVO que precisa ser persistido. O antigo
        é invalidado imediatamente. Se a persistência falhar e a DAG
        rodar de novo, vai dar erro de auth.
        """
        if not (self._client_id and self._client_secret and self._refresh_token):
            raise ContaAzulAuthError(
                "Faltam credenciais (client_id/secret/refresh_token) — "
                "configure a Connection do Airflow."
            )

        basic = base64.b64encode(
            f"{self._client_id}:{self._client_secret}".encode("utf-8")
        ).decode("ascii")

        log.info("Conta Azul: refreshing access_token (conn_id=%s)", self._conn_id)

        try:
            resp = httpx.post(
                _TOKEN_URL,
                headers={
                    "Authorization": f"Basic {basic}",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                },
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self._refresh_token,
                },
                timeout=_DEFAULT_TIMEOUT_S,
            )
        except httpx.HTTPError as e:
            raise ContaAzulAuthError(
                f"Falha de rede no refresh_token: {e}"
            ) from e

        # 400/401 do oauth2 = refresh_token expirado/revogado. Não retenta.
        if resp.status_code in (400, 401):
            raise ContaAzulAuthError(
                f"Refresh token rejeitado ({resp.status_code}): {resp.text[:200]}. "
                f"Atualize a Connection {self._conn_id!r} no Airflow com novo refresh_token."
            )
        if resp.status_code >= 500:
            raise ContaAzulAuthError(
                f"Conta Azul auth indisponível ({resp.status_code}): {resp.text[:200]}"
            )
        resp.raise_for_status()

        body = resp.json()
        self._access_token = body["access_token"]
        # CRITICO: refresh_token novo. Substituir o antigo (uso unico).
        new_refresh = body.get("refresh_token")
        if new_refresh:
            self._refresh_token = new_refresh
        expires_in = int(body.get("expires_in", 3600))
        self._access_token_expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=expires_in
        )

        # Persistir tokens novos na Connection. Se falhar, log warning —
        # a run atual continua, mas a proxima run vai precisar de admin
        # atualizar a Connection manualmente.
        self._persist_tokens()

    def _persist_tokens(self) -> None:
        """Salva access_token + refresh_token novos na Connection do Airflow
        via REST API.

        Airflow 3.0 bloqueia ORM direto de dentro de tasks (sandbox via
        Task SDK). REST API e' o caminho oficial pra mutar Connections em
        runtime. Login com admin/admin (override via env vars
        AIRFLOW_API_USER/PASSWORD).

        Crítico: Conta Azul gera refresh_token novo a cada refresh (uso
        único do antigo), então sem persistência a próxima execução vai
        falhar com 401.

        Best-effort: log warning em falha; run atual já tem tokens em
        memória.
        """
        if not self._conn_id:
            return

        try:
            import os

            airflow_url = os.environ.get(
                "AIRFLOW_API_URL", "http://airflow-apiserver:8080"
            )
            user = os.environ.get("AIRFLOW_API_USER", "admin")
            password = os.environ.get("AIRFLOW_API_PASSWORD", "admin")

            # 1) Login pra obter Bearer token da Airflow REST API.
            login = httpx.post(
                f"{airflow_url}/auth/token",
                json={"username": user, "password": password},
                timeout=10,
            )
            login.raise_for_status()
            token = login.json()["access_token"]

            # 2) GET connection atual (pra preservar outras chaves do extra).
            headers = {"Authorization": f"Bearer {token}"}
            get_resp = httpx.get(
                f"{airflow_url}/api/v2/connections/{self._conn_id}",
                headers=headers,
                timeout=10,
            )
            get_resp.raise_for_status()
            current = get_resp.json()

            extra_raw = current.get("extra") or "{}"
            try:
                extra = json.loads(extra_raw)
            except json.JSONDecodeError:
                extra = {}

            extra["refresh_token"] = self._refresh_token
            extra["access_token"] = self._access_token
            if self._access_token_expires_at:
                extra["access_token_expires_at"] = (
                    self._access_token_expires_at.isoformat()
                )

            # 3) PATCH connection com extra atualizado.
            # Airflow 3.0 valida o body via ConnectionBody (Pydantic) — exige
            # connection_id + conn_type mesmo com update_mask=extra. Sem isso
            # retorna 422. Reaproveita os valores do GET pra evitar drift.
            patch_resp = httpx.patch(
                f"{airflow_url}/api/v2/connections/{self._conn_id}",
                headers={**headers, "Content-Type": "application/json"},
                params={"update_mask": "extra"},
                json={
                    "connection_id": current.get("connection_id", self._conn_id),
                    "conn_type": current.get("conn_type", "http"),
                    "extra": json.dumps(extra),
                },
                timeout=10,
            )
            patch_resp.raise_for_status()
            log.info(
                "Conta Azul: tokens persistidos via REST API na Connection %s",
                self._conn_id,
            )
        except Exception as e:  # noqa: BLE001
            # Backlog #9 — antes era só log.warning silencioso. Agora:
            # 1) log.error com `alert` estruturado (Grafana/Datadog scraping
            #    pega via mesma pipeline do retries_exhausted alarm)
            # 2) backup local em /tmp pra recovery manual sem precisar
            #    re-autorizar OAuth do zero (refresh_token e' uso unico —
            #    sem o novo, a re-autorizacao do OAuth flow e' a unica saida)
            backup_path = self._write_token_backup()
            log.error(
                "refresh_token_persist_failed",
                extra={
                    "alert": "refresh_token_persist_failed",
                    "connector": "conta_azul",
                    "conn_id": self._conn_id,
                    "error": str(e)[:500],
                    "backup_path": backup_path,
                    "next_run_will_fail_401": True,
                    # Info acionavel pro time SRE: refresh_token e' uso unico.
                    # Recovery manual: copiar backup_path -> Connection extra.
                    "remediation": (
                        f"Copie o conteudo de {backup_path} pra extra da "
                        f"Connection {self._conn_id} (Airflow Admin > Connections)."
                    ),
                },
            )

    def _write_token_backup(self) -> str | None:
        """Escreve refresh_token + access_token num arquivo /tmp pra recovery
        manual. Permissions 0600 — best-effort, evita que outros users do
        host vejam o token.

        Retorna o path escrito ou None se falhar (raro).
        """
        try:
            import os
            import tempfile
            from datetime import datetime

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_conn = (self._conn_id or "unknown").replace("/", "_")
            path = os.path.join(
                tempfile.gettempdir(),
                f"conta_azul_token_backup_{safe_conn}_{ts}.json",
            )
            payload = {
                "conn_id": self._conn_id,
                "saved_at": datetime.now().isoformat(),
                "refresh_token": self._refresh_token,
                "access_token": self._access_token,
                "access_token_expires_at": (
                    self._access_token_expires_at.isoformat()
                    if self._access_token_expires_at
                    else None
                ),
                "_note": (
                    "BACKUP gerado por _persist_tokens apos falha. "
                    "refresh_token e' uso unico — copie pra Connection "
                    "extra antes da proxima DAG run pra evitar 401."
                ),
            }
            # tempfile.NamedTemporaryFile com delete=False pra controlar
            # permissions e nome explicitamente.
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(payload, f, indent=2)
            except Exception:
                os.close(fd)
                raise
            return path
        except Exception:  # noqa: BLE001
            log.exception("Falha ao gravar backup de token (best-effort)")
            return None

    # ────────────────── HTTP request com retry ──────────────────

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        timeout: float = _DEFAULT_TIMEOUT_S,
    ) -> dict:
        """Executa request autenticado com retry/backoff/rate-limit awareness.

        Política:
        - 401 (auth)         → refresh + 1 retry da mesma request (sem dobrar tentativa)
        - 429 (rate limit)   → respeita Retry-After header (ou 5s default)
        - 5xx (server)       → backoff exponencial 2/4/8/16/32s (cap 60s)
        - timeout/network    → backoff exponencial igual 5xx
        - 4xx outros         → ContaAzulApiError direto (não retenta — bug nosso)

        Cap total: _MAX_ATTEMPTS tentativas. Loga cada retry pra debug.
        """
        url = f"{_BASE_URL}{path}"
        token = self._ensure_access_token()

        already_refreshed_401 = False
        last_exc: Exception | None = None

        # Throttle preventivo entre requests do MESMO client. Ajuda em
        # paginacao continua e em casos onde o tenant ja consumiu quota
        # recente (ex: rerun apos falha).
        time.sleep(_THROTTLE_S)

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            log.info(
                "Conta Azul %s %s params=%s attempt=%d/%d",
                method, path, params, attempt, _MAX_ATTEMPTS,
            )
            try:
                resp = httpx.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Accept": "application/json",
                    },
                    timeout=timeout,
                )
                log.info(
                    "Conta Azul %s %s -> HTTP %d (attempt %d)",
                    method, path, resp.status_code, attempt,
                )
            except (httpx.TimeoutException, httpx.NetworkError) as e:
                last_exc = e
                wait = _backoff_seconds(attempt)
                log.warning(
                    "Conta Azul %s %s: %s (attempt %d/%d) — esperando %ds",
                    method,
                    path,
                    type(e).__name__,
                    attempt,
                    _MAX_ATTEMPTS,
                    wait,
                )
                time.sleep(wait)
                continue

            # 401: tentar 1x refresh + retry, mas APENAS se o client tem
            # permissao pra refrescar. Tasks paralelas de extract sao construidas
            # com allow_refresh=False — pra elas, 401 falha loud (sinal de
            # AT vencido durante o fan-out, deveria ter sido capturado pela
            # task `ensure_token_fresh` no inicio da DAG).
            if resp.status_code == 401 and not already_refreshed_401:
                if not self._allow_refresh:
                    raise ContaAzulAuthError(
                        f"{method} {path} retornou 401 e este client nao tem "
                        "permissao pra refresh. Provavel causa: AT expirou no "
                        "meio do DAG run. Refresh deve rodar na task "
                        "`ensure_token_fresh` no inicio da DAG."
                    )
                log.info(
                    "Conta Azul %s %s: 401, tentando refresh do access_token",
                    method,
                    path,
                )
                self._refresh_access_token()
                token = self._access_token  # type: ignore[assignment]
                already_refreshed_401 = True
                continue  # nao incrementa "perdeu" — refresh nao conta

            # 429: rate limit. Conta Azul nao manda Retry-After consistente.
            # Quando manda, respeita; quando nao, usa backoff exponencial igual
            # ao de 5xx (10/20/40/80/120s) — bem mais paciente que os 5s
            # default que o `_parse_retry_after` retorna.
            if resp.status_code == 429:
                header_wait = _parse_retry_after(resp.headers.get(_RETRY_AFTER_HEADER))
                # Se header veio com valor explicito (>5s), usa. Se veio o
                # fallback de 5s, prefere o backoff exponencial — mais agressivo
                # nas tentativas finais.
                wait = header_wait if header_wait > 5 else _backoff_seconds(attempt + 2)
                log.warning(
                    "Conta Azul %s %s: 429 rate limit, esperando %ds (attempt %d/%d)",
                    method,
                    path,
                    wait,
                    attempt,
                    _MAX_ATTEMPTS,
                )
                time.sleep(wait)
                continue

            # 5xx: server error. Backoff exponencial.
            if 500 <= resp.status_code < 600:
                wait = _backoff_seconds(attempt)
                log.warning(
                    "Conta Azul %s %s: %d server error (attempt %d/%d) — esperando %ds",
                    method,
                    path,
                    resp.status_code,
                    attempt,
                    _MAX_ATTEMPTS,
                    wait,
                )
                time.sleep(wait)
                continue

            # 4xx outros (exceto 401/429): bug do nosso lado. Não retenta.
            if 400 <= resp.status_code < 500:
                raise ContaAzulApiError(
                    f"{method} {path} retornou {resp.status_code}: "
                    f"{resp.text[:500]}"
                )

            # 2xx: sucesso. Parsea JSON.
            try:
                return resp.json()
            except json.JSONDecodeError as e:
                raise ContaAzulApiError(
                    f"{method} {path} retornou {resp.status_code} "
                    f"mas body não é JSON: {resp.text[:200]}"
                ) from e

        # Esgotou tentativas — propaga o último erro de rede ou levanta genérico.
        if last_exc:
            raise ContaAzulApiError(
                f"{method} {path}: max retries ({_MAX_ATTEMPTS}) excedido — último erro: {last_exc}"
            ) from last_exc
        raise ContaAzulApiError(
            f"{method} {path}: max retries ({_MAX_ATTEMPTS}) excedido (429/5xx persistente)"
        )


# ───────── helpers ─────────


def _backoff_seconds(attempt: int) -> int:
    """Exponencial 2^attempt com teto _BACKOFF_MAX_S."""
    return min(_BACKOFF_BASE_S ** attempt, _BACKOFF_MAX_S)


def _parse_retry_after(value: str | None) -> int:
    """Header Retry-After pode ser segundos (`5`) ou data HTTP. Default 5s."""
    if not value:
        return 5
    try:
        return max(1, int(value))
    except ValueError:
        # Formato data HTTP — não tratamos hoje. Volta 5s defensivo.
        return 5


# ═══════════════════════════════════════════════════════════════════
# MOCK DATA — usado em mock=True (dev/CI sem creds)
# ═══════════════════════════════════════════════════════════════════


def _mock_pessoas(*, page: int, page_size: int) -> list[Pessoa]:
    """Retorna fakes deterministicos pra dev/teste.

    Total de 25 pessoas mock; pagina de 100 retorna tudo na pagina 1
    e [] na pagina 2 (encerra o loop de paginacao do extractor).
    """
    if page > 1:
        return []

    now = datetime.now(timezone.utc)
    return [
        Pessoa(
            id=uuid4(),
            nome=f"Pessoa Mock {i:02d}",
            email=f"pessoa{i:02d}@mock.local" if i % 3 != 0 else None,
            documento=f"{10000000000 + i:011d}" if i % 2 == 0 else None,
            tipo_pessoa="Fisica" if i % 2 == 0 else "Juridica",
            telefone=f"+551199990{i:04d}" if i % 4 == 0 else None,
            data_criacao=now - timedelta(days=30 - i),
            data_alteracao=now - timedelta(hours=i),
        )
        for i in range(1, 26)
    ]
