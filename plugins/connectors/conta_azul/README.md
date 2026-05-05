# Conector Conta Azul

Cliente OAuth2 + paginação para a API v3 do Conta Azul (`https://api-v2.contaazul.com/v1/`).

## Endpoints e padrões usados

| Item | Valor |
|---|---|
| **Token endpoint** | `POST https://auth.contaazul.com/oauth2/token` |
| **Auth do token** | `Basic base64(client_id:client_secret)` |
| **API base** | `https://api-v2.contaazul.com/v1/` |
| **API auth** | `Authorization: Bearer <access_token>` |
| **access_token TTL** | 1 hora |
| **refresh_token TTL** | 2 semanas, **uso único** (rotaciona a cada refresh) |
| **Paginação** | `?pagina=N&tamanho_pagina=M` (offset-based) |
| **Rate limit** | ~50/min, ~10/s — 429 com header `Retry-After` |

## Entidades extraídas

Cada linha vira `conta_azul__<entity>` em `<tenant>_raw`. Adicionar/remover em `extractors/__init__.py` (registry) + DAG (`ENTITIES`). Todas as 24 entidades têm pipeline completo: **raw → staging → curated**.

| Entity | Endpoint | Envelope | Estratégia | dbt curated |
|---|---|---|---|---|
| `pessoas` | `GET /v1/pessoas` | `itens` | **incremental** (`data_alteracao_de`) | `dim_conta_azul__pessoas` |
| `pessoas_detalhe` | `GET /v1/pessoas/{id}` (N+1) | objeto | **incremental** (filtro local sobre raw.pessoas) | alimenta `dim_pessoas` |
| `categorias_financeiras` | `GET /v1/categorias` | `itens` | full refresh — master data | `dim_conta_azul__categorias_financeiras` |
| `centros_de_custo` | `GET /v1/centro-de-custo` | `itens` | full refresh — master data | `dim_conta_azul__centros_de_custo` |
| `contas_financeiras` | `GET /v1/conta-financeira` | `itens` | full refresh — master data | `dim_conta_azul__contas_financeiras` |
| `produtos` | `GET /v1/produtos` (PLURAL) | `items` (EN) | **incremental** (`data_alteracao_de`) | `dim_conta_azul__produtos` |
| `servicos` | `GET /v1/servico` (singular) | `itens` | full refresh — sem `data_alteracao` no schema | `dim_conta_azul__servicos` |
| `vendedores` | `GET /v1/venda/vendedores` | lista direta | full refresh — master data | `dim_conta_azul__vendedores` |
| `contratos` | `GET /v1/contratos` (data_inicio + data_fim obrigatorios) | `itens` | window 5 anos | `dim_conta_azul__contratos` |
| `notas_servico` | `GET /v1/notas-fiscais-servico` (NFS-e, todos status) | `itens` + `paginacao` | window 15d (limite da API) | `fct_conta_azul__notas_servico` |
| `notas_fiscais` | `GET /v1/notas-fiscais` (NFe, so EMITIDA/CORRIGIDA_SUCESSO) | `itens` + `paginacao` | window 30d, 1 ano | `fct_conta_azul__notas_fiscais` |
| `contas_a_receber` | `GET /v1/financeiro/eventos-financeiros/contas-a-receber/buscar` | `itens` | **híbrido**: window 5a por `data_vencimento` (required) + `data_alteracao_de` (incremental) | `fct_conta_azul__contas_a_receber` |
| `contas_a_pagar` | `GET /v1/financeiro/eventos-financeiros/contas-a-pagar/buscar` | `itens` | **híbrido**: window 5a por `data_vencimento` (required) + `data_alteracao_de` (incremental) | `fct_conta_azul__contas_a_pagar` |
| `vendas` | `GET /v1/venda/busca` | `itens` (+ `total_itens`) | **incremental** (`data_alteracao_de`/`ate`) | `fct_conta_azul__vendas` |
| `produto_categorias` | `GET /v1/produtos/categorias` | `itens` | full refresh — master data | `dim_conta_azul__produto_categorias` |
| `produto_ncm` | `GET /v1/produtos/ncm` | `itens` | full refresh — master data fiscal | `dim_conta_azul__produto_ncm` |
| `produto_cest` | `GET /v1/produtos/cest` | `itens` | full refresh — master data fiscal | `dim_conta_azul__produto_cest` |
| `produto_unidades_medida` | `GET /v1/produtos/unidades-medida` | `itens` | full refresh — master data | `dim_conta_azul__produto_unidades_medida` |
| `produto_ecommerce_marcas` | `GET /v1/produtos/ecommerce-marcas` | `itens` | full refresh — 0 rows sem integração e-com | `dim_conta_azul__produto_ecommerce_marcas` |
| `categorias_dre` | `GET /v1/financeiro/categorias-dre` | `itens` | full refresh — estrutura contábil DRE | `dim_conta_azul__categorias_dre` |
| `transferencias` | `GET /v1/financeiro/transferencias` (`data_inicio`/`fim`) | `itens` | window 5 anos | `fct_conta_azul__transferencias` |
| `saldo_inicial` | `GET /v1/financeiro/eventos-financeiros/saldo-inicial` (`data_inicio`/`fim`) | `itens` | window 1 ano | `fct_conta_azul__saldo_inicial` |
| `conta_conectada` | `GET /v1/pessoas/conta-conectada` | objeto único (sem envelope) | full refresh — 1 row por tenant | `dim_conta_azul__conta_conectada` |

> **dim vs fct:** dimensoes (`dim_*`) representam entidades — pessoas, produtos, vendedores, categorias. Fatos (`fct_*`) representam eventos/transacoes com medidas — vendas, NFs, contas a receber/pagar, transferências, saldos. `contratos` fica como dim por ser entidade-like (acordo com ciclo de vida), nao evento pontual.

### Por que algumas não são incrementais

Endpoints **window-only** (`contratos`, `notas_servico`, `notas_fiscais`) não expõem `data_alteracao_de` na doc oficial v3 — só `data_inicial/final` ou `data_competencia`. UPSERT por `id` garante idempotência mesmo re-extraindo a mesma janela.

Endpoints **master data** (`categorias_*`, `centros_*`, `contas_financeiras`, `servicos`, `vendedores`) têm volume baixo e não expõem `data_alteracao_de` no endpoint. Full refresh é barato e simples.

> **Nota sobre formato do filtro `data_alteracao_de`:** a doc oficial v3 tipa diferente conforme o endpoint:
> - `/v1/pessoas` e `/v1/venda/busca`: ISO 8601 com hora (GMT-3) — `YYYY-MM-DDTHH:MM:SS`
> - `/v1/produtos` e `/v1/financeiro/eventos-financeiros/...`: data simples — `YYYY-MM-DD`
>
> O conector trata cada caso adequadamente. Truncar pra data nas runs incrementais é seguro — o UPSERT por `id` absorve o re-fetch de algumas horas extras de overlap.

### Endpoints conhecidos não implementados

Auditados mas deixados de fora — todos são GET-by-ID (não-listáveis), CDC opcional ou redundantes com algo já capturado:

- `/v1/financeiro/eventos-financeiros/alteracoes` — **CDC endpoint** (lista de IDs alterados num período). Vale considerar quando o volume estourar e a janela de 5 anos ficar cara — mas exige N+1 follow-up via `/financeiro/eventos-financeiros/parcelas/{id}`.
- `/v1/financeiro/baixas/{id}` + `/v1/financeiro/cobrancas/{id}` — GET-by-ID. As baixas/cobranças já vêm aninhadas em `contas_a_receber.baixas` (jsonb). Se precisar de mais detalhe, expandir via dbt `jsonb_array_elements`.
- `/v1/financeiro/eventos-financeiros/parcelas/{id}` + `/{id_evento}/parcelas` — GET-by-ID. As parcelas (composicao_valor, status) já estão dentro de `contas_a_*`.
- `/v1/conta-financeira/{id}/saldo-atual` — GET-by-ID. Útil em dashboard real-time, mas não vale ETL diário (calcular saldo via fct_transferencias + fct_contas_a_*).
- `/v1/produtos/ecommerce-categorias` — endpoint retorna 400 "filtros inválidos" sem documentação clara dos params requeridos. Provavelmente exige `tipo_ecommerce` ou similar. Reabrir investigação quando algum tenant tiver integração e-commerce ativa.
- `/v1/venda/{id}` — detalhe de venda com line items. O endpoint `/venda/busca` (que usamos) NÃO retorna a lista de produtos/serviços vendidos linha-a-linha — só agregado (total, tipo). Pra fct_vendas_itens (granularidade venda × item) será necessário implementar com pattern N+1 similar ao `pessoas_detalhe`.

## Configurar Connection no Airflow (passo a passo)

> Pré-requisito: ter um app registrado no [portal de devs Conta Azul](https://developers-portal.contaazul.com/) e ter feito o Authorization Code Flow uma vez para obter o `refresh_token` inicial.

### 1. Obter credenciais

No portal de developers do Conta Azul:
- `client_id` e `client_secret` ficam na página do app
- `refresh_token` inicial sai do Authorization Code Flow:
  1. Redireciona o cliente final para `https://app.contaazul.com.br/auth/...?response_type=code&client_id=<seu>&redirect_uri=<seu>&scope=<escopos>`
  2. Cliente autoriza, volta com `?code=<authorization_code>`
  3. Você troca o code por tokens em `POST https://auth.contaazul.com/oauth2/token` com `grant_type=authorization_code&code=<code>&redirect_uri=<seu>`
  4. Resposta inclui `refresh_token` — **anota agora**, ele só aparece aqui

> Atalho: existe a ferramenta [`get-conta-azul-oauth-token`](https://github.com/FabioDavidF/get-conta-azul-oauth-token) que faz isso localmente.

### 2. Criar Connection no Airflow

Vá em http://localhost:8080 → **Admin** → **Connections** → **+ (Add new)**:

| Campo | Valor |
|---|---|
| **Connection Id** | `luminea__conta_azul` (convenção `<tenant_slug>__<source>`) |
| **Connection Type** | `HTTP` |
| **Host** | (deixe em branco — o client usa URL hardcoded) |
| **Login** | `<client_id>` |
| **Password** | `<client_secret>` |
| **Extra** | `{"refresh_token": "<refresh_token_inicial>"}` |

Outros campos (Port, Schema, etc) ficam em branco.

> **Importante sobre o `refresh_token` na Extra:** o Conta Azul rotaciona o `refresh_token` a cada refresh. O client **persiste o novo refresh_token de volta na Connection automaticamente** após cada renovação. Você só precisa preencher o inicial.

### 3. (Opcional) Validar localmente sem creds reais

Forçar mock via env var:

```bash
docker compose exec airflow-scheduler env CONTA_AZUL_MOCK=1 airflow dags test luminea__conta_azul_etl
```

A DAG roda inteira retornando 25 customers fake.

## Comportamento de retries

| Cenário | Política |
|---|---|
| **401** | Refresh do `access_token` + 1 retry da request original (não conta como tentativa). Se persistir, levanta `ContaAzulAuthError` (refresh inválido — atualizar Connection manual). |
| **429** | Respeita header `Retry-After` (segundos). Conta como tentativa. |
| **5xx** | Backoff exponencial 2/4/8/16/32s (cap 60s). Conta como tentativa. |
| **Timeout / network** | Mesmo backoff dos 5xx. |
| **4xx outros** | Levanta `ContaAzulApiError` direto — não retenta (provável bug). |
| **Cap total** | 5 tentativas. Esgotou → levanta `ContaAzulApiError`. |

## Como adicionar um tenant novo

1. Criar Connection `<tenant_slug>__conta_azul` (passos acima).
2. Copiar `dags/luminea/conta_azul.py` para `dags/<tenant>/conta_azul.py` e ajustar:
   - `TENANT_SLUG = "<novo>"`
   - tags (`tenant:<slug>`)
3. Registrar `ServiceInstance` no banco da plataforma com `kind=etl` e `config.dag_id` apontando pro novo `dag_id`. Sem isso, `JobRun` callbacks não aparecem no portal.

## Estrutura interna

```
conta_azul/
├── __init__.py        # exports publicos
├── client.py          # OAuth2 + retry + paginacao (TUDO local)
├── extractors/        # 1 modulo por entidade (registry pattern)
│   ├── __init__.py    # EXTRACTORS dict + Extractor base
│   ├── base.py        # contract abstrato (DDL/UPSERT/fetch/serialize)
│   ├── pessoas.py
│   ├── vendas.py
│   └── ...            # uma classe por entidade da API
├── loader.py          # generico: resolve via EXTRACTORS[name]
├── schemas.py         # pydantic types (1 por entidade)
└── README.md          # este arquivo
```

Não há código compartilhado com outros conectores. Atualizar este arquivo não afeta Vindi/Shopify/etc — decisão deliberada de isolamento.

## Exceções

- **`ContaAzulAuthError`** — `refresh_token` rejeitado/revogado. Não retentável; admin precisa atualizar a Connection.
- **`ContaAzulApiError`** — 4xx (exceto 401/429) ou max retries excedido em 429/5xx. Stack trace inclui status + body pra debug.

## Versionamento

Este conector está **flat** (sem `v1/`/`v2/`). Quando precisar fazer breaking change em produção, mover tudo pra `v1/` e criar `v2/`. DAGs importam versão explicita: `from connectors.conta_azul.v1 import ContaAzulClient`.
