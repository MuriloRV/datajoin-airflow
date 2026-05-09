# Onboarding Conta Azul — checklist para um tenant novo

Documento prescritivo, baseado nas pegadinhas reais do onboarding da Luminea (1º tenant em produção). Siga na ordem — cada passo previne uma falha já vista.

Para o **conceito** do conector (retry, OAuth2, paginação) ver `README.md`. Este doc é só o **procedimento de cadastro inicial** específico do conector Conta Azul.

> **Pré-requisito de plataforma:** Airflow rodando com `AIRFLOW_FERNET_KEY` válido no `.env`. Se a stack ainda não está de pé, ver `CLAUDE.md` na raiz e o `README.md` do projeto.

---

## Pré-requisitos do conector

Cliente final precisa ter:
- Conta cadastrada no [portal de devs Conta Azul](https://developers-portal.contaazul.com/).
- App criado com **redirect_uri** registrado. Padrão recomendado: `https://contaazul.com` (não exige rodar callback local).
- Te repassar: `client_id`, `client_secret`, `redirect_uri`.

---

## Passo 1: Gerar o `refresh_token` inicial

Authorization Code Flow. Manual e **uma vez por tenant** — depois disso o connector cuida da rotação automática.

### 1.1 — Abrir URL de authorize no browser

Substitua `<CLIENT_ID>` e `<TENANT_SLUG>` (o slug é só pra identificar o state CSRF):

```
https://auth.contaazul.com/oauth2/authorize?response_type=code&client_id=<CLIENT_ID>&redirect_uri=https://contaazul.com&state=<TENANT_SLUG>_001
```

Cliente final autoriza com a conta dele. O browser redireciona pra:

```
https://contaazul.com/?code=<AUTHORIZATION_CODE>&state=<TENANT_SLUG>_001
```

Copie o valor de `code` da URL.

> **Pegadinha:** o `code` do Cognito (provedor OAuth da Conta Azul) **expira em ~5 minutos** e é **uso único**. Se o curl do passo seguinte falhar, o code já era — gera de novo abrindo a URL de authorize.

### 1.2 — Trocar code por tokens via curl

```bash
curl -s -X POST https://auth.contaazul.com/oauth2/token \
  -u "<CLIENT_ID>:<CLIENT_SECRET>" \
  -d "grant_type=authorization_code" \
  -d "code=<AUTHORIZATION_CODE>" \
  -d "redirect_uri=https://contaazul.com"
```

A resposta traz `access_token`, `refresh_token` e `expires_in`. **Anota o `refresh_token` agora** — só aparece nesta resposta. É um JWE longo (~4kB).

---

## Passo 2: Criar a Connection no Airflow

UI: http://localhost:8080 → **Admin** → **Connections** → **+**

| Campo | Valor |
|---|---|
| **Connection Id** | `<tenant_slug>__conta_azul` (convenção fixa — não invente) |
| **Connection Type** | `HTTP` |
| **Host** | (em branco — URL é hardcoded no client) |
| **Login** | `<client_id>` |
| **Password** | `<client_secret>` |
| **Extra** | `{"refresh_token": "<refresh_token_inicial>"}` |

> **Pegadinha:** o `refresh_token` é grande e a UI do Airflow às vezes acrescenta quebra de linha quando você cola. Se a primeira run der `invalid_grant`, abre a Connection e confere se o valor virou multiline. Cola colado-em-uma-linha-só. Não escapa nada — só o JSON do Extra.

---

## Passo 3: Criar a DAG do tenant

Copia `dags/luminea/conta_azul.py` para `dags/<tenant>/conta_azul.py` e troca:

```python
TENANT_SLUG = "<tenant_slug>"   # bate com slug do banco datajoin
```

Ajusta `tags` (`tenant:<slug>`) e o `dag_id` derivado já é `<slug>__conta_azul_etl`.

### Imports obrigatórios — não remover

A DAG copiada já vem com:

```python
import platform_warnings  # noqa: F401  -- side effect: filtra warnings do Cosmos
```

Esse import **tem que vir antes de `from cosmos import ...`** — instala filtros que silenciam ~5 deprecation warnings por task que vêm da Cosmos 1.14.x x Airflow 3.x. Sem ele, os logs ficam ilegíveis (centenas de linhas de ruído escondendo warnings reais). Detalhes no docstring de `plugins/platform_warnings.py`.

> **Pegadinha:** o `dag_id` precisa bater com `ServiceInstance.config.dag_id` no banco datajoin (responsabilidade do passo de cadastro do tenant, não do conector). Sem isso a DAG roda mas os callbacks de `pipeline_run`/`pipeline_task` não chegam no portal — telemetria silenciosamente quebrada.

---

## Passo 4: Validar sem creds reais (smoke test)

Antes de queimar credencial em testes, força o mock:

```bash
docker compose exec airflow-scheduler env CONTA_AZUL_MOCK=1 \
    airflow dags test <tenant>__conta_azul_etl
```

Roda inteira (extract → dbt staging/curated → dbt delivery) com 25 customers fake. Se isso falhar, o problema **não** é Conta Azul — é DAG/dbt/DW. Resolve aqui antes de partir pra real.

---

## Passo 5: Primeira run real

```bash
docker compose exec airflow-scheduler airflow dags trigger <tenant>__conta_azul_etl
```

Acompanha em http://localhost:8080. Tempo esperado: ~30s pra extract + dbt curado pequeno.

### Verificações pós-run

```bash
# 1. Dados no DW
docker compose exec postgres psql -U dw_admin -d warehouse \
    -c "SELECT COUNT(*) FROM <tenant>_raw.conta_azul__customers;"

# 2. Rotação do refresh_token persistiu — extra deve ter 3 chaves
# (refresh_token + access_token + access_token_expires_at)
docker compose exec airflow-scheduler airflow connections get <tenant>__conta_azul -o json
```

Se `extra` ficou só com `refresh_token` depois da run, a rotação **falhou silenciosamente** (vai dar `invalid_grant` na próxima run em ≤1h). Ver troubleshooting abaixo.

---

## Troubleshooting (erros que já vimos no Conta Azul)

### `invalid_grant` no refresh

Cenários possíveis:
1. **Refresh_token foi consumido sem persistir o novo** — Conta Azul rotaciona a cada refresh. Solução: refazer Passo 1 e atualizar a Connection.
2. **Authorization code já foi usado** — codes são uso único. Gera novo a partir da URL de authorize.
3. **Authorization code expirou** — TTL ~5min. Mesmo fix do (2).
4. **refresh_token com quebra de linha no extra** — confere o JSON do extra. Espaços/newlines extras = JWE inválido.

### `ValidationError: 3 validation errors for Customer (nome / data_criacao / data_alteracao)`

Causa: API retorna campos em pt-BR que o pydantic schema antigo não aceitava.

Fix: já tratado em `schemas.py` via `Field(alias=...)` + `populate_by_name=True`. Se você adicionar campos novos ao schema, segue o mesmo padrão (alias = nome em pt-BR da API).

### `422 Unprocessable Content` ao persistir tokens na Connection

Causa: `_persist_tokens` no `client.py` montou PATCH sem `connection_id` + `conn_type`. Airflow 3.0 valida o body via Pydantic (`ConnectionBody`) **mesmo com `update_mask=extra`**.

Fix: já corrigido em `client.py:_persist_tokens` (inclui ambos os campos no PATCH). Se você mexer nesse método, mantenha os 3 campos no body.

### Warning "envelope sem chave conhecida (itens|items|dados|data)"

Causa: API retornou `{"items": null}` ou `{"items": []}` em página vazia (terminator de paginação).

Fix: já tratado em `client.py:list_customers` — distingue "chave ausente" de "chave presente com null/[]". Se o warning aparecer em produção, é sinal de que a API mudou shape — investigar antes de silenciar.

### DAG roda mas portal datajoin não mostra a `PipelineRun`

Causa: `ServiceInstance` no banco da plataforma sem `config.dag_id` apontando pro `dag_id` correto. Não é um problema do conector — é do cadastro do tenant na plataforma.

Fix: registrar via `make shell-meta`:
```sql
INSERT INTO service_instances (id, tenant_id, kind, config)
VALUES (gen_random_uuid(), '<tenant_uuid>', 'etl',
        '{"dag_id": "<tenant>__conta_azul_etl"}'::jsonb);
```

---

## Resumo do fluxo (TL;DR)

1. Cliente entrega `client_id` + `client_secret` + `redirect_uri`.
2. Abre URL de authorize → cliente autoriza → captura `code` da URL de redirect.
3. Curl POST no `oauth2/token` → recebe `refresh_token`.
4. Cria Connection `<slug>__conta_azul` (HTTP) com login/password/extra.
5. Copia DAG da Luminea, troca `TENANT_SLUG` (mantém `import platform_warnings`).
6. Smoke test com `CONTA_AZUL_MOCK=1`.
7. Trigger real, valida 5+ rows no DW e 3 keys no extra da Connection.

A partir daí, a DAG roda diariamente às 4am UTC e rotaciona o `refresh_token` sozinha. Intervenção manual só se a Connection for revogada externamente (cliente cancelar acesso, secret rotacionado, etc.).
