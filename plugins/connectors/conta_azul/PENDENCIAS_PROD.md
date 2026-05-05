# Pendências do conector Conta Azul para revalidar em produção

> Este documento lista o que ficou pendente, tentativo ou não-validado durante o desenvolvimento contra a conta **Luminea** (tenant dev no Conta Azul). Luminea é uma conta de teste com volume mínimo — várias entidades retornam 0 rows ou shapes incompletos, o que limitou a validação dos schemas. **Reabrir e validar cada item abaixo no primeiro tenant de produção.**

Tenant testado: `luminea` · Data do desenvolvimento: 2026-05-02

---

## 1. Schemas tentativos (entidades com 0 rows hoje)

Os schemas pydantic e DDLs destas entidades foram derivados do padrão da API v3 + `extra="allow"` + raw jsonb fallback, **sem confirmação contra dado real**. Quando aparecer volume em prod:

1. **Inspecionar `raw` jsonb das primeiras N rows** (`SELECT raw FROM <tenant>_raw.conta_azul__<entity> LIMIT 5;`)
2. **Comparar campos do shape real vs schema** (`pydantic` model + DDL)
3. **Adicionar colunas escalares relevantes ao DDL** (hoje só vai pra raw jsonb)
4. **Ajustar staging/curated pra extrair os campos novos**

| Entidade | Risco | O que validar |
|---|---|---|
| `contratos` | Médio | Fields de duração/valor/recorrência, status enum, condicao_pagamento, FK pra cliente_id, line items se houver |
| `notas_servico` (NFS-e) | Alto | Status transitions completas (10 valores possíveis), numero_rps, retencoes (ISS/PIS/COFINS/IRRF), tomador completo, valor_total vs valor_servicos |
| `notas_fiscais` (NFe produto) | Alto | Hoje só `EMITIDA`/`CORRIGIDA_SUCESSO` — quando outros statuses ativarem (CANCELADA, etc), validar fields novos. Itens de NFe (linhas de produto) provavelmente em endpoint separado |
| `transferencias` | Médio | Shape de `conta_origem`/`conta_destino` aninhados, conciliacao, taxa_transferencia |
| `centros_de_custo` | Baixo | Hierarquia (parent_id), código, classificação |
| `produto_ecommerce_marcas` | Baixo | Só relevante pra tenants com integração e-commerce ativa |

---

## 2. Issues específicos por entidade

### `conta_conectada` — `documento` vazio
Luminea dev não tem CNPJ preenchido. Schema já trata como opcional + pk synthetic. Em prod:
- [ ] Validar formato do documento (com/sem máscara — `12345678901234` vs `12.345.678/0001-90`)
- [ ] Validar `data_fundacao` (string ou date ISO)
- [ ] Considerar normalizar documento removendo máscara via field_validator

### `saldo_inicial` — retornou 1 row com tudo null
API retornou item vazio. Provável que endpoint exija filtros adicionais não documentados. Investigar:
- [ ] Testar com `ids_contas_financeiras=<uuid>` específico
- [ ] Verificar se data_inicio/data_fim precisam ser período curto específico (mês fiscal?)
- [ ] Comparar com endpoint `/v1/conta-financeira/{id}/saldo-atual` (GET-by-ID)
- [ ] Decisão: se permanecer null, **remover entidade** (não agrega valor) ou implementar via endpoint by-id N+1

### `vendas` — line items não vêm em `/venda/busca`
Endpoint só retorna agregado (total, tipo). Para granularidade venda × item:
- [ ] Implementar `vendas_detalhe` (N+1 via `/v1/venda/{id}`) seguindo pattern de `pessoas_detalhe`
- [ ] Criar `fct_vendas_itens` em curated com 1 row por linha (produto_id ou servico_id × venda_id)
- [ ] Considerar paralelização: 10k vendas × 250ms throttle = ~40min. Avaliar se justifica daily refresh ou se vai pra schedule menos frequente

### `produto_ecommerce_categorias` — REMOVIDO até saber filtros
Endpoint `/v1/produtos/ecommerce-categorias` retorna 400 "filtros inválidos" sem params específicos. Doc oficial não detalha. Reabrir quando tenant com e-commerce ativo:
- [ ] Testar com `tipo_ecommerce=<valor>` (Shopify/VTEX/Mercado Livre/etc)
- [ ] Pode ser que endpoint só responda com OAuth scope específico de e-commerce
- [ ] Se confirmado funcional: restaurar schema/extractor (estão no histórico do git)

### `categorias_dre` — árvore recursiva via `subitens`
Schema captura top-level (13 rows raiz). Filhos vivem em `subitens` jsonb aninhado. Quando precisar análise hierárquica completa:
- [ ] Criar model dbt que faz `jsonb_array_elements(subitens_raw)` recursivamente
- [ ] Resultado: dim plana com `nivel`, `parent_id`, `caminho_completo` ("01 > 01.1 > 01.1.2")
- [ ] Útil pra DRE drill-down em dashboards

### `produto_categorias` / `produto_unidades_medida` — IDs legacy int
API retorna `id` como integer (62920252) — DDL é `BIGINT`. Em produção verificar se:
- [ ] API começou a expor UUID v3-style além do int legacy (alguns endpoints v3 fazem migração progressiva)
- [ ] Se sim, manter ambos (`id BIGINT` legacy + `id_uuid UUID` opcional)

---

## 3. Padrões de filter `data_alteracao_de` por endpoint

Investigado e estabilizado, mas **revalidar em prod** se a doc oficial mudar:

| Endpoint | Formato esperado | Status atual |
|---|---|---|
| `/v1/pessoas` | ISO 8601 datetime sem timezone | ✅ confirmado prod |
| `/v1/venda/busca` | ISO 8601 datetime sem timezone | ✅ confirmado prod |
| `/v1/produtos` | Doc diz YYYY-MM-DD; mandamos datetime | ⚠️ funciona em dev, validar em prod |
| `/v1/financeiro/eventos-financeiros/contas-a-*` | ISO 8601 datetime | ✅ confirmado prod (erro 400 explicito quando estava errado) |
| `/v1/financeiro/eventos-financeiros/saldo-inicial` | ISO 8601 datetime | ✅ confirmado prod |

---

## 4. Volume / rate limit em prod

A conta Luminea tem volumes mínimos. Em prod, considerar:

### Janelas pesadas
- `notas_fiscais`: 365d / 15d = **24 chunks** + paginação. Tenant com 100/dia = 36k/ano = 360 páginas. Cada página ~1s ⇒ ~6min só pra esta entidade
- `notas_servico`: idem
- `contas_a_receber/pagar`: window de 5 anos. Tenant com 1k/mês = 60k rows. Sem chunking — pode estourar rate limit
- `transferencias`: 5 anos chunked em 1y = 5 chunks

### Rate limit
- API: ~50 req/min, ~10 req/s (doc oficial)
- Throttle local: 250ms entre requests (4 req/s)
- Em paralelismo do Airflow (max_active_tis_per_dag=2), 2 entities consomem ~8 req/s — perto do limite
- **Em prod com volume alto**, monitorar HTTP 429 e ajustar `_THROTTLE_S` se necessário

### N+1 patterns
- `pessoas_detalhe`: incremental, mas primeira run em tenant com 10k pessoas = ~40min
- `vendas_detalhe` (futuro): mesmo padrão
- **Considerar `/v1/financeiro/eventos-financeiros/alteracoes`** (CDC endpoint) quando volume estourar — retorna só IDs alterados num período, evita escanear janelas completas

---

## 5. Endpoints conhecidos NÃO implementados

Lista pra futuro (todos auditados na doc oficial):

| Endpoint | Razão de não ter implementado | Quando reabrir |
|---|---|---|
| `/v1/financeiro/eventos-financeiros/alteracoes` | CDC, complexo (N+1 follow-up). Janela atual cobre o caso | Volume estourar (5y window ficar caro) |
| `/v1/financeiro/baixas/{id}` | GET-by-ID. Já vem aninhado em `contas_a_receber.baixas` | Se precisar de fields extras só do detalhe |
| `/v1/financeiro/cobrancas/{id}` | GET-by-ID. Boletos/PIX já estão em `contas_a_*.cobranca` | Idem |
| `/v1/financeiro/eventos-financeiros/parcelas/{id}` | GET-by-ID. Parcelas vêm em `contas_a_*.parcelas` aninhado | Idem |
| `/v1/conta-financeira/{id}/saldo-atual` | Real-time, não cabe em ETL diário | Se for usar em dashboard real-time (calcular a partir de fct_transferencias é melhor) |
| `/v1/produtos/ecommerce-categorias` | Filtros não documentados — 400 | Tenant com e-com ativo |
| `/v1/venda/{id}` (line items) | Não implementado pra ter agregado das vendas | Quando precisar `fct_vendas_itens` |

---

## 6. Onboarding de tenant novo — checklist

Pra cada novo cliente que usa Conta Azul:

1. **Configurar Connection no Airflow:**
   - Connection ID: `<tenant_slug>__conta_azul`
   - Tipo: `HTTP`
   - Login: `<client_id>` · Password: `<client_secret>`
   - Extra: `{"refresh_token": "<token_inicial>"}`
   - Ver passo-a-passo no `README.md` do conector

2. **Criar DAG do tenant:**
   - `cp dags/luminea/conta_azul.py dags/<tenant>/conta_azul.py`
   - Ajustar `TENANT_SLUG`, `tags=["tenant:<slug>", ...]`, `schedule` (default 4am UTC)

3. **Configurar dbt project:**
   - `cp -r dbt/projects/luminea dbt/projects/<tenant>`
   - Ajustar `name`, `profile`, schemas em `dbt_project.yml` e `profiles.yml`
   - Atualizar `sources.yml` com `<tenant>_raw`

4. **Registrar no API metadata:**
   - `ServiceInstance` com `kind=etl` e `config.dag_id=<tenant>__conta_azul_etl`
   - Sem isso, JobRun callbacks não aparecem no portal

5. **Primeira run:**
   - Trigger manual, monitorar
   - Validar volumes esperados em todas 23 entidades
   - **Atualizar este documento** com achados específicos do tenant — especialmente shapes reais das 6 entidades hoje "tentativas"

6. **Rotina pós-onboarding:**
   - Verificar dbt tests com `severity: warn` — se constantemente falhando em prod, considerar promover pra error ou ajustar
   - Revisar tempo de cada run e otimizar window/throttle se necessário

---

## 7. Bugs/issues conhecidos a watchar

- ~~**Conta Azul rotaciona `refresh_token` a cada renovação.** Se persistência falhar...~~ ✅ resolvido em 2026-05-03. `_persist_tokens` em falha agora gera `log.error` estruturado (`alert=refresh_token_persist_failed`) + backup em `/tmp/conta_azul_token_backup_<conn>_<ts>.json` (perms 0600). Recovery: copiar backup pra Connection extra. Ver `docs/BACKLOG_FUTURO.md#9`.
- **Race condition no `CREATE TABLE IF NOT EXISTS`** entre task instances concorrentes do mesmo entity. Mitigado por advisory lock no `loader.py`, mas só efetivo dentro da mesma transação. Em deployment com múltiplos workers em hosts diferentes, considerar lock distribuído (Redis ou Postgres-based).
- **Watermark "preso" se pydantic validation falhar** — extractor não yield items, loader não grava watermark, próxima run repete o mesmo período (idempotente, mas perde tempo). Quando ver isso em prod, capturar o JSON cru via log e fixar schema.

---

## 8. Próximas frentes (escopo maior)

Não é "pendência do conector Conta Azul" mas relacionado:

- **Camada `delivery` (marts cross-source)** — agregados unificando Conta Azul + Vindi + Shopify + outros. Hoje `dbt_delivery` task group existe mas com poucos models. Definir specs com produto.
- **Outros conectores** — Vindi, Shopify, Meta Ads, Klaviyo. Padrão estabelecido pelo Conta Azul (extractors + base.py + paginate_all + loader genérico) deve ser replicado.
- **Observabilidade dos extractors** — métricas Prometheus/Grafana de rate de 429, tempo médio por entity, watermark drift, etc.

---

**Última atualização:** 2026-05-02
**Próxima revisão:** ao primeiro tenant de produção
