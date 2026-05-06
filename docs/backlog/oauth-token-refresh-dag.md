# DAG dedicada de refresh de tokens OAuth

**Status:** backlog (ideia)
**Origem:** discussão 2026-05-06 sobre race condition no refresh do Conta Azul.

## Ideia

Em vez de cada DAG de ETL ter sua própria task `ensure_token_fresh` no
início (solução atual), centralizar o refresh numa DAG dedicada que roda
periodicamente (ex: a cada 15-20min, bem abaixo do TTL de 1h dos AT
típicos do Conta Azul).

A DAG de ETL deixa de se preocupar com auth — só lê o `access_token` da
Connection, que sempre estará fresco porque foi atualizado <20min atrás
pela DAG de refresh.

## Como funcionaria

```
DAG: platform__oauth_token_refresh    schedule: */15 * * * *
  └─ refresh_all_oauth_connections (mapped task)
       └─ refresh_connection(conn_id)   x N tenants × M sources
```

A DAG itera sobre todas as Connections que seguem o padrão OAuth2 com
refresh_token rotativo (ex: `*__conta_azul`, `*__vindi`, ...) e chama o
método `ensure_token_fresh` do client correspondente.

Discovery das connections: 2 opções

1. **Naming convention** — todas as Connections que matchem regex tipo
   `^[a-z]+__(conta_azul|vindi|...)$` viram alvos.
2. **Registry no platform DB** — tabela `oauth_connections_managed` lista
   `(conn_id, source, refresh_strategy)` e a DAG itera dela.

## Vantagens vs solução atual (task no início de cada DAG ETL)

- ETL DAG fica com responsabilidade unica (extract); não pensa em auth
- Refresh logic centralizada — 1 lugar pra debugar/corrigir
- Multi-tenant + multi-source compartilham infra (1 DAG cobre todos)
- Token sempre fresco (gap max ~20min vs TTL de 1h)
- Falha no refresh é detectada no DAG dedicado (sinal claro pra ops),
  desacoplada da execução do ETL
- Várias DAGs de ETL podem disparar simultaneamente sem nenhum risco
  de race no refresh — o refresh dedicado já garantiu AT válido

## Considerações / tradeoffs

- **Disciplina de `allow_refresh=False`** continua sendo necessária nas
  DAGs de ETL — se elas pudessem refrescar, ainda haveria possibilidade
  de race com a DAG dedicada. A flag fica como hard guarantee.
- **Custo do refresh proativo:** a cada 15-20min × N connections =
  ~100-200 refreshes/dia mesmo quando ninguém ta usando os tokens.
  Cognito não cobra mas gera churn no `refresh_token` (rotaciona toda
  vez). Aceitável.
- **Falha do scheduler do refresh:** se a DAG dedicada parar de rodar
  por X horas, AT expira e ETL DAGs começam a falhar. Precisa de
  alerting no DAG de refresh (sucesso recente é precondição).
- **Cold start** de tenant novo: até a DAG dedicada rodar pela primeira
  vez (~20min), o AT inicial cravado pelo onboarding precisa estar
  válido. Onboarding já faz isso.
- **Plugin contract:** cada conector (conta_azul, vindi, ...) precisa
  expor uma função `refresh_connection(conn_id)` ou equivalente. Hoje
  o Conta Azul já tem `ContaAzulClient.ensure_token_fresh()` — é só
  expor uniformemente.

## Migração a partir do estado atual

1. Implementar DAG `platform__oauth_token_refresh` com schedule `*/15 * * * *`
2. Adicionar discovery (naming convention é mais simples pra começar)
3. Manter task `ensure_token_fresh` no início das DAGs de ETL como
   fallback durante migração — após confirmar que a DAG dedicada está
   estável, remover essas tasks
4. Documentar/treinar: nunca chamar refresh fora dessa DAG dedicada

## Decisão pendente

- (?) Frequência: 15min vs 20min vs 30min — depende do TTL mínimo do AT
  entre todos os fornecedores que serão integrados
- (?) DAG única (multi-tenant, multi-source) vs DAG por source — uma única
  começa mais simples, uma por source escala melhor com isolation
- (?) Onde grava o registry de connections gerenciadas (se for por DB)
