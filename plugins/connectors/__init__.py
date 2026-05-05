"""Conectores de extracao de APIs externas.

Cada conector e' ISOLADO — implementa seu proprio HTTP client, retry,
paginacao, OAuth. Nao ha codigo compartilhado entre conectores
diferentes (decisao deliberada — atualizar a lib do Conta Azul nao
pode quebrar a DAG da Vindi).

Cross-cutting concerns que SAO compartilhados:
- platform_telemetry (callbacks de job_run pro API Core)
- DW connection (env vars)

Cada conector segue o layout:
    connectors/<source>/
        __init__.py     # exports publicos (Client, extractors)
        client.py       # auth + HTTP + retry + paginacao
        extractors.py   # funcoes extract_<entity>(client) -> Iterator
        schemas.py      # pydantic types das respostas
        README.md       # como adicionar novo tenant a este conector

Versionamento: comeca flat. Quando precisar quebrar contrato em prod,
mover tudo pra `<source>/v1/` e criar `<source>/v2/`. DAGs importam
versao explicita: `from connectors.conta_azul.v1 import Client`.
"""
