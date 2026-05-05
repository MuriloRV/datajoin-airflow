"""Conector da API do Conta Azul.

Doc oficial: https://developers.contaazul.com/

Auth: OAuth 2.0 Authorization Code Flow.
Token endpoint: POST https://auth.contaazul.com/oauth2/token
Rate limit: ~50 req/min, ~10 req/s por ERP conectado.

Arquitetura:
- client.py        OAuth2 + retries + paginacao primitiva por endpoint
- schemas.py       pydantic models (1 por entidade)
- extractors/      registry pattern — 1 modulo por entidade
- loader.py        generico, resolve via EXTRACTORS

Novo extractor: ver docstring de extractors/__init__.py.
"""
from connectors.conta_azul.client import ContaAzulClient
from connectors.conta_azul.extractors import EXTRACTORS
from connectors.conta_azul.loader import load_entity_to_raw
from connectors.conta_azul.schemas import Pessoa

__all__ = [
    "ContaAzulClient",
    "EXTRACTORS",
    "Pessoa",
    "load_entity_to_raw",
]
