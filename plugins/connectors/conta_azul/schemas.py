"""Schemas pydantic das respostas da API do Conta Azul.

A API retorna campos em pt-BR (`nome`, `data_criacao`, ...). Mantemos
os atributos do model em pt-BR tambem, alinhado com a convencao
"nomes da API atravessam raw -> staging -> curated; so delivery
renomeia pra business names".

Defaults defensivos: APIs de ERP costumam ter campos opcionais
inconsistentes entre envs. `extra="allow"` evita que campos novos
da API derrubem o parsing. Empty strings (`""`) viram None via
validator onde aplicavel.

Endpoints e shapes confirmados via probe na API real (Luminea, mai/2026).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ─────────────── Mixin: empty string -> None ───────────────
def _empty_str_to_none(v):
    """API retorna `""` em vez de null pra campos nao preenchidos."""
    if isinstance(v, str) and v.strip() == "":
        return None
    return v


# ═══════════════════════════════════════════════════════════════════
# Pessoas (PF/PJ)
# Endpoint: GET /v1/pessoas  | envelope: `itens`
# ═══════════════════════════════════════════════════════════════════


class Pessoa(BaseModel):
    """Pessoa cadastrada no Conta Azul (PF ou PJ — clientes/fornecedores).

    Mapeamento:
        nome           -> nome           (obrigatorio)
        documento      -> documento      (CPF/CNPJ, sem formatacao)
        email          -> email
        telefone       -> telefone
        tipo_pessoa    -> tipo_pessoa    ("Fisica" | "Juridica")
        data_criacao   -> data_criacao
        data_alteracao -> data_alteracao
        ativo          -> ativo
        id_legado      -> id_legado
        perfis         -> perfis         (ex: ["Cliente", "Fornecedor"])
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    nome: str = Field(..., min_length=1)
    email: str | None = None
    documento: str | None = None
    telefone: str | None = None
    tipo_pessoa: str | None = None
    data_criacao: datetime
    data_alteracao: datetime
    ativo: bool | None = None
    id_legado: int | None = None
    uuid_legado: UUID | None = None
    perfis: list[str] | None = None

    _empty_to_none = field_validator("email", "documento", "telefone", mode="before")(
        _empty_str_to_none
    )


# ═══════════════════════════════════════════════════════════════════
# Categorias financeiras (arvore de categorias DRE)
# Endpoint: GET /v1/categorias  | envelope: `itens`
# Sample real: {"id":..., "versao":0, "nome":"13o Salario - 1a Parcela",
#   "categoria_pai":"<uuid>", "tipo":"DESPESA",
#   "entrada_dre":"DESPESAS_ADMINISTRATIVAS", "considera_custo_dre":false}
# ═══════════════════════════════════════════════════════════════════


class CategoriaFinanceira(BaseModel):
    """Categoria financeira do DRE (arvore via categoria_pai)."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    nome: str = Field(..., min_length=1)
    tipo: str | None = None  # 'RECEITA' | 'DESPESA'
    categoria_pai: UUID | None = None
    entrada_dre: str | None = None  # ex: 'DESPESAS_ADMINISTRATIVAS'
    considera_custo_dre: bool | None = None
    versao: int | None = None


# ═══════════════════════════════════════════════════════════════════
# Centros de custo
# Endpoint: GET /v1/centro-de-custo  | envelope: `itens`
# Tenant Luminea retorna vazio. Schema baseado em padrao de outras
# entidades + documentacao oficial.
# ═══════════════════════════════════════════════════════════════════


class CentroDeCusto(BaseModel):
    """Centro de custo cadastrado."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    nome: str = Field(..., min_length=1)
    descricao: str | None = None
    ativo: bool | None = None

    _empty_to_none = field_validator("descricao", mode="before")(_empty_str_to_none)


# ═══════════════════════════════════════════════════════════════════
# Contas financeiras (bancos, caixas, cartoes)
# Endpoint: GET /v1/conta-financeira  | envelope: `itens`
# Sample real: {"id":..., "banco":"NAO_BANCO", "codigo_banco":-10,
#   "nome":"Caixa", "ativo":true, "tipo":"CAIXINHA",
#   "conta_padrao":false, "possui_config_boleto_bancario":false,
#   "agencia":null, "numero":null}
# ═══════════════════════════════════════════════════════════════════


class ContaFinanceira(BaseModel):
    """Conta financeira (banco, caixa, cartao)."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    nome: str = Field(..., min_length=1)
    tipo: str | None = None  # 'CONTA_CORRENTE' | 'CAIXINHA' | 'CARTAO_CREDITO' | ...
    banco: str | None = None
    codigo_banco: int | None = None
    agencia: str | None = None
    numero: str | None = None
    ativo: bool | None = None
    conta_padrao: bool | None = None
    possui_config_boleto_bancario: bool | None = None

    _empty_to_none = field_validator("agencia", "numero", mode="before")(
        _empty_str_to_none
    )


# ═══════════════════════════════════════════════════════════════════
# Produtos
# Endpoint: GET /v1/produtos (PLURAL!)  | envelope: `items` (EN!)
# Restricao: page_size deve ser 10|20|50|100|200|500|1000.
# Tenant Luminea retorna vazio. Schema baseado em padrao da v3.
# ═══════════════════════════════════════════════════════════════════


class Produto(BaseModel):
    """Produto do catalogo."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    nome: str | None = None
    codigo: str | None = None
    descricao: str | None = None
    preco: float | None = None
    custo: float | None = None
    estoque: float | None = None
    estoque_minimo: float | None = None
    unidade: str | None = None
    status: str | None = None  # 'ATIVO' | 'INATIVO'
    data_criacao: datetime | None = None
    data_alteracao: datetime | None = None

    _empty_to_none = field_validator(
        "nome", "codigo", "descricao", "unidade", mode="before"
    )(_empty_str_to_none)


# ═══════════════════════════════════════════════════════════════════
# Servicos
# Endpoint: GET /v1/servico  | envelope: `itens`
# Sample real: {"id":..., "id_servico":476840263, "nome":null,
#   "codigo":"", "descricao":"Servico 01", "preco":0.0, "custo":0.0,
#   "status":"ATIVO", "tipo_servico":"PRESTADO"}
# ═══════════════════════════════════════════════════════════════════


class Servico(BaseModel):
    """Servico do catalogo."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    id_servico: int | None = None  # ID legado numerico
    nome: str | None = None
    codigo: str | None = None
    descricao: str | None = None
    preco: float | None = None
    custo: float | None = None
    status: str | None = None  # 'ATIVO' | 'INATIVO'
    tipo_servico: str | None = None  # 'PRESTADO' | 'TOMADO'

    _empty_to_none = field_validator("nome", "codigo", "descricao", mode="before")(
        _empty_str_to_none
    )


# ═══════════════════════════════════════════════════════════════════
# Vendedores
# Endpoint: GET /v1/venda/vendedores  | retorna LISTA direta (sem envelope)
# Sample real: [{"id":"<uuid>","nome":"Vitor Azul"}]
# ═══════════════════════════════════════════════════════════════════


class Vendedor(BaseModel):
    """Vendedor cadastrado pra associacao com vendas."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    nome: str = Field(..., min_length=1)


# ═══════════════════════════════════════════════════════════════════
# Contratos
# Endpoint: GET /v1/contratos?data_inicio=YYYY-MM-DD&data_fim=YYYY-MM-DD
# Tenant Luminea retorna 0 — schema baseado em research da v3.
# ═══════════════════════════════════════════════════════════════════


class Contrato(BaseModel):
    """Contrato com cliente. Schema tentativo — sera validado quando
    aparecer dado real. extra='allow' protege de campos novos."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    numero: str | None = None
    descricao: str | None = None
    cliente_id: UUID | None = None
    valor: float | None = None
    data_inicio: datetime | None = None
    data_fim: datetime | None = None
    status: str | None = None
    data_criacao: datetime | None = None
    data_alteracao: datetime | None = None

    _empty_to_none = field_validator("numero", "descricao", mode="before")(
        _empty_str_to_none
    )


# ═══════════════════════════════════════════════════════════════════
# Pessoa detalhe (endpoint /v1/pessoas/{id})
# Traz nested arrays: enderecos, outros_contatos, inscricoes, pessoas_legado.
# Plus escalares extras nao expostos no list endpoint.
# ═══════════════════════════════════════════════════════════════════


class NotaServico(BaseModel):
    """Nota fiscal de servico (NFS-e). Endpoint: /v1/notas-fiscais-servico.

    Schema TENTATIVO — Luminea retorna 0. Campos baseados em padrao da API v3.
    extra='allow' protege contra schema drift.

    Doc oficial confirma envelope `{itens, paginacao: {total_itens}}`.
    Status possiveis: PENDENTE | PRONTA_ENVIO | AGUARDANDO_RETORNO | EM_ESPERA
                    | EMITINDO | EMITIDA | CANCELADA | FALHA
                    | FALHA_CANCELAMENTO | CORRIGIDA_SUCESSO.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    numero: str | None = None
    status: str | None = None
    valor: float | None = None
    data_emissao: datetime | None = None
    data_competencia: datetime | None = None
    cliente_id: UUID | None = None

    _empty_to_none = field_validator("numero", mode="before")(_empty_str_to_none)


class NotaFiscal(BaseModel):
    """Nota fiscal de produto (NFe). Endpoint: /v1/notas-fiscais.

    Schema TENTATIVO — doc oficial nao expoe shape do item ({...}). Filtros
    documentados (numero_nota, id_venda, documento_tomador) revelam parte
    do modelo de dados. extra='allow' + raw jsonb preservam o resto.

    Doc oficial: hoje so retorna NFe com status EMITIDA e CORRIGIDA_SUCESSO
    (outros status em construcao).

    Distincao vs NotaServico:
      - NFe = nota fiscal eletronica de PRODUTO (NF-e).
      - NotaServico = nota fiscal eletronica de SERVICO (NFS-e), endpoint
        /v1/notas-fiscais-servico.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    numero: str | None = None
    status: str | None = None  # 'EMITIDA' | 'CORRIGIDA_SUCESSO' (hoje)
    valor: float | None = None
    data_emissao: datetime | None = None
    chave_acesso: str | None = None
    serie: str | None = None
    venda_id: UUID | None = None
    cliente_id: UUID | None = None
    documento_tomador: str | None = None

    _empty_to_none = field_validator(
        "numero", "chave_acesso", "serie", "documento_tomador", mode="before"
    )(_empty_str_to_none)


class EventoFinanceiro(BaseModel):
    """Evento financeiro — base pra contas_a_receber e contas_a_pagar.

    Endpoints:
      /v1/financeiro/eventos-financeiros/contas-a-receber/buscar  (cliente)
      /v1/financeiro/eventos-financeiros/contas-a-pagar/buscar    (fornecedor)

    Sample real (contas-a-receber):
        id, status ('ACQUITTED'), status_traduzido, total, descricao,
        data_vencimento, data_competencia, data_criacao, data_alteracao,
        nao_pago, pago, categorias[{id,nome}], centros_de_custo[],
        cliente|fornecedor {id, nome}, renegociacao (nullable)
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    status: str | None = None
    status_traduzido: str | None = None
    total: float | None = None
    descricao: str | None = None
    data_vencimento: datetime | None = None
    data_competencia: datetime | None = None
    data_criacao: datetime | None = None
    data_alteracao: datetime | None = None
    nao_pago: float | None = None
    pago: float | None = None
    # Nested arrays — JSONB no raw, expandidos em dbt staging.
    categorias: list[dict] | None = None
    centros_de_custo: list[dict] | None = None
    # Objeto unico — cliente em /contas-a-receber, fornecedor em /contas-a-pagar.
    # Schema generico: 'parte' (cliente OU fornecedor). Subclasses fazem alias.
    cliente: dict | None = None
    fornecedor: dict | None = None
    renegociacao: dict | None = None

    _empty_to_none = field_validator(
        "descricao", "status", "status_traduzido", mode="before"
    )(_empty_str_to_none)


def _coerce_dict_to_nome(v):
    """API as vezes retorna campos enum como {'nome': 'X', 'descricao': '...'}
    em vez de string direta. Extrai .nome quando vier dict."""
    if isinstance(v, dict) and "nome" in v:
        return v["nome"]
    return v


class Venda(BaseModel):
    """Venda do Conta Azul. Endpoint: /v1/venda/busca | envelope: `itens`.

    Shape confirmado via probe (Luminea, mai/2026):
        {
            "id": uuid,
            "data": "2026-04-18",            # data da venda (date YYYY-MM-DD)
            "tipo": "SALE",                  # SALE | ORCAMENTO | PEDIDO | NOTA_FISCAL
            "itens": "PRODUCT",              # ENUM tipo (PRODUCT/SERVICE), NAO array!
            "total": 50.0,
            "numero": 1,
            "origem": str,
            "versao": int,                   # controle de concorrencia
            "cliente": {id, nome, email, uuid_legado} | null,
            "vendedor": {id, nome} | null,
            "categoria": {...} | null,
            "natureza_operacao": {...} | null,
            "pagamento": {...} | null,
            "situacao": "APROVADO",          # str (algumas variantes vem como dict)
            "pendente": bool | null,
            "desconto": float | null,
            "observacoes": str | null,
            "criado_em": "2026-04-18T14:26:00.728",
            "data_alteracao": "...",
            "id_legado": int,
            "condicao_pagamento": bool,
            # data_venda / data_criacao APARECEM mas sempre null (campos legados)
        }

    NOTA SOBRE LINE ITEMS: o endpoint /venda/busca NAO retorna a lista de
    produtos/servicos vendidos — so o agregado (total, tipo). Pra obter
    os items linha-a-linha, precisaria GET /v1/venda/{id} (detalhe, N+1).
    Fora de escopo desta versao.

    Resposta envelope tem campos extras nao mapeados aqui — gerenciados
    no client (totais, quantidades, total_itens):
        {totais, quantidades, total_itens, itens: [Venda, ...]}
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    numero: int | None = None
    # API tem 2 variantes para situacao/tipo/origem: str ou dict {nome, descricao}.
    # _coerce_dict_to_nome normaliza dict -> str.nome.
    situacao: str | None = None
    tipo: str | None = None
    origem: str | None = None
    # `tipo_item` mapeia o campo "itens" da API — que e' enum string
    # ('PRODUCT'/'SERVICE'), NAO array de items. Aliased pra evitar confusao.
    tipo_item: str | None = Field(default=None, alias="itens")
    pendente: bool | None = None
    versao: int | None = None
    condicao_pagamento: bool | None = None

    total: float | None = None
    desconto: float | None = None

    # Datas — campos REAIS confirmados:
    #   - `data` (date YYYY-MM-DD) = data da venda emitida
    #   - `criado_em` (datetime) = criacao no Conta Azul
    #   - `data_alteracao` (datetime) = ultima alteracao
    # Os legados `data_venda`/`data_criacao` sempre vem null mas mantidos
    # como passthrough no extra='allow' caso voltem em alguma rota.
    data: datetime | None = None
    criado_em: datetime | None = None
    data_alteracao: datetime | None = None

    observacoes: str | None = None

    # Objetos aninhados — JSONB no raw, expandidos via dbt staging.
    cliente: dict | None = None
    vendedor: dict | None = None
    natureza_operacao: dict | None = None
    categoria: dict | None = None
    pagamento: dict | None = None

    # IDs legados (api v2) — pra reconciliacao com bases historicas.
    id_legado: int | None = None
    id_legado_cliente: int | None = None
    id_legado_dono: int | None = None

    _empty_to_none = field_validator(
        "situacao", "tipo", "origem", "observacoes", mode="before"
    )(_empty_str_to_none)
    _coerce_enums = field_validator("situacao", "tipo", "origem", mode="before")(
        _coerce_dict_to_nome
    )


class ProdutoCategoria(BaseModel):
    """Categoria de produto. Endpoint: /v1/produtos/categorias.

    API retorna `id` como int legacy (ex: 62920252) — usamos BIGINT no
    DDL pra preservar operacoes numericas (joins, range queries).
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: int
    nome: str | None = None
    descricao: str | None = None
    ativo: bool | None = None

    _empty_to_none = field_validator("nome", "descricao", mode="before")(_empty_str_to_none)


class ProdutoNcm(BaseModel):
    """Codigo NCM (Nomenclatura Comum do Mercosul). Endpoint: /v1/produtos/ncm.

    PK natural = codigo (8 digitos numericos). Schema tentativo;
    extra='allow' captura campos descobertos no run real.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    codigo: str = Field(..., min_length=1)
    descricao: str | None = None

    _empty_to_none = field_validator("descricao", mode="before")(_empty_str_to_none)


class ProdutoCest(BaseModel):
    """Codigo CEST (Codigo Especificador da Substituicao Tributaria).
    Endpoint: /v1/produtos/cest.

    PK natural = codigo. Schema tentativo.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    codigo: str = Field(..., min_length=1)
    descricao: str | None = None
    ncm_relacionado: str | None = None

    _empty_to_none = field_validator(
        "descricao", "ncm_relacionado", mode="before"
    )(_empty_str_to_none)


class ProdutoUnidadeMedida(BaseModel):
    """Unidade de medida (UN, KG, M, etc.). Endpoint: /v1/produtos/unidades-medida.

    API retorna `id` como int legacy. Mantemos BIGINT no DDL.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: int
    sigla: str | None = None
    descricao: str | None = None
    fracionavel: bool | None = None

    _empty_to_none = field_validator("sigla", "descricao", mode="before")(
        _empty_str_to_none
    )


class ProdutoEcommerceMarca(BaseModel):
    """Marca pra integracao e-commerce. Endpoint: /v1/produtos/ecommerce-marcas.

    Volume tipicamente 0 em tenants sem integracao e-commerce ativa.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    nome: str = Field(..., min_length=1)


class CategoriaDre(BaseModel):
    """Categoria do DRE (Demonstrativo de Resultado).
    Endpoint: /v1/financeiro/categorias-dre.

    Shape REAL confirmado via probe (Luminea, mai/2026):
        {
            "id": uuid,
            "descricao": "Lucro Bruto" | "Despesas Operacionais" | ...,
            "codigo": "04" | null,            # codigo da categoria
            "posicao": int,                   # ordem no DRE
            "indica_totalizador": bool,       # se e' linha de totalizacao
            "representa_soma_custo_medio": bool,
            "subitens": [<recursive CategoriaDre>],     # filhos nested
            "categorias_financeiras": [{id, nome, codigo, ativo}],
            "parent_id": null,                # sempre null — hierarquia via subitens
            # Campos legados sempre null:
            "nome": null,
            "tipo": null,
            "nivel": null,
            "ordem": null
        }

    NOTA: API retorna a arvore expandida via `subitens` aninhados. Os
    13 rows top-level sao as categorias raiz; os filhos vivem dentro de
    subitens jsonb (expandir via dbt jsonb_array_elements se preciso).
    Por isso `parent_id` sempre vem null no top-level.

    Distinto de CategoriaFinanceira (/v1/categorias) — esta e' especifica
    do DRE, estrutura contabil hierarquica.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    # `descricao` e' o NOME real da categoria (campo `nome` da API e' sempre null).
    descricao: str | None = None
    codigo: str | None = None
    posicao: int | None = None
    indica_totalizador: bool | None = None
    representa_soma_custo_medio: bool | None = None
    # Nested arrays — expandiveis via dbt jsonb_array_elements se preciso.
    subitens: list[dict] | None = None
    categorias_financeiras: list[dict] | None = None

    _empty_to_none = field_validator("descricao", "codigo", mode="before")(
        _empty_str_to_none
    )


class Transferencia(BaseModel):
    """Transferencia entre contas financeiras (movimento interno).
    Endpoint: /v1/financeiro/transferencias.

    Granularidade: 1 row = 1 transferencia (ja com debito + credito vinculados).
    Schema tentativo — doc nao detalha shape do item.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    data_transferencia: datetime | None = None
    valor: float | None = None
    descricao: str | None = None
    conta_origem_id: UUID | None = None
    conta_destino_id: UUID | None = None
    # Objetos completos quando vierem aninhados.
    conta_origem: dict | None = None
    conta_destino: dict | None = None
    data_criacao: datetime | None = None
    data_alteracao: datetime | None = None

    _empty_to_none = field_validator("descricao", mode="before")(_empty_str_to_none)


class SaldoInicial(BaseModel):
    """Saldo inicial por conta financeira em determinado periodo.
    Endpoint: /v1/financeiro/eventos-financeiros/saldo-inicial.

    Schema tentativo. Provavel granularidade: 1 row por (conta_financeira, periodo).
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID | None = None  # pode nao existir; PK composta (conta + data) como fallback
    conta_financeira_id: UUID | None = None
    data_referencia: datetime | None = None
    saldo: float | None = None
    saldo_inicial: float | None = None
    conta_financeira: dict | None = None


class ContaConectada(BaseModel):
    """Dados da empresa conectada (1 row por tenant).
    Endpoint: /v1/pessoas/conta-conectada.

    Single-object response (sem envelope). Tenants podem ter campos
    opcionais (vimos Luminea com `documento=''`) — todos sao opcionais
    no schema. PK no DW e' synthetic (sempre 1 row por schema_raw).
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    razao_social: str | None = None
    nome_fantasia: str | None = None
    documento: str | None = None  # CNPJ — pode vir vazio em contas dev
    email: str | None = None
    data_fundacao: str | None = None  # API pode retornar string ou date

    _empty_to_none = field_validator(
        "razao_social", "nome_fantasia", "documento", "email", "data_fundacao",
        mode="before",
    )(_empty_str_to_none)


class PessoaDetalhe(BaseModel):
    """Detalhe completo de pessoa. Diferente do schema Pessoa (list) — tem nested.

    Subset dos campos retornados — extra='allow' captura o resto sem quebrar.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: UUID
    nome: str = Field(..., min_length=1)
    documento: str | None = None
    email: str | None = None
    telefone_comercial: str | None = None
    telefone_celular: str | None = None
    tipo_pessoa: str | None = None
    nome_empresa: str | None = None
    rg: str | None = None
    data_nascimento: str | None = None  # API retorna como string ou null
    optante_simples_nacional: bool | None = None
    orgao_publico: bool | None = None
    observacao: str | None = None
    codigo: str | None = None
    ativo: bool | None = None

    # Nested arrays — armazenados como jsonb no raw, expandidos via dbt.
    enderecos: list[dict] | None = None
    outros_contatos: list[dict] | None = None
    inscricoes: list[dict] | None = None
    pessoas_legado: list[dict] | None = None
    perfis: list[dict] | None = None  # detail traz [{"tipo_perfil": "Cliente"}]

    _empty_to_none = field_validator(
        "documento", "email", "telefone_comercial", "telefone_celular",
        "nome_empresa", "rg", "observacao", "codigo",
        mode="before",
    )(_empty_str_to_none)
