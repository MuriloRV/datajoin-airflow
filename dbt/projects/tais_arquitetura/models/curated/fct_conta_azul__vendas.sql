-- Curated: tabela fato de vendas com flags derivados pra analise.
-- Granularidade: 1 linha por venda (transacao).
-- FK: cliente_id -> dim_pessoas.
--
-- MODO ENXUTO (2026-07): o LEFT JOIN com stg_vendas_detalhe foi removido —
-- a extracao de vendas_detalhe (N+1 caro) retornava 100% NULL da API do
-- Conta Azul (valor_liquido/frete/impostos/num_parcelas nunca vinham
-- preenchidos). As colunas do detalhe permanecem na saida como NULL tipado
-- pra nao quebrar o dataset `vendas_detalhe.sql` do portal.
-- REATIVAR: restaurar a CTE `detalhe`/join abaixo + reativar a entidade
-- `vendas_detalhe` na DAG e o model `stg_conta_azul__vendas_detalhe` no
-- dbt_project.yml.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__vendas') }}
)

select
    venda_id,
    venda_id_legado,
    venda_numero,
    situacao,
    tipo,
    tipo_item,
    origem,
    pendente,
    condicao_pagamento,
    versao,
    valor_total,
    valor_desconto,
    -- Campos do detalhe (desativados — API retorna 100% NULL; ver cabecalho).
    null::numeric   as valor_bruto_detalhe,
    null::numeric   as valor_frete,
    null::numeric   as valor_impostos,
    null::numeric   as valor_liquido,
    null::text      as natureza_operacao_nome,
    null::jsonb     as condicao_pagamento_detalhe,
    null::int       as num_parcelas,
    data_venda,
    source_created_at,
    source_updated_at,
    observacoes,
    cliente_id,
    cliente_nome,
    cliente_email,
    cliente_id_legado,
    vendedor_id,
    vendedor_nome,
    dono_id_legado,
    -- Flags derivados a partir da situacao.
    case when upper(situacao) = 'APROVADO'            then true else false end as is_aprovada,
    case when upper(situacao) = 'CANCELADO'           then true else false end as is_cancelada,
    case when upper(situacao) = 'ESPERANDO_APROVACAO' then true else false end as is_esperando_aprovacao,
    -- Flag de tipo_item (enum PRODUCT/SERVICE).
    case when upper(tipo_item) = 'PRODUCT' then true else false end as is_produto,
    case when upper(tipo_item) = 'SERVICE' then true else false end as is_servico,
    -- Flag de completude.
    case when cliente_id is not null then true else false end as has_cliente,
    false                                                     as has_vendedor,
    false                                                     as has_detalhe,
    current_timestamp                                         as fct_refreshed_at
from stg
