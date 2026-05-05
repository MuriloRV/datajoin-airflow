-- Curated: tabela fato de vendas com flags derivados pra analise.
-- Granularidade: 1 linha por venda (transacao). Itens (linha-a-linha)
-- ficam em fct futuro com granularidade venda x item via jsonb expansion.
-- FKs: cliente_id -> dim_pessoas, vendedor_id -> dim_vendedores.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__vendas') }}
),

with_flags as (
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
        case when cliente_id  is not null then true else false end as has_cliente,
        case when vendedor_id is not null then true else false end as has_vendedor,
        current_timestamp                                          as fct_refreshed_at
    from stg
)

select * from with_flags
