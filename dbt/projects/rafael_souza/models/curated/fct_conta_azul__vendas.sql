-- Curated: tabela fato de vendas com flags derivados pra analise.
-- Granularidade: 1 linha por venda (transacao). Itens (linha-a-linha)
-- ficam em fct_vendas_itens.
-- FKs: cliente_id -> dim_pessoas, vendedor_id -> dim_vendedores.
--
-- Enriquecido via LEFT JOIN com stg_vendas_detalhe pra trazer
-- valor_bruto/desconto/frete/impostos/liquido + condicao_pagamento e
-- num_parcelas (ausentes no agregado /venda/busca).

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__vendas') }}
),

detalhe as (
    select * from {{ ref('stg_conta_azul__vendas_detalhe') }}
),

joined as (
    select
        s.venda_id,
        s.venda_id_legado,
        s.venda_numero,
        s.situacao,
        s.tipo,
        s.tipo_item,
        s.origem,
        s.pendente,
        s.condicao_pagamento,
        s.versao,
        s.valor_total,
        s.valor_desconto,
        s.data_venda,
        s.source_created_at,
        s.source_updated_at,
        s.observacoes,
        s.cliente_id,
        s.cliente_nome,
        s.cliente_email,
        s.cliente_id_legado,
        s.vendedor_id,
        s.vendedor_nome,
        s.dono_id_legado,
        -- Campos do detalhe (NULL quando vendas_detalhe nao foi puxado).
        d.valor_bruto                            as valor_bruto_detalhe,
        d.valor_frete,
        d.valor_impostos,
        d.valor_liquido,
        d.natureza_operacao_nome,
        d.condicao_pagamento                     as condicao_pagamento_detalhe,
        d.num_parcelas
    from stg s
    left join detalhe d using (venda_id)
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
    valor_bruto_detalhe,
    valor_frete,
    valor_impostos,
    valor_liquido,
    natureza_operacao_nome,
    condicao_pagamento_detalhe,
    num_parcelas,
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
    case when cliente_id   is not null then true else false end as has_cliente,
    case when vendedor_id  is not null then true else false end as has_vendedor,
    case when valor_liquido is not null then true else false end as has_detalhe,
    current_timestamp                                            as fct_refreshed_at
from joined
