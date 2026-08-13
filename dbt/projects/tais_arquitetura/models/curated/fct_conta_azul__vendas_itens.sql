-- Curated: fato de itens de venda (linhas de pedido).
-- Granularidade: 1 row = 1 SKU vendido. Habilita mix de produtos,
-- ticket medio por SKU, margem por item.
-- FK: venda_id -> fct_vendas, produto_id -> dim_produtos, servico_id -> dim_servicos.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__vendas_itens') }}
)

select
    venda_item_id,
    venda_id,
    produto_id,
    servico_id,
    descricao,
    codigo,
    quantidade,
    valor_unitario,
    valor_desconto,
    valor_total,
    -- Valor unitario liquido (apos desconto) — base pra margem por linha.
    case when quantidade > 0
         then (valor_total / quantidade)
         else null
    end                                                                        as valor_unitario_liquido,
    cfop,
    ncm,
    unidade,
    case when produto_id is not null then true else false end                  as is_produto,
    case when servico_id is not null then true else false end                  as is_servico,
    case when valor_desconto > 0     then true else false end                  as tem_desconto,
    current_timestamp                                                          as fct_refreshed_at
from stg
