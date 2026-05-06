-- Curated: fato de itens de NFe (linhas da nota com tributos por linha).
-- Granularidade: 1 row = 1 produto na nota fiscal.
-- FK: nota_fiscal_id -> fct_notas_fiscais, produto_id -> dim_produtos.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__notas_fiscais_itens') }}
)

select
    nota_item_id,
    nota_fiscal_id,
    chave_acesso,
    numero_item,
    produto_id,
    codigo,
    descricao,
    ncm,
    cfop,
    quantidade,
    valor_unitario,
    valor_total,
    valor_icms,
    valor_ipi,
    valor_pis,
    valor_cofins,
    -- Total de tributos por linha — facilita analise de carga tributaria.
    coalesce(valor_icms, 0) + coalesce(valor_ipi, 0)
        + coalesce(valor_pis, 0) + coalesce(valor_cofins, 0)                   as valor_tributos_total,
    case when produto_id is not null then true else false end                  as has_produto,
    current_timestamp                                                          as fct_refreshed_at
from stg
