-- Delivery: vendas por DIA e tipo de item (PRODUCT/SERVICE) — pagina Vendas
-- do dashboard (KPIs, barras, ticket medio, quebra produtos x servicos).
-- Usa valor_total (valor_liquido vem NULL da API de vendas). Exclui canceladas.
{{ config(materialized='view') }}

with vendas as (
    select * from {{ ref('fct_conta_azul__vendas') }}
)

select
    data_venda::date                    as dia,
    coalesce(tipo_item, 'OUTRO')        as tipo_item,
    count(*)::int                       as qtd,
    sum(valor_total)::numeric(14,2)     as valor_total
from vendas
where data_venda is not null
  and not coalesce(is_cancelada, false)
group by 1, 2
order by 1, 2
