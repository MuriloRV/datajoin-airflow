-- Curated: dimensao de vendedores. Master data simples — passa staging
-- direto adicionando dim_refreshed_at. Joinable em fct_vendas via vendedor_id.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__vendedores') }}
)

select
    vendedor_id,
    vendedor_nome,
    current_timestamp as dim_refreshed_at
from stg
