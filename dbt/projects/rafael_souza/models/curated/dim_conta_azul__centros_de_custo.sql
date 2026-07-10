-- Curated: dimensao de centros de custo.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__centros_de_custo') }}
)

select
    centro_de_custo_id,
    centro_de_custo_nome,
    descricao,
    ativo,
    current_timestamp as dim_refreshed_at
from stg
