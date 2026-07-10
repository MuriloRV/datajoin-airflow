-- Curated: dimensao de NCMs. PK natural = codigo.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__produto_ncm') }}
)

select
    ncm_codigo,
    descricao,
    current_timestamp as dim_refreshed_at
from stg
