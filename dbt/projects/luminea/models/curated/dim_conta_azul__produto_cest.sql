-- Curated: dimensao de CESTs. PK natural = codigo.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__produto_cest') }}
)

select
    cest_codigo,
    descricao,
    ncm_relacionado,
    case when ncm_relacionado is not null then true else false end as has_ncm_relacionado,
    current_timestamp as dim_refreshed_at
from stg
