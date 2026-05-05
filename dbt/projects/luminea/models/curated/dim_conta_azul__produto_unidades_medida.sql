-- Curated: dimensao de unidades de medida.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__produto_unidades_medida') }}
)

select
    unidade_id,
    sigla,
    descricao,
    fracionavel,
    current_timestamp as dim_refreshed_at
from stg
