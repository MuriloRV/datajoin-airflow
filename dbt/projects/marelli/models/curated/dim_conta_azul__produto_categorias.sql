-- Curated: dimensao de categorias de produto.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__produto_categorias') }}
)

select
    categoria_id,
    categoria_nome,
    descricao,
    ativo,
    current_timestamp as dim_refreshed_at
from stg
