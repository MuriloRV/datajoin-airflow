-- Curated: dimensao de marcas e-commerce.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__produto_ecommerce_marcas') }}
)

select
    marca_ecom_id,
    marca_nome,
    current_timestamp as dim_refreshed_at
from stg
