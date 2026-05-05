-- Staging: marcas e-commerce. Volume tipicamente 0.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__produto_ecommerce_marcas') }}
)

select
    id::uuid                       as marca_ecom_id,
    trim(nome)                     as marca_nome,
    raw,
    loaded_at                      as raw_loaded_at,
    current_timestamp              as staged_at
from source
