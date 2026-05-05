-- Staging: codigos CEST. PK natural = codigo.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__produto_cest') }}
)

select
    trim(codigo)                       as cest_codigo,
    nullif(trim(descricao), '')        as descricao,
    nullif(trim(ncm_relacionado), '')  as ncm_relacionado,
    raw,
    loaded_at                          as raw_loaded_at,
    current_timestamp                  as staged_at
from source
