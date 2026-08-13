-- Staging: codigos NCM. PK natural = codigo.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__produto_ncm') }}
)

select
    trim(codigo)                  as ncm_codigo,
    nullif(trim(descricao), '')   as descricao,
    raw,
    loaded_at                     as raw_loaded_at,
    current_timestamp             as staged_at
from source
