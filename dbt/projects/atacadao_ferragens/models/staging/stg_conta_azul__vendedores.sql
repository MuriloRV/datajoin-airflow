-- Staging: vendedores. Lookup minimal.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__vendedores') }}
)

select
    id::uuid             as vendedor_id,
    trim(nome)           as vendedor_nome,
    loaded_at            as raw_loaded_at,
    current_timestamp    as staged_at
from source
