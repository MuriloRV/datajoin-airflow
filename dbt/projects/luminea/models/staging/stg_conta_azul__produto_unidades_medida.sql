-- Staging: unidades de medida.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__produto_unidades_medida') }}
)

select
    id                             as unidade_id,
    nullif(trim(sigla), '')        as sigla,
    nullif(trim(descricao), '')    as descricao,
    coalesce(fracionavel, false)   as fracionavel,
    raw,
    loaded_at                      as raw_loaded_at,
    current_timestamp              as staged_at
from source
