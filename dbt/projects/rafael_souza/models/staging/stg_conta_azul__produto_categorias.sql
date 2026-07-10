-- Staging: categorias de produto.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__produto_categorias') }}
)

select
    id                             as categoria_id,
    nullif(trim(nome), '')         as categoria_nome,
    nullif(trim(descricao), '')    as descricao,
    coalesce(ativo, true)          as ativo,
    raw,
    loaded_at                      as raw_loaded_at,
    current_timestamp              as staged_at
from source
