-- Staging: NFS-e. Schema tentativo (Luminea retorna 0).

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__notas_servico') }}
)

select
    id::uuid                       as nota_id,
    nullif(trim(numero), '')       as numero,
    nullif(trim(status), '')       as status,
    valor,
    data_emissao,
    data_competencia,
    cliente_id::uuid               as cliente_id,
    raw,
    loaded_at                      as raw_loaded_at,
    current_timestamp              as staged_at
from source
