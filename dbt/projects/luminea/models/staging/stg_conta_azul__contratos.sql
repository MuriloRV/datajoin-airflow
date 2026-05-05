-- Staging: contratos. Schema TENTATIVO — ajustar quando aparecer dado real.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__contratos') }}
)

select
    id::uuid                       as contrato_id,
    nullif(trim(numero), '')       as contrato_numero,
    nullif(trim(descricao), '')    as descricao,
    cliente_id::uuid               as cliente_id,
    valor,
    data_inicio                    as data_inicio,
    data_fim                       as data_fim,
    nullif(trim(status), '')       as status,
    data_criacao                   as source_created_at,
    data_alteracao                 as source_updated_at,
    raw,                            -- jsonb cru pra investigacao
    loaded_at                      as raw_loaded_at,
    current_timestamp              as staged_at
from source
