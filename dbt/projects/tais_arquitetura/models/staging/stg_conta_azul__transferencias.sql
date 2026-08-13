-- Staging: transferencias entre contas. Granularidade: 1 row = 1 transferencia.

{{ config(
    materialized='incremental',
    unique_key='transferencia_id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__transferencias') }}

    {% if is_incremental() %}
      where loaded_at > (
          select coalesce(max(staged_at), '1900-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
)

select
    id::uuid                                   as transferencia_id,
    data_transferencia,
    valor,
    nullif(trim(descricao), '')                as descricao,
    conta_origem_id::uuid                      as conta_origem_id,
    nullif(trim(conta_origem->>'nome'), '')    as conta_origem_nome,
    conta_destino_id::uuid                     as conta_destino_id,
    nullif(trim(conta_destino->>'nome'), '')   as conta_destino_nome,
    data_criacao                               as source_created_at,
    data_alteracao                             as source_updated_at,
    raw,
    loaded_at                                  as raw_loaded_at,
    current_timestamp                          as staged_at
from source
