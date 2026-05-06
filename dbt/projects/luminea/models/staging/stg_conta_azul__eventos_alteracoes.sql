-- Staging: log de IDs de eventos financeiros alterados (CDC oficial).
-- Tabela auxiliar — captura mudancas em eventos cuja data_vencimento
-- caiu fora da janela de 5 anos do contas_a_*.

{{ config(
    materialized='incremental',
    unique_key='evento_id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__eventos_alteracoes') }}

    {% if is_incremental() %}
      where loaded_at > (
          select coalesce(max(staged_at), '1900-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

renamed as (
    select
        id::uuid                       as evento_id,
        data_alteracao                 as source_updated_at,
        nullif(trim(tipo_evento), '')  as tipo_evento,
        loaded_at                      as raw_loaded_at,
        current_timestamp              as staged_at
    from source
)

select * from renamed
