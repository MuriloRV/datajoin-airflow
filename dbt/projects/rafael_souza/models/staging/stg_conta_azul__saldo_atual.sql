-- Staging: snapshot diario de saldo por conta financeira. PK natural
-- e' (conta_id, snapshot_date) — historico append-only por dia.

{{ config(
    materialized='incremental',
    unique_key='saldo_atual_id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__saldo_atual') }}

    {% if is_incremental() %}
      where loaded_at > (
          select coalesce(max(staged_at), '1900-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

renamed as (
    select
        id::uuid                            as saldo_atual_id,
        conta_financeira_id::uuid           as conta_financeira_id,
        coalesce(saldo_atual, saldo)        as saldo,
        snapshot_date,
        data_snapshot,
        loaded_at                           as raw_loaded_at,
        current_timestamp                   as staged_at
    from source
)

select * from renamed
