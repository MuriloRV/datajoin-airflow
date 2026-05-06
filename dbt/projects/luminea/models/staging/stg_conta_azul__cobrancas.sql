-- Staging: cobrancas (boletos/PIX) emitidas pra parcelas de contas a
-- receber. Status do funil: AGUARDANDO_CONFIRMACAO -> REGISTRADO ->
-- QUITADO ou CANCELADO/EXPIRADO.

{{ config(
    materialized='incremental',
    unique_key='cobranca_id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__cobrancas') }}

    {% if is_incremental() %}
      where loaded_at > (
          select coalesce(max(staged_at), '1900-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

renamed as (
    select
        id::uuid                       as cobranca_id,
        parcela_id::uuid               as parcela_id,
        evento_financeiro_id::uuid     as evento_financeiro_id,
        nullif(trim(status), '')       as status,
        nullif(trim(url), '')          as url,
        valor                          as valor_cobranca,
        nullif(trim(tipo), '')         as tipo,
        data_vencimento,
        data_emissao,
        data_alteracao                 as source_updated_at,
        loaded_at                      as raw_loaded_at,
        current_timestamp              as staged_at
    from source
)

select * from renamed
