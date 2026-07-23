-- Staging: detalhe de parcelas de eventos financeiros (contas a receber/pagar).
-- Granularidade: 1 row por parcela. Aqui mora a `data_pagamento` que o
-- agregado /buscar nao expoe.

{{ config(
    materialized='incremental',
    unique_key='parcela_id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__parcelas_detalhe') }}

    {% if is_incremental() %}
      where loaded_at > (
          select coalesce(max(staged_at), '1900-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

renamed as (
    select
        id::uuid                            as parcela_id,
        evento_financeiro_id::uuid          as evento_financeiro_id,
        numero_parcela,
        valor                               as valor_parcela,
        valor_bruto,
        valor_liquido,
        desconto                            as valor_desconto,
        juros                               as valor_juros,
        multa                               as valor_multa,
        data_vencimento,
        data_pagamento,
        nullif(trim(status), '')            as status,
        nullif(trim(forma_pagamento), '')   as forma_pagamento,
        conta_financeira_id::uuid           as conta_financeira_id,
        nullif(trim(conta_financeira->>'nome'), '') as conta_financeira_nome,
        data_criacao                        as source_created_at,
        data_alteracao                      as source_updated_at,
        loaded_at                           as raw_loaded_at,
        current_timestamp                   as staged_at
    from source
)

select * from renamed
