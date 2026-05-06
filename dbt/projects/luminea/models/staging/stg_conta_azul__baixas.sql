-- Staging: baixas (pagamentos efetivos) de parcelas. 1 parcela pode ter
-- N baixas (pagamentos parciais). Verdade absoluta sobre quando entrou
-- caixa — base pra DRE de caixa, prazo medio de recebimento.

{{ config(
    materialized='incremental',
    unique_key='baixa_id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__baixas') }}

    {% if is_incremental() %}
      where loaded_at > (
          select coalesce(max(staged_at), '1900-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

renamed as (
    select
        id::uuid                            as baixa_id,
        parcela_id::uuid                    as parcela_id,
        data_pagamento,
        valor                               as valor_pago,
        juros                               as valor_juros,
        multa                               as valor_multa,
        desconto                            as valor_desconto,
        nullif(trim(metodo_pagamento), '')  as metodo_pagamento,
        nullif(trim(forma_pagamento), '')   as forma_pagamento,
        conta_financeira_id::uuid           as conta_financeira_id,
        nullif(trim(conta_financeira->>'nome'), '') as conta_financeira_nome,
        nullif(trim(observacao), '')        as observacao,
        data_criacao                        as source_created_at,
        data_alteracao                      as source_updated_at,
        loaded_at                           as raw_loaded_at,
        current_timestamp                   as staged_at
    from source
)

select * from renamed
