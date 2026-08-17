-- Staging: detalhe completo de venda. Complementa o agregado de
-- /venda/busca com condicao_pagamento, parcelas, frete, impostos,
-- natureza_operacao, endereco_entrega.

{{ config(
    materialized='incremental',
    unique_key='venda_id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__vendas_detalhe') }}

    {% if is_incremental() %}
      where loaded_at > (
          select coalesce(max(staged_at), '1900-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

renamed as (
    select
        id::uuid                       as venda_id,
        numero                         as venda_numero,
        nullif(trim(situacao), '')     as situacao,
        data_emissao,
        data_alteracao                 as source_updated_at,
        valor_bruto,
        valor_desconto,
        valor_frete,
        valor_impostos,
        valor_liquido,
        nullif(trim(observacoes), '')  as observacoes,
        (cliente->>'id')::uuid         as cliente_id,
        nullif(trim(cliente->>'nome'), '') as cliente_nome,
        (vendedor->>'id')::uuid        as vendedor_id,
        nullif(trim(vendedor->>'nome'), '') as vendedor_nome,
        nullif(trim(natureza_operacao->>'nome'), '') as natureza_operacao_nome,
        condicao_pagamento,            -- jsonb cru pra expansao downstream
        parcelas,                       -- jsonb (lista de parcelas da venda)
        endereco_entrega,
        coalesce(jsonb_array_length(parcelas), 0) as num_parcelas,
        loaded_at                      as raw_loaded_at,
        current_timestamp              as staged_at
    from source
)

select * from renamed
