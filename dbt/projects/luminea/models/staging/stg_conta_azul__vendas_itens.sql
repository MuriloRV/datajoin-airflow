-- Staging: itens (linhas) de venda. 1 row = 1 SKU vendido. Habilita
-- mix de produtos, ticket medio por SKU, margem por item.

{{ config(
    materialized='incremental',
    unique_key='venda_item_id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__vendas_itens') }}

    {% if is_incremental() %}
      where loaded_at > (
          select coalesce(max(staged_at), '1900-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

renamed as (
    select
        id::uuid                       as venda_item_id,
        venda_id::uuid                 as venda_id,
        produto_id::uuid               as produto_id,
        servico_id::uuid               as servico_id,
        nullif(trim(descricao), '')    as descricao,
        nullif(trim(codigo), '')       as codigo,
        quantidade,
        valor_unitario,
        desconto                       as valor_desconto,
        valor_total,
        nullif(trim(cfop), '')         as cfop,
        nullif(trim(ncm), '')          as ncm,
        nullif(trim(unidade), '')      as unidade,
        loaded_at                      as raw_loaded_at,
        current_timestamp              as staged_at
    from source
)

select * from renamed
