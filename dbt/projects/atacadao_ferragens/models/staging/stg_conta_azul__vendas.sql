-- Staging: vendas do Conta Azul. Extrai cliente_id/cliente_nome,
-- vendedor_id/vendedor_nome dos JSONB nested e prepara escalares.
-- Itens da venda (line items) ficam como jsonb pra futura expansao em
-- fct_vendas_itens (1 linha por item).

{{ config(
    materialized='incremental',
    unique_key='venda_id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__vendas') }}

    {% if is_incremental() %}
      where loaded_at > (
          select coalesce(max(staged_at), '1900-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

renamed as (
    select
        id::uuid                                       as venda_id,
        numero                                         as venda_numero,
        nullif(trim(situacao), '')                     as situacao,
        nullif(trim(tipo), '')                         as tipo,
        -- tipo_item = enum PRODUCT/SERVICE (o "itens" enganoso da API).
        -- NAO confundir com lista de line items — o endpoint de busca nao
        -- retorna line items; pra isso usar /v1/venda/{id} (futuro).
        nullif(trim(tipo_item), '')                    as tipo_item,
        nullif(trim(origem), '')                       as origem,
        coalesce(pendente, false)                      as pendente,
        coalesce(condicao_pagamento, false)            as condicao_pagamento,
        versao,
        total                                          as valor_total,
        desconto                                       as valor_desconto,
        data_venda,
        criado_em                                      as source_created_at,
        data_alteracao                                 as source_updated_at,
        nullif(trim(observacoes), '')                  as observacoes,
        -- Cliente/vendedor vem como objeto unico {id, nome}.
        (cliente->>'id')::uuid                         as cliente_id,
        nullif(trim(cliente->>'nome'), '')             as cliente_nome,
        nullif(lower(trim(cliente->>'email')), '')     as cliente_email,
        (vendedor->>'id')::uuid                        as vendedor_id,
        nullif(trim(vendedor->>'nome'), '')            as vendedor_nome,
        id_legado                                      as venda_id_legado,
        id_legado_cliente                              as cliente_id_legado,
        id_legado_dono                                 as dono_id_legado,
        loaded_at                                      as raw_loaded_at,
        current_timestamp                              as staged_at
    from source
)

select * from renamed
