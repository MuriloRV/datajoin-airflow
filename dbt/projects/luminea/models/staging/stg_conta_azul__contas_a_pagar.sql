-- Staging: contas a pagar. Espelha receber, com fornecedor_id em vez de cliente.

{{ config(
    materialized='incremental',
    unique_key='evento_id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__contas_a_pagar') }}

    {% if is_incremental() %}
      where loaded_at > (
          select coalesce(max(staged_at), '1900-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

renamed as (
    select
        id::uuid                                       as evento_id,
        nullif(trim(status), '')                       as status,
        nullif(trim(status_traduzido), '')             as status_traduzido,
        total,
        nao_pago                                       as valor_nao_pago,
        pago                                           as valor_pago,
        nullif(trim(descricao), '')                    as descricao,
        data_vencimento,
        data_competencia,
        data_criacao                                   as source_created_at,
        data_alteracao                                 as source_updated_at,
        (fornecedor->>'id')::uuid                      as fornecedor_id,
        nullif(trim(fornecedor->>'nome'), '')          as fornecedor_nome,
        coalesce(jsonb_array_length(categorias), 0)        as num_categorias,
        coalesce(jsonb_array_length(centros_de_custo), 0)  as num_centros_de_custo,
        case when renegociacao is not null then true else false end as foi_renegociado,
        loaded_at                                      as raw_loaded_at,
        current_timestamp                              as staged_at
    from source
)

select * from renamed
