-- Staging: produtos do catalogo.
-- Volume potencialmente medio — incremental por loaded_at evita
-- reprocessar tudo em runs subsequentes.

{{ config(
    materialized='incremental',
    unique_key='produto_id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__produtos') }}

    {% if is_incremental() %}
      where loaded_at > (
          select coalesce(max(staged_at), '1900-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

renamed as (
    select
        id::uuid                        as produto_id,
        nullif(trim(nome), '')          as produto_nome,
        nullif(trim(codigo), '')        as codigo,
        nullif(trim(descricao), '')     as descricao,
        preco,
        custo,
        estoque,
        estoque_minimo,
        nullif(trim(unidade), '')       as unidade,
        case lower(trim(status))
            when 'ativo'   then 'ativo'
            when 'inativo' then 'inativo'
            else null
        end                             as status,
        data_criacao                    as source_created_at,
        data_alteracao                  as source_updated_at,
        loaded_at                       as raw_loaded_at,
        current_timestamp               as staged_at
    from source
)

select * from renamed
