-- Staging: detalhe completo de produto. Acrescenta tributacao, dimensoes
-- (peso/altura/largura/comprimento), fotos, NCM/CEST detalhados ao
-- agregado da listagem.

{{ config(
    materialized='incremental',
    unique_key='produto_id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__produtos_detalhe') }}

    {% if is_incremental() %}
      where loaded_at > (
          select coalesce(max(staged_at), '1900-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

renamed as (
    select
        id::uuid                       as produto_id,
        nullif(trim(nome), '')         as produto_nome,
        nullif(trim(codigo), '')       as codigo,
        nullif(trim(descricao), '')    as descricao,
        preco,
        custo,
        estoque,
        estoque_minimo,
        nullif(trim(ncm), '')          as ncm,
        nullif(trim(cest), '')         as cest,
        nullif(trim(unidade), '')      as unidade,
        peso,
        altura,
        largura,
        comprimento,
        tributacao,                    -- jsonb cru (ICMS/IPI/PIS/COFINS)
        fotos,                          -- jsonb array
        case lower(trim(status))
            when 'ativo'   then 'ativo'
            when 'inativo' then 'inativo'
            else null
        end                            as status,
        data_criacao                   as source_created_at,
        data_alteracao                 as source_updated_at,
        loaded_at                      as raw_loaded_at,
        current_timestamp              as staged_at
    from source
)

select * from renamed
