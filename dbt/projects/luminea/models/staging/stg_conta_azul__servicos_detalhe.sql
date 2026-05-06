-- Staging: detalhe completo de servico. Acrescenta ISS, codigo de
-- servico municipal, observacoes fiscais ao agregado da listagem.

{{ config(
    materialized='incremental',
    unique_key='servico_id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__servicos_detalhe') }}

    {% if is_incremental() %}
      where loaded_at > (
          select coalesce(max(staged_at), '1900-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

renamed as (
    select
        id::uuid                                  as servico_id,
        nullif(trim(nome), '')                    as servico_nome,
        nullif(trim(codigo), '')                  as codigo,
        nullif(trim(descricao), '')               as descricao,
        preco,
        custo,
        aliquota_iss,
        nullif(trim(codigo_servico_municipal), '') as codigo_servico_municipal,
        tributacao,                               -- jsonb cru (ISS/PIS/COFINS)
        nullif(trim(observacao), '')              as observacao,
        case lower(trim(status))
            when 'ativo'   then 'ativo'
            when 'inativo' then 'inativo'
            else null
        end                                       as status,
        case lower(trim(tipo_servico))
            when 'prestado' then 'prestado'
            when 'tomado'   then 'tomado'
            else null
        end                                       as tipo_servico,
        data_criacao                              as source_created_at,
        data_alteracao                            as source_updated_at,
        loaded_at                                 as raw_loaded_at,
        current_timestamp                         as staged_at
    from source
)

select * from renamed
