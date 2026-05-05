-- Staging: pessoas do Conta Azul, normalizadas sem regras de negocio.
--
-- Materializacao incremental: a cada run pega so linhas com loaded_at
-- maior que o ultimo staged_at. Em runs subsequentes, evita reprocessar
-- a tabela inteira (importante quando o tenant tiver milhoes de pessoas).
--
-- A primeira execucao faz table cheia (is_incremental() = false).
--
-- Naming: nome da entidade (pessoa) atravessa raw -> staging -> curated.
-- So delivery renomeia pra business name (cliente, fornecedor, ...).

{{ config(
    materialized='incremental',
    unique_key='pessoa_id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__pessoas') }}

    {% if is_incremental() %}
      where loaded_at > (
          select coalesce(max(staged_at), '1900-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

renamed as (
    select
        id::uuid                              as pessoa_id,
        uuid_legado                           as pessoa_uuid_legado,
        id_legado                             as pessoa_id_legado,
        trim(nome)                            as pessoa_nome,
        nullif(lower(trim(email)), '')        as email,
        nullif(trim(documento), '')           as documento,
        case
            when lower(trim(tipo_pessoa)) in ('fisica', 'física')     then 'pf'
            when lower(trim(tipo_pessoa)) in ('juridica', 'jurídica') then 'pj'
            else null
        end                                   as tipo_pessoa,
        nullif(trim(telefone), '')            as telefone,
        coalesce(ativo, true)                 as ativo,
        perfis                                as perfis_raw,        -- jsonb (lista de strings)
        data_criacao                          as source_created_at,
        data_alteracao                        as source_updated_at,
        loaded_at                             as raw_loaded_at,
        current_timestamp                     as staged_at
    from source
)

select * from renamed
