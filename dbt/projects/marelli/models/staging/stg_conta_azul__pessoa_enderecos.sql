-- Staging: enderecos de pessoa (expansao do nested jsonb).
-- 1 row por endereco (1:N com pessoa).

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__pessoas_detalhe') }}
),

unnested as (
    select
        id::uuid                                              as pessoa_id,
        ord                                                   as endereco_seq,
        nullif(trim(end_obj->>'cep'), '')                     as cep,
        shared.normalize_cep(end_obj->>'cep')                 as cep_normalizado,
        nullif(trim(end_obj->>'logradouro'), '')              as logradouro,
        nullif(trim(end_obj->>'numero'), '')                  as numero,
        nullif(trim(end_obj->>'complemento'), '')             as complemento,
        nullif(trim(end_obj->>'bairro'), '')                  as bairro,
        nullif(trim(end_obj->>'cidade'), '')                  as cidade,
        nullif((end_obj->>'id_cidade')::int, 0)               as id_cidade,
        nullif(trim(end_obj->>'estado'), '')                  as estado,
        nullif(trim(end_obj->>'pais'), '')                    as pais,
        loaded_at                                             as raw_loaded_at,
        current_timestamp                                     as staged_at
    from source,
         lateral jsonb_array_elements(coalesce(enderecos, '[]'::jsonb)) with ordinality as t(end_obj, ord)
)

select * from unnested
where cep is not null or logradouro is not null or cidade is not null
