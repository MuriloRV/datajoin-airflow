-- Staging: detalhe de pessoa (escalares so). Os arrays nested
-- (enderecos, outros_contatos, inscricoes) viram models proprios
-- expandidos via jsonb_array_elements.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__pessoas_detalhe') }}
)

select
    id::uuid                                  as pessoa_id,
    trim(nome)                                as pessoa_nome,
    nullif(trim(documento), '')               as documento,
    nullif(lower(trim(email)), '')            as email,
    nullif(trim(telefone_comercial), '')      as telefone_comercial,
    nullif(trim(telefone_celular), '')        as telefone_celular,
    case
        when lower(trim(tipo_pessoa)) in ('fisica', 'física')     then 'pf'
        when lower(trim(tipo_pessoa)) in ('juridica', 'jurídica') then 'pj'
        else null
    end                                       as tipo_pessoa,
    nullif(trim(nome_empresa), '')            as nome_empresa,
    nullif(trim(rg), '')                      as rg,
    nullif(trim(data_nascimento), '')         as data_nascimento,
    coalesce(optante_simples_nacional, false) as optante_simples_nacional,
    coalesce(orgao_publico, false)            as orgao_publico,
    nullif(trim(observacao), '')              as observacao,
    nullif(trim(codigo), '')                  as codigo,
    coalesce(ativo, true)                     as ativo,
    loaded_at                                 as raw_loaded_at,
    current_timestamp                         as staged_at
from source
