-- Staging: info da empresa conectada (1 row por tenant).

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__conta_conectada') }}
)

select
    pk                               as pk,
    nullif(trim(documento), '')      as documento,
    nullif(trim(razao_social), '')   as razao_social,
    nullif(trim(nome_fantasia), '')  as nome_fantasia,
    nullif(lower(trim(email)), '')   as email,
    nullif(trim(data_fundacao), '')  as data_fundacao,
    raw,
    loaded_at                        as raw_loaded_at,
    current_timestamp                as staged_at
from source
