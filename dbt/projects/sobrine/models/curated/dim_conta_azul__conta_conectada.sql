-- Curated: info da empresa conectada (1 row por tenant).
-- PK natural = documento (CNPJ).

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__conta_conectada') }}
)

select
    pk,
    documento,
    razao_social,
    nome_fantasia,
    email,
    data_fundacao,
    case when documento is not null then true else false end as has_documento,
    case when email     is not null then true else false end as has_email,
    current_timestamp as dim_refreshed_at
from stg
