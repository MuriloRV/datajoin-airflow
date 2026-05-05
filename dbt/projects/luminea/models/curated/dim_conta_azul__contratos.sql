-- Curated: dimensao de contratos com flags is_ativo/is_vencido.
-- Schema tentativo — Luminea retorna 0; ajustar campos quando aparecer
-- dado real (raw jsonb preservado em stg pra investigacao).

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__contratos') }}
)

select
    contrato_id,
    contrato_numero,
    descricao,
    cliente_id,
    valor,
    data_inicio,
    data_fim,
    status,
    source_created_at,
    source_updated_at,
    -- Flags derivados.
    case
        when data_fim is null then null
        when data_fim < current_timestamp then true
        else false
    end                                                       as is_vencido,
    case when upper(status) = 'ATIVO' then true else false end as is_ativo,
    case when cliente_id is not null then true else false end as has_cliente,
    current_timestamp                                          as dim_refreshed_at
from stg
