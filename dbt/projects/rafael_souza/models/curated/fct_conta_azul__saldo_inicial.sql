-- Curated: tabela fato de saldos iniciais por conta x periodo.
-- Granularidade tentativa — afinar quando aparecer dado real.
-- FK: conta_financeira_id -> dim_contas_financeiras.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__saldo_inicial') }}
)

select
    saldo_id,
    conta_financeira_id,
    conta_financeira_nome,
    data_referencia,
    saldo,
    saldo_inicial,
    case when conta_financeira_id is not null then true else false end as has_conta,
    current_timestamp as fct_refreshed_at
from stg
