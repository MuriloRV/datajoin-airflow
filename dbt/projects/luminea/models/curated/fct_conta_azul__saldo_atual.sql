-- Curated: fato de snapshot diario de saldo por conta financeira.
-- Granularidade: 1 row = 1 conta x snapshot_date. Historico append-only.
-- FK: conta_financeira_id -> dim_contas_financeiras.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__saldo_atual') }}
)

select
    saldo_atual_id,
    conta_financeira_id,
    saldo,
    snapshot_date,
    data_snapshot,
    case when saldo < 0 then true else false end                               as is_negativo,
    case when saldo = 0 then true else false end                               as is_zerado,
    current_timestamp                                                          as fct_refreshed_at
from stg
