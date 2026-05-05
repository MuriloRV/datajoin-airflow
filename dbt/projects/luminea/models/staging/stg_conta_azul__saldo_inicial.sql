-- Staging: saldos iniciais por conta financeira. Granularidade tentativa
-- (provavel: 1 row por conta x periodo). Schema sera afinado quando
-- aparecer dado real — raw jsonb preserva tudo.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__saldo_inicial') }}
)

select
    id::uuid                                       as saldo_id,
    conta_financeira_id::uuid                      as conta_financeira_id,
    nullif(trim(conta_financeira->>'nome'), '')    as conta_financeira_nome,
    data_referencia,
    saldo,
    saldo_inicial,
    raw,
    loaded_at                                      as raw_loaded_at,
    current_timestamp                              as staged_at
from source
