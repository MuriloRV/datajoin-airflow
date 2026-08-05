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
    nullif(trim(raw->>'tipo'), '')                 as tipo,
    saldo,
    saldo_inicial,
    -- API manda saldo_inicial sempre POSITIVO; o sinal vem de tipo
    -- (RECEITA=+ / DESPESA=-). saldo_inicial_liquido = valor com sinal.
    case when upper(raw->>'tipo') = 'DESPESA'
         then -coalesce(saldo_inicial, 0)
         else  coalesce(saldo_inicial, 0)
    end                                            as saldo_inicial_liquido,
    raw,
    loaded_at                                      as raw_loaded_at,
    current_timestamp                              as staged_at
from source
