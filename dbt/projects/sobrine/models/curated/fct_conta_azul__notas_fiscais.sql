-- Curated: tabela fato de NFe (produto).
-- Granularidade: 1 row por emissao fiscal de produto.
-- Hoje a API so retorna EMITIDA e CORRIGIDA_SUCESSO — ainda assim
-- mantemos os flags pra futura compatibilidade quando outros status
-- (CANCELADA, etc) forem habilitados.
-- FKs: cliente_id -> dim_pessoas, venda_id -> fct_vendas.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__notas_fiscais') }}
)

select
    nota_id,
    numero,
    status,
    valor,
    data_emissao,
    chave_acesso,
    serie,
    venda_id,
    cliente_id,
    documento_tomador,
    case when upper(status) in ('EMITIDA', 'CORRIGIDA_SUCESSO') then true else false end as is_emitida,
    case when upper(status) = 'CANCELADA' then true else false end                       as is_cancelada,
    case when chave_acesso is not null then true else false end                          as has_chave_acesso,
    case when venda_id     is not null then true else false end                          as has_venda,
    current_timestamp                                                                     as fct_refreshed_at
from stg
