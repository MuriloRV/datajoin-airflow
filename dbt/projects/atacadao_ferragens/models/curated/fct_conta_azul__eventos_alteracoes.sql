-- Curated: log auxiliar de IDs de eventos financeiros alterados.
-- Granularidade: 1 row por evento alterado. Tabela CDC — usar em
-- joins pra detectar quais ids contas_a_receber/pagar mudaram fora
-- da janela rolante de 5 anos.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__eventos_alteracoes') }}
)

select
    evento_id,
    source_updated_at,
    tipo_evento,
    case when upper(coalesce(tipo_evento, '')) = 'CONTAS_RECEBER' then true else false end as is_contas_a_receber,
    case when upper(coalesce(tipo_evento, '')) = 'CONTAS_PAGAR'   then true else false end as is_contas_a_pagar,
    current_timestamp                                                                       as fct_refreshed_at
from stg
