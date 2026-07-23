-- Delivery: despesa (contas a pagar) por DIA de competencia.
-- Grao diario viabiliza o filtro de periodo por data no dashboard. Exclui
-- cancelados. Substitui fin_despesa_mensal. View (ver fin_receita_diaria).
{{ config(materialized='view') }}

with pagar as (
    select * from {{ ref('fct_conta_azul__contas_a_pagar') }}
)

select
    data_competencia::date              as dia,
    count(*)::int                       as qtd,
    sum(valor_total)::numeric(14,2)     as total,
    sum(valor_pago)::numeric(14,2)      as pago,
    sum(valor_nao_pago)::numeric(14,2)  as em_aberto
from pagar
where data_competencia is not null
  and not is_cancelado
group by 1
order by 1
