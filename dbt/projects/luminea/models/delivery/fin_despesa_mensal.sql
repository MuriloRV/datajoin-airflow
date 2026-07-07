-- Delivery: despesa por (ano, mes) — tela Despesas (comparativos ano x ano,
-- mes x mes, media movel). Exclui cancelados. View (ver fin_receita_mensal).
{{ config(materialized='view') }}

with pagar as (
    select * from {{ ref('fct_conta_azul__contas_a_pagar') }}
)

select
    extract(year  from data_competencia)::int  as ano,
    extract(month from data_competencia)::int  as mes_num,
    count(*)::int                               as qtd,
    sum(valor_total)::numeric(14,2)             as total,
    sum(valor_pago)::numeric(14,2)              as pago,
    sum(valor_nao_pago)::numeric(14,2)          as em_aberto
from pagar
where data_competencia is not null
  and not is_cancelado
group by 1, 2
order by 1, 2
