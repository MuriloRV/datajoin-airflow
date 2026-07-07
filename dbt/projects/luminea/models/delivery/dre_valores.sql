-- Delivery: valores por (regime, categoria_id, ano, mes), com SINAL aplicado
-- (receita +1 / despesa -1). Alimenta as folhas da arvore do DRE (join por
-- categoria_id em dre_estrutura). Dois regimes: competencia (data_competencia)
-- e vencimento (data_vencimento) — toggle no frontend. Exclui cancelados.
{{ config(materialized='view') }}

with tx as (
    select categoria_id, 1 as sinal, valor_total, data_competencia, data_vencimento, is_cancelado
    from {{ ref('fct_conta_azul__contas_a_receber') }}
    union all
    select categoria_id, -1, valor_total, data_competencia, data_vencimento, is_cancelado
    from {{ ref('fct_conta_azul__contas_a_pagar') }}
),

norm as (
    select categoria_id, sinal, valor_total, 'competencia'::text as regime, data_competencia as dt
    from tx where not is_cancelado and data_competencia is not null
    union all
    select categoria_id, sinal, valor_total, 'vencimento', data_vencimento
    from tx where not is_cancelado and data_vencimento is not null
)

select
    regime,
    categoria_id,
    (extract(year from dt))::int                 as ano,
    to_char(date_trunc('month', dt), 'YYYY-MM')  as mes,
    sum(valor_total * sinal)::numeric(14,2)      as valor
from norm
where categoria_id is not null
group by 1, 2, 3, 4
order by regime, categoria_id, mes
