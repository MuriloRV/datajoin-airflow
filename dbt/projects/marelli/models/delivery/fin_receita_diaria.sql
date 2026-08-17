-- Delivery: receita (contas a receber) por DIA de competencia.
-- Grao diario viabiliza o filtro de periodo por data no dashboard (a agregacao
-- mensal e feita client-side). Exclui cancelados. Substitui fin_receita_mensal.
-- View (dado pequeno, sempre fresco; dashboards leem com cache curto).
{{ config(materialized='view') }}

with receber as (
    select * from {{ ref('fct_conta_azul__contas_a_receber') }}
)

select
    data_competencia::date              as dia,
    count(*)::int                       as qtd,
    sum(valor_total)::numeric(14,2)     as faturamento,
    sum(valor_pago)::numeric(14,2)      as recebido,
    sum(valor_nao_pago)::numeric(14,2)  as em_aberto
from receber
where data_competencia is not null
  and not is_cancelado
group by 1
order by 1
