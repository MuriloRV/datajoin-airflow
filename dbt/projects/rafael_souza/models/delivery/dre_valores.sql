-- Delivery: valores por (regime, categoria_id, DIA), com SINAL aplicado
-- (receita +1 / despesa -1). Alimenta as folhas da arvore do DRE (join por
-- categoria_id em dre_estrutura) e TODA analise por categoria/nivel do DRE no
-- dashboard (Despesas, Comparativo, Detalhamento) — a agregacao mensal/por
-- nivel e client-side; o grao diario habilita o filtro por data.
-- Dois regimes: competencia (data_competencia) e vencimento (data_vencimento).
-- Exclui cancelados.
--
-- pre_hook: dropa a relacao antes de recriar. O grao mudou (mensal ano/mes ->
-- diario dia); CREATE OR REPLACE VIEW no Postgres nao troca o conjunto de
-- colunas, entao um drop explicito garante recriacao limpa em ambientes que
-- ainda tenham a versao antiga materializada.
{{ config(materialized='view', pre_hook="drop view if exists {{ this }} cascade") }}

with tx as (
    select categoria_id, 1 as sinal, valor_total, data_competencia, data_vencimento, is_cancelado
    from {{ ref('fct_conta_azul__contas_a_receber') }}
    union all
    select categoria_id, -1, valor_total, data_competencia, data_vencimento, is_cancelado
    from {{ ref('fct_conta_azul__contas_a_pagar') }}
),

norm as (
    select categoria_id, sinal, valor_total, 'competencia'::text as regime, data_competencia::date as dia
    from tx where not is_cancelado and data_competencia is not null
    union all
    select categoria_id, sinal, valor_total, 'vencimento', data_vencimento::date
    from tx where not is_cancelado and data_vencimento is not null
)

select
    regime,
    categoria_id,
    dia,
    sum(valor_total * sinal)::numeric(14,2) as valor
from norm
where categoria_id is not null
group by 1, 2, 3
order by regime, categoria_id, dia
