-- Delivery: receita por (ano, mes) — telas Faturamento e Vendas.
--   faturamento = toda conta a receber
--   vendas      = recorte da categoria "Receitas de Servicos"
--                 (categoria_id 68a59492-1456-432d-b19c-7aa27e86cd01)
-- Exclui cancelados. Materializado como view (dado pequeno, sempre fresco;
-- dashboards leem com cache curto).
{{ config(materialized='view') }}

with receber as (
    select * from {{ ref('fct_conta_azul__contas_a_receber') }}
)

select
    extract(year  from data_competencia)::int  as ano,
    extract(month from data_competencia)::int  as mes_num,
    count(*)::int                               as qtd,
    sum(valor_total)::numeric(14,2)             as faturamento,
    sum(valor_pago)::numeric(14,2)              as recebido,
    sum(valor_nao_pago)::numeric(14,2)          as em_aberto,
    coalesce(sum(valor_total) filter (
        where categoria_id = '68a59492-1456-432d-b19c-7aa27e86cd01'
    ), 0)::numeric(14,2)                        as vendas
from receber
where data_competencia is not null
  and not is_cancelado
group by 1, 2
order by 1, 2
