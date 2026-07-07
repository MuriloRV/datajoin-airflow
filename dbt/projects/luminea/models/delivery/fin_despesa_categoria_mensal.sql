-- Delivery: despesa por (ano, mes, grupo, categoria) — tela Detalhamento de
-- Despesas e comparativos por categoria. grupo = bucket DRE (entrada_dre,
-- "categoria pai" efetiva); categoria = categoria folha. Exclui cancelados.
{{ config(materialized='view') }}

with pagar as (
    select * from {{ ref('fct_conta_azul__contas_a_pagar') }}
),

categorias as (
    select * from {{ ref('dim_conta_azul__categorias_financeiras') }}
)

select
    extract(year  from f.data_competencia)::int      as ano,
    extract(month from f.data_competencia)::int      as mes_num,
    coalesce(nullif(c.entrada_dre, ''), 'SEM_GRUPO') as grupo_key,
    c.categoria_nome                                 as categoria,
    count(*)::int                                    as qtd,
    sum(f.valor_total)::numeric(14,2)                as total
from pagar f
join categorias c on c.categoria_id = f.categoria_id
where f.data_competencia is not null
  and not f.is_cancelado
group by 1, 2, 3, 4
order by 1, 2, total desc
