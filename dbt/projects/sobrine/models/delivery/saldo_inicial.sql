-- Delivery: saldo inicial total das contas financeiras (1 linha). Ancora do
-- Saldo Acumulado na pagina Fluxo de Caixa. Hoje o Conta Azul so expoe 1
-- registro (sem conta/data vinculadas) — soma defensiva com coalesce.
{{ config(materialized='view') }}

select coalesce(sum(saldo_inicial), 0)::numeric(14,2) as saldo_inicial
from {{ ref('fct_conta_azul__saldo_inicial') }}
