-- Delivery: saldo inicial total das contas financeiras (1 linha). Ancora do
-- Saldo Acumulado na pagina Fluxo de Caixa. Hoje o Conta Azul so expoe 1
-- registro (sem conta/data vinculadas) — soma defensiva com coalesce.
{{ config(materialized='view') }}

-- Soma com SINAL (saldo_inicial_liquido): despesas entram negativas.
-- Valida contra o 'Saldo Inicial' do extrato do Conta Azul.
select coalesce(sum(saldo_inicial_liquido), 0)::numeric(14,2) as saldo_inicial
from {{ ref('fct_conta_azul__saldo_inicial') }}
