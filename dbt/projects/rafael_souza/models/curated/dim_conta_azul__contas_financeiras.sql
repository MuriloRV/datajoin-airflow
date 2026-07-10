-- Curated: dimensao de contas financeiras com categorizacao por natureza.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__contas_financeiras') }}
)

select
    conta_financeira_id,
    conta_nome,
    tipo,
    -- Agrupamento de alto nivel pra dashboards (banco vs caixa vs cartao).
    case
        when tipo in ('CONTA_CORRENTE', 'POUPANCA', 'INVESTIMENTO') then 'banco'
        when tipo = 'CAIXINHA'                                       then 'caixa'
        when tipo in ('CARTAO_CREDITO', 'CARTAO_DEBITO')             then 'cartao'
        else 'outro'
    end                                              as natureza,
    banco,
    codigo_banco,
    agencia,
    numero_conta,
    ativo,
    conta_padrao,
    possui_config_boleto,
    current_timestamp                                as dim_refreshed_at
from stg
