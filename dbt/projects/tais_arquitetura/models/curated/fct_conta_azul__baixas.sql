-- Curated: fato de baixas (pagamentos efetivos). 1 row = 1 pagamento.
-- 1 parcela pode ter N baixas (parciais).
-- FK: parcela_id -> fct_parcelas, conta_financeira_id -> dim_contas_financeiras.
--
-- Verdade absoluta sobre o caixa — usar este fato pra DRE de caixa,
-- prazo medio de recebimento real, recuperacao de juros/multa.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__baixas') }}
)

select
    baixa_id,
    parcela_id,
    data_pagamento,
    valor_pago,
    valor_juros,
    valor_multa,
    valor_desconto,
    -- Valor liquido recebido (base pra DRE de caixa).
    valor_pago + coalesce(valor_juros, 0) + coalesce(valor_multa, 0)
        - coalesce(valor_desconto, 0)                                          as valor_liquido_caixa,
    metodo_pagamento,
    forma_pagamento,
    conta_financeira_id,
    conta_financeira_nome,
    observacao,
    source_created_at,
    source_updated_at,
    case when valor_juros    > 0 then true else false end                      as tem_juros,
    case when valor_multa    > 0 then true else false end                      as tem_multa,
    case when valor_desconto > 0 then true else false end                      as tem_desconto,
    case when conta_financeira_id is not null then true else false end         as has_conta_financeira,
    current_timestamp                                                          as fct_refreshed_at
from stg
