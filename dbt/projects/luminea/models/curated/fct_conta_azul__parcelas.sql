-- Curated: fato de parcelas de eventos financeiros (a receber + a pagar).
-- Granularidade: 1 row por parcela. Status mutavel — table full refresh.
-- FK: evento_financeiro_id -> fct_contas_a_receber/pagar.
--
-- Aqui mora a primeira data_pagamento "esperada" da parcela. A verdade
-- absoluta sobre baixas (parciais, multa, juros aplicados) vive em
-- fct_baixas (1:N a partir desta).

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__parcelas_detalhe') }}
)

select
    parcela_id,
    evento_financeiro_id,
    numero_parcela,
    valor_parcela,
    valor_bruto,
    valor_liquido,
    valor_desconto,
    valor_juros,
    valor_multa,
    data_vencimento,
    data_pagamento,
    status,
    forma_pagamento,
    conta_financeira_id,
    conta_financeira_nome,
    source_created_at,
    source_updated_at,
    -- Flags por status. PAGO = quitada, ABERTO = ainda em aberto.
    case when upper(coalesce(status, '')) in ('PAGO', 'QUITADO', 'ACQUITTED')
         then true else false end                                              as is_paga,
    case when upper(coalesce(status, '')) in ('ABERTO', 'PENDING', 'PENDENTE')
         then true else false end                                              as is_aberta,
    case when upper(coalesce(status, '')) in ('CANCELADO', 'CANCELED')
         then true else false end                                              as is_cancelada,
    -- Atraso: vencida + ainda nao paga.
    case
        when upper(coalesce(status, '')) in ('PAGO', 'QUITADO', 'ACQUITTED', 'CANCELADO', 'CANCELED') then false
        when data_vencimento is null                                           then false
        when data_vencimento < current_timestamp                               then true
        else false
    end                                                                        as is_atrasada,
    case when data_pagamento is not null      then true else false end         as has_pagamento,
    case when conta_financeira_id is not null then true else false end         as has_conta_financeira,
    -- Dias entre vencimento e pagamento (negativo = pagou adiantado).
    case
        when data_pagamento is null or data_vencimento is null then null
        else extract(day from (data_pagamento - data_vencimento))::int
    end                                                                        as dias_atraso_pagamento,
    current_timestamp                                                          as fct_refreshed_at
from stg
