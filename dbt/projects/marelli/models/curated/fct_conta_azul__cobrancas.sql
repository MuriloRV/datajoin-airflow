-- Curated: fato de cobrancas (boletos/PIX) emitidas pra parcelas.
-- Granularidade: 1 row por cobranca. Funil:
--   AGUARDANDO_CONFIRMACAO -> REGISTRADO -> QUITADO
--                                       -> CANCELADO / EXPIRADO / INVALIDO
-- FK: parcela_id -> fct_parcelas, evento_financeiro_id -> fct_contas_a_receber.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__cobrancas') }}
)

select
    cobranca_id,
    parcela_id,
    evento_financeiro_id,
    status,
    url,
    valor_cobranca,
    tipo,
    data_vencimento,
    data_emissao,
    source_updated_at,
    -- Flags por status. QUITADO = paga, EXPIRADO/CANCELADO/INVALIDO = morta.
    case when upper(coalesce(status, '')) = 'QUITADO'                then true else false end as is_quitada,
    case when upper(coalesce(status, '')) = 'REGISTRADO'             then true else false end as is_registrada,
    case when upper(coalesce(status, '')) = 'AGUARDANDO_CONFIRMACAO' then true else false end as is_aguardando,
    case when upper(coalesce(status, '')) = 'CANCELADO'              then true else false end as is_cancelada,
    case when upper(coalesce(status, '')) = 'EXPIRADO'               then true else false end as is_expirada,
    case when upper(coalesce(status, '')) = 'INVALIDO'               then true else false end as is_invalida,
    case when upper(coalesce(tipo, ''))   = 'BOLETO'                 then true else false end as is_boleto,
    case when upper(coalesce(tipo, ''))   = 'PIX'                    then true else false end as is_pix,
    current_timestamp                                                                          as fct_refreshed_at
from stg
