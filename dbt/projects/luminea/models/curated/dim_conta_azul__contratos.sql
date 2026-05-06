-- Curated: dimensao de contratos com flags is_ativo/is_vencido.
-- Schema tentativo — Luminea retorna 0; ajustar campos quando aparecer
-- dado real (raw jsonb preservado em stg pra investigacao).
--
-- Enriquecido via LEFT JOIN com stg_contratos_detalhe pra trazer
-- composicao_de_valor, configuracao_recorrencia, condicao_pagamento,
-- termos, vendedor — campos ausentes no agregado da listagem.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__contratos') }}
),

detalhe as (
    select * from {{ ref('stg_conta_azul__contratos_detalhe') }}
),

joined as (
    select
        s.contrato_id,
        s.contrato_numero,
        s.descricao,
        s.cliente_id,
        s.valor,
        s.data_inicio,
        s.data_fim,
        s.status,
        s.source_created_at,
        s.source_updated_at,
        d.vendedor_id,
        d.vendedor_nome,
        d.composicao_de_valor,
        d.condicao_pagamento,
        d.configuracao_recorrencia,
        d.termos,
        d.data_emissao,
        d.valor_bruto,
        d.valor_liquido
    from stg s
    left join detalhe d using (contrato_id)
)

select
    contrato_id,
    contrato_numero,
    descricao,
    cliente_id,
    vendedor_id,
    vendedor_nome,
    valor,
    valor_bruto,
    valor_liquido,
    data_emissao,
    data_inicio,
    data_fim,
    status,
    composicao_de_valor,
    condicao_pagamento,
    configuracao_recorrencia,
    termos,
    source_created_at,
    source_updated_at,
    -- Flags derivados.
    case
        when data_fim is null then null
        when data_fim < current_timestamp then true
        else false
    end                                                         as is_vencido,
    case when upper(status) = 'ATIVO' then true else false end  as is_ativo,
    case when cliente_id        is not null then true else false end as has_cliente,
    case when vendedor_id       is not null then true else false end as has_vendedor,
    case when valor_liquido     is not null then true else false end as has_detalhe,
    case when configuracao_recorrencia is not null then true else false end as is_recorrente,
    current_timestamp                                                          as dim_refreshed_at
from joined
