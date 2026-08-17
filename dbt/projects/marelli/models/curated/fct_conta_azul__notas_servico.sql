-- Curated: tabela fato de NFS-e com flags por status normalizado.
-- Granularidade: 1 row por emissao fiscal de servico.
-- A API retorna 10 status possiveis; reduzimos a flags binarias pra
-- consumo analitico simples (is_emitida, is_cancelada, is_falha, is_em_curso).
-- FK: cliente_id -> dim_pessoas.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__notas_servico') }}
)

select
    nota_id,
    numero,
    status,
    valor,
    data_emissao,
    data_competencia,
    cliente_id,
    -- Flags por bucket de status. PENDENTE/PRONTA_ENVIO/AGUARDANDO_RETORNO/
    -- EM_ESPERA/EMITINDO sao "em curso"; EMITIDA/CORRIGIDA_SUCESSO sao OK;
    -- FALHA/FALHA_CANCELAMENTO sao erro; CANCELADA e' final negativo.
    case when upper(status) in ('EMITIDA', 'CORRIGIDA_SUCESSO') then true else false end as is_emitida,
    case when upper(status) = 'CANCELADA' then true else false end                       as is_cancelada,
    case when upper(status) in ('FALHA', 'FALHA_CANCELAMENTO') then true else false end  as is_falha,
    case when upper(status) in (
        'PENDENTE', 'PRONTA_ENVIO', 'AGUARDANDO_RETORNO',
        'EM_ESPERA', 'EMITINDO'
    ) then true else false end                                                            as is_em_curso,
    current_timestamp                                                                     as fct_refreshed_at
from stg
