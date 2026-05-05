-- Curated: tabela fato de contas a receber.
-- Granularidade: 1 row por evento (titulo a receber). Status mutavel
-- (PENDING, ACQUITTED, ...) — table full refresh garante reconciliacao.
-- FK: cliente_id -> dim_pessoas.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__contas_a_receber') }}
)

select
    evento_id,
    status,
    status_traduzido,
    total                                                                  as valor_total,
    valor_pago,
    valor_nao_pago,
    descricao,
    data_vencimento,
    data_competencia,
    source_created_at,
    source_updated_at,
    cliente_id,
    cliente_nome,
    num_categorias,
    num_centros_de_custo,
    foi_renegociado,
    -- Flags por status. ACQUITTED = pago, PENDING = aberto, OVERDUE/CANCELED.
    case when upper(status) = 'ACQUITTED' then true else false end                       as is_pago,
    case when upper(status) = 'PENDING'   then true else false end                       as is_pendente,
    case when upper(status) = 'CANCELED'  then true else false end                       as is_cancelado,
    -- Atraso real: vencido + ainda nao quitado.
    case
        when upper(coalesce(status, '')) in ('ACQUITTED', 'CANCELED') then false
        when data_vencimento is null                                  then false
        when data_vencimento < current_timestamp                      then true
        else false
    end                                                                                   as is_atrasado,
    case when cliente_id is not null then true else false end                            as has_cliente,
    current_timestamp                                                                    as fct_refreshed_at
from stg
