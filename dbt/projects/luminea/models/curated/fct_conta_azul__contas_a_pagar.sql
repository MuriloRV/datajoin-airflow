-- Curated: tabela fato de contas a pagar.
-- Granularidade: 1 row por evento (titulo a pagar). Espelha contas_a_receber
-- com fornecedor_id em vez de cliente_id.
-- FK: fornecedor_id -> dim_pessoas.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__contas_a_pagar') }}
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
    fornecedor_id,
    fornecedor_nome,
    num_categorias,
    num_centros_de_custo,
    foi_renegociado,
    case when upper(status) = 'ACQUITTED' then true else false end                       as is_pago,
    case when upper(status) = 'PENDING'   then true else false end                       as is_pendente,
    case when upper(status) = 'CANCELED'  then true else false end                       as is_cancelado,
    case
        when upper(coalesce(status, '')) in ('ACQUITTED', 'CANCELED') then false
        when data_vencimento is null                                  then false
        when data_vencimento < current_timestamp                      then true
        else false
    end                                                                                   as is_atrasado,
    case when fornecedor_id is not null then true else false end                         as has_fornecedor,
    current_timestamp                                                                    as fct_refreshed_at
from stg
