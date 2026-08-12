-- Curated: tabela fato de transferencias entre contas.
-- Granularidade: 1 row = 1 transferencia (movimento interno).
-- FKs: conta_origem_id e conta_destino_id -> dim_contas_financeiras.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__transferencias') }}
)

select
    transferencia_id,
    data_transferencia,
    valor,
    descricao,
    conta_origem_id,
    conta_origem_nome,
    conta_destino_id,
    conta_destino_nome,
    source_created_at,
    source_updated_at,
    case when conta_origem_id  is not null then true else false end as has_origem,
    case when conta_destino_id is not null then true else false end as has_destino,
    case
        when conta_origem_id is not null
         and conta_destino_id is not null
         and conta_origem_id = conta_destino_id then true
        else false
    end as is_self_transfer,
    current_timestamp as fct_refreshed_at
from stg
