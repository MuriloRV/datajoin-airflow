-- Curated: dimensao de servicos com margem.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__servicos') }}
)

select
    servico_id,
    servico_id_legado,
    servico_nome,
    codigo,
    descricao,
    preco,
    custo,
    case when preco > 0 then preco - coalesce(custo, 0) end           as margem_absoluta,
    case when preco > 0 then ((preco - coalesce(custo, 0)) / preco)
         else null end                                                as margem_percentual,
    status,
    tipo_servico,
    current_timestamp as dim_refreshed_at
from stg
