-- Curated: dimensao de produtos com margem calculada.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__produtos') }}
)

select
    produto_id,
    produto_nome,
    codigo,
    descricao,
    preco,
    custo,
    -- Margem absoluta + percentual. NULL se preco for 0/null pra evitar div/0.
    case when preco > 0 then preco - coalesce(custo, 0) end           as margem_absoluta,
    case when preco > 0 then ((preco - coalesce(custo, 0)) / preco)
         else null end                                                as margem_percentual,
    estoque,
    estoque_minimo,
    case when estoque is not null and estoque_minimo is not null
              and estoque < estoque_minimo
         then true else false end                                     as abaixo_estoque_minimo,
    unidade,
    status,
    source_created_at,
    source_updated_at,
    current_timestamp as dim_refreshed_at
from stg
