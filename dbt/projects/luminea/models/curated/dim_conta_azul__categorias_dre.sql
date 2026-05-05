-- Curated: dimensao de categorias DRE com flags is_raiz/is_folha
-- pra navegar arvore hierarquica via parent_id.

{{ config(materialized='table') }}

-- API entrega arvore via `subitens` aninhados; top-level (rows desta dim)
-- sao sempre as raizes do DRE. is_raiz = true. is_folha = (num_subitens=0).

with stg as (
    select * from {{ ref('stg_conta_azul__categorias_dre') }}
)

select
    categoria_dre_id,
    categoria_nome,
    codigo,
    ordem,
    indica_totalizador,
    representa_soma_custo_medio,
    num_subitens,
    num_categorias_financeiras,
    -- Top-level rows = raizes da arvore DRE (parent_id sempre null no nivel 0).
    true                                                          as is_raiz,
    case when num_subitens = 0 then true else false end           as is_folha,
    case when num_categorias_financeiras > 0 then true else false end
                                                                  as has_categorias_financeiras,
    current_timestamp                                             as dim_refreshed_at
from stg
