-- Staging: categorias DRE. Estrutura hierarquica via parent_id.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__categorias_dre') }}
)

select
    id::uuid                                  as categoria_dre_id,
    -- API expoe nome real em `descricao` (campo `nome` vem sempre null).
    nullif(trim(descricao), '')               as categoria_nome,
    nullif(trim(codigo), '')                  as codigo,
    posicao                                   as ordem,
    coalesce(indica_totalizador, false)       as indica_totalizador,
    coalesce(representa_soma_custo_medio, false) as representa_soma_custo_medio,
    -- Estrutura aninhada — expansao via dbt em modelo dedicado se preciso.
    subitens                                  as subitens_raw,
    categorias_financeiras                    as categorias_financeiras_raw,
    case
        when jsonb_typeof(subitens) = 'array' then jsonb_array_length(subitens)
        else 0
    end                                       as num_subitens,
    case
        when jsonb_typeof(categorias_financeiras) = 'array'
            then jsonb_array_length(categorias_financeiras)
        else 0
    end                                       as num_categorias_financeiras,
    raw,
    loaded_at                                 as raw_loaded_at,
    current_timestamp                         as staged_at
from source
