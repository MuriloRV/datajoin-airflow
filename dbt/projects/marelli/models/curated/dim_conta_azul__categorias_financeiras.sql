-- Curated: dimensao de categorias financeiras com flag de raiz/folha.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__categorias_financeiras') }}
),

pais as (
    select distinct categoria_pai_id from stg where categoria_pai_id is not null
),

with_flags as (
    select
        s.categoria_id,
        s.categoria_nome,
        s.tipo,
        s.categoria_pai_id,
        s.entrada_dre,
        s.considera_custo_dre,
        s.versao,
        case when s.categoria_pai_id is null then true else false end as is_raiz,
        case when p.categoria_pai_id is null then true else false end as is_folha,
        current_timestamp as dim_refreshed_at
    from stg s
    left join pais p on p.categoria_pai_id = s.categoria_id
)

select * from with_flags
