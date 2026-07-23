-- Curated: dimensao de produtos com margem calculada.
--
-- Enriquecido via LEFT JOIN com stg_produtos_detalhe pra trazer
-- tributacao (ICMS/IPI/PIS/COFINS), dimensoes (peso/altura/largura/
-- comprimento), fotos, NCM/CEST detalhados.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__produtos') }}
),

detalhe as (
    select * from {{ ref('stg_conta_azul__produtos_detalhe') }}
),

joined as (
    select
        s.produto_id,
        s.produto_nome,
        s.codigo,
        s.descricao,
        s.preco,
        s.custo,
        s.estoque,
        s.estoque_minimo,
        s.unidade,
        s.status,
        s.source_created_at,
        s.source_updated_at,
        -- detalhe
        d.ncm,
        d.cest,
        d.peso,
        d.altura,
        d.largura,
        d.comprimento,
        d.tributacao,
        d.fotos
    from stg s
    left join detalhe d using (produto_id)
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
    -- Campos de detalhe (NULL quando produtos_detalhe nao foi puxado).
    ncm,
    cest,
    peso,
    altura,
    largura,
    comprimento,
    tributacao,
    fotos,
    -- Volume = peso ou volume cubico estimado (m3) pra logistica.
    case when altura is not null and largura is not null and comprimento is not null
         then (altura * largura * comprimento) / 1000000.0
         else null end                                                as volume_m3,
    case when ncm        is not null then true else false end         as has_ncm,
    case when tributacao is not null then true else false end         as has_tributacao,
    case when fotos is not null and jsonb_array_length(fotos) > 0
         then true else false end                                     as has_fotos,
    source_created_at,
    source_updated_at,
    current_timestamp                                                  as dim_refreshed_at
from joined
