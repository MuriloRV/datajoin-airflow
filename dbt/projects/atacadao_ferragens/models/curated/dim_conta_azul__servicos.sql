-- Curated: dimensao de servicos com margem.
--
-- Enriquecido via LEFT JOIN com stg_servicos_detalhe pra trazer
-- aliquota_iss, codigo_servico_municipal, tributacao detalhada e
-- observacoes fiscais.

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__servicos') }}
),

detalhe as (
    select * from {{ ref('stg_conta_azul__servicos_detalhe') }}
),

joined as (
    select
        s.servico_id,
        s.servico_id_legado,
        s.servico_nome,
        s.codigo,
        s.descricao,
        s.preco,
        s.custo,
        s.status,
        s.tipo_servico,
        d.aliquota_iss,
        d.codigo_servico_municipal,
        d.tributacao,
        d.observacao
    from stg s
    left join detalhe d using (servico_id)
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
    -- Campos de detalhe (NULL quando servicos_detalhe nao foi puxado).
    aliquota_iss,
    codigo_servico_municipal,
    tributacao,
    observacao,
    case when aliquota_iss             is not null then true else false end as has_aliquota_iss,
    case when codigo_servico_municipal is not null then true else false end as has_codigo_municipal,
    case when tributacao               is not null then true else false end as has_tributacao,
    current_timestamp                                                  as dim_refreshed_at
from joined
