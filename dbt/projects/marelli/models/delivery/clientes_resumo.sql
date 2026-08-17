-- Delivery: agregado de clientes pra dashboards.
--
-- Nome neutro de fonte (cliente, nao "pessoa do conta_azul") — convencao
-- da camada delivery: business names, source-agnostic. Quando entrar
-- Vindi/Shopify, este model passa a unir multiplas fontes via UNION ALL.
--
-- Por enquanto: 1:1 com dim_conta_azul__pessoas, sem regras de negocio.
-- Quando a modelagem de "cliente unico" estiver pronta, este vira o
-- mart final que dashboards consomem.

{{ config(materialized='table') }}

with dim as (
    select * from {{ ref('dim_conta_azul__pessoas') }}
),

agg as (
    select
        coalesce(tipo_pessoa, 'desconhecido') as tipo_pessoa,
        count(*)                              as total_clientes,
        sum(case when has_email then 1 else 0 end)         as com_email,
        sum(case when has_phone then 1 else 0 end)         as com_telefone,
        sum(case when has_documento then 1 else 0 end)     as com_documento,
        sum(case when documento_is_valid then 1 else 0 end) as com_documento_valido,
        current_timestamp                                   as snapshot_at
    from dim
    group by coalesce(tipo_pessoa, 'desconhecido')
)

select * from agg
