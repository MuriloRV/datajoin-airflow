-- Staging: categorias financeiras DRE.
-- Dimensao pequena (dezenas a centenas de linhas) — full refresh
-- materializado como view evita custo de I/O sem ganho real.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__categorias_financeiras') }}
),

renamed as (
    select
        id::uuid                       as categoria_id,
        trim(nome)                     as categoria_nome,
        nullif(upper(trim(tipo)), '')  as tipo,            -- RECEITA | DESPESA
        categoria_pai::uuid            as categoria_pai_id,
        nullif(trim(entrada_dre), '')  as entrada_dre,
        coalesce(considera_custo_dre, false) as considera_custo_dre,
        versao,
        loaded_at                      as raw_loaded_at,
        current_timestamp              as staged_at
    from source
)

select * from renamed
