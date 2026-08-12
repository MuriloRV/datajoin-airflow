-- Staging: centros de custo. Dimensao estatica.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__centros_de_custo') }}
),

renamed as (
    select
        id::uuid                  as centro_de_custo_id,
        trim(nome)                as centro_de_custo_nome,
        nullif(trim(descricao), '') as descricao,
        coalesce(ativo, true)     as ativo,
        loaded_at                 as raw_loaded_at,
        current_timestamp         as staged_at
    from source
)

select * from renamed
