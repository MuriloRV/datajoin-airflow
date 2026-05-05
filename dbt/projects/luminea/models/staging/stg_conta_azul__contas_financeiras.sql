-- Staging: contas financeiras (bancos/caixas/cartoes).

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__contas_financeiras') }}
),

renamed as (
    select
        id::uuid                          as conta_financeira_id,
        trim(nome)                        as conta_nome,
        nullif(upper(trim(tipo)), '')     as tipo,
        nullif(upper(trim(banco)), '')    as banco,
        codigo_banco,
        nullif(trim(agencia), '')         as agencia,
        nullif(trim(numero), '')          as numero_conta,
        coalesce(ativo, true)             as ativo,
        coalesce(conta_padrao, false)     as conta_padrao,
        coalesce(possui_config_boleto_bancario, false) as possui_config_boleto,
        loaded_at                         as raw_loaded_at,
        current_timestamp                 as staged_at
    from source
)

select * from renamed
