-- Staging: itens (linhas) de NFe. 1 row = 1 produto na nota com tributos
-- por linha (ICMS, IPI, PIS, COFINS).

{{ config(
    materialized='incremental',
    unique_key='nota_item_id',
    on_schema_change='append_new_columns'
) }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__notas_fiscais_itens') }}

    {% if is_incremental() %}
      where loaded_at > (
          select coalesce(max(staged_at), '1900-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

renamed as (
    -- id pode ser UUID (quando API expoe) ou hash deterministico
    -- (chave_acesso + numero_item) — mantem como TEXT pra preservar ambos.
    select
        id                             as nota_item_id,
        nota_fiscal_id::uuid           as nota_fiscal_id,
        nullif(trim(chave_acesso), '') as chave_acesso,
        numero_item,
        produto_id::uuid               as produto_id,
        nullif(trim(codigo), '')       as codigo,
        nullif(trim(descricao), '')    as descricao,
        nullif(trim(ncm), '')          as ncm,
        nullif(trim(cfop), '')         as cfop,
        quantidade,
        valor_unitario,
        valor_total,
        valor_icms,
        valor_ipi,
        valor_pis,
        valor_cofins,
        loaded_at                      as raw_loaded_at,
        current_timestamp              as staged_at
    from source
)

select * from renamed
