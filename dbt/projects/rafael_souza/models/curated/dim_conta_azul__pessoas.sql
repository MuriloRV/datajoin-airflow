-- Curated: dimensao de pessoas pronta pra consumo analitico.
-- Materializada como table (full refresh) — dimensoes sao normalmente
-- pequenas e a sobrecarga vs incremental nao se justifica.
--
-- Naming: ainda mantem nome da entidade (pessoa). Marts unificados
-- combinando varias fontes (Conta Azul + Vindi + Shopify) ficam num
-- model em delivery sem prefixo (ex: dim_clientes).

{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_conta_azul__pessoas') }}
),

with_flags as (
    select
        pessoa_id,
        pessoa_uuid_legado,
        pessoa_id_legado,
        pessoa_nome,
        email,
        documento,
        tipo_pessoa,
        telefone,
        ativo,
        -- Perfis vem como jsonb array (ex: ["Cliente"], ["Fornecedor"], ou ambos).
        -- Flags pra cada perfil — facilita filtros downstream.
        (perfis_raw @> '"Cliente"'::jsonb)    as is_cliente,
        (perfis_raw @> '"Fornecedor"'::jsonb) as is_fornecedor,
        source_created_at,
        source_updated_at,
        -- Flags de completude.
        case when email is not null     then true else false end as has_email,
        case when telefone is not null  then true else false end as has_phone,
        case when documento is not null then true else false end as has_documento,
        case when pessoa_uuid_legado is not null then true else false end as has_legado,
        -- Validacao de DV usando shared (mig 0029).
        case when tipo_pessoa = 'pj' then shared.is_valid_cnpj(documento)
             when tipo_pessoa = 'pf' then shared.is_valid_cpf(documento)
             else null
        end                                                       as documento_is_valid,
        current_timestamp                                          as dim_refreshed_at
    from stg
)

select * from with_flags
