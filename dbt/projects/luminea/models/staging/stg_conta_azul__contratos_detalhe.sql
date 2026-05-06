-- Staging: detalhe completo de contrato. Complementa o agregado da
-- listagem com composicao_de_valor, condicao_pagamento, configuracao
-- de recorrencia, termos.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__contratos_detalhe') }}
)

select
    id::uuid                       as contrato_id,
    nullif(trim(numero), '')       as contrato_numero,
    nullif(trim(descricao), '')    as descricao,
    (cliente->>'id')::uuid         as cliente_id,
    nullif(trim(cliente->>'nome'), '') as cliente_nome,
    (vendedor->>'id')::uuid        as vendedor_id,
    nullif(trim(vendedor->>'nome'), '') as vendedor_nome,
    composicao_de_valor,           -- jsonb cru
    condicao_pagamento,
    configuracao_recorrencia,
    termos,
    data_emissao,
    data_inicio,
    data_fim,
    nullif(trim(status), '')       as status,
    valor_bruto,
    valor_liquido,
    data_criacao                   as source_created_at,
    data_alteracao                 as source_updated_at,
    loaded_at                      as raw_loaded_at,
    current_timestamp              as staged_at
from source
