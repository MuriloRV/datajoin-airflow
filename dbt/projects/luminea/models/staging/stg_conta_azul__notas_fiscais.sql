-- Staging: NFe (nota fiscal de produto). Schema tentativo — hoje a API
-- so retorna EMITIDA e CORRIGIDA_SUCESSO. raw jsonb preserva tudo pra
-- ajuste downstream quando o shape do item for confirmado.

{{ config(materialized='view') }}

with source as (
    select * from {{ source('conta_azul', 'conta_azul__notas_fiscais') }}
)

select
    id::uuid                              as nota_id,
    nullif(trim(numero), '')              as numero,
    nullif(trim(status), '')              as status,
    valor,
    data_emissao,
    nullif(trim(chave_acesso), '')        as chave_acesso,
    nullif(trim(serie), '')               as serie,
    venda_id::uuid                        as venda_id,
    cliente_id::uuid                      as cliente_id,
    nullif(trim(documento_tomador), '')   as documento_tomador,
    raw,
    loaded_at                             as raw_loaded_at,
    current_timestamp                     as staged_at
from source
