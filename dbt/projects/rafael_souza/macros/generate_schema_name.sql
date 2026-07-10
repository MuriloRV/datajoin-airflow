-- Override do gerador de schema do dbt.
--
-- Comportamento default do dbt: schema = `<target.schema>_<custom_schema>`.
-- Pra Luminea isso geraria `rafael_souza_staging_rafael_souza_staging` (duplicado),
-- porque `+schema:` ja vem com o nome final medalhao (`rafael_souza_staging`,
-- `rafael_souza_curated`, `rafael_souza_delivery`).
--
-- Esta macro forca o schema a ser usado LITERAL — sem prefixo. Quando
-- nao ha custom_schema_name, cai pro target.schema (fallback).
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
