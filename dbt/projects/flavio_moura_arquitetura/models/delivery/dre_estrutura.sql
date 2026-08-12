-- Delivery: arvore do DRE (4 niveis) para a tela DRE.
--   N1 Grupo         <- categorias_dre.itens        (ex: Despesas Operacionais)
--   N2 Subgrupo      <- categorias_dre.subitens     (ex: Despesas Administrativas)
--   N3 Categoria pai <- categorias_financeiras.categoria_pai_id (ex: 4.03 Salarios e Encargos)
--   N4 Folha         <- categorias_financeiras      (categoria do lancamento, ex: Salarios)
-- Totalizadores (indica_totalizador) entram como linhas N1 intercaladas, na
-- ordem (posicao, indica_totalizador). `seq` = ordem de exibicao final.
-- Le a arvore aninhada do raw (subitens jsonb) via source; resolve o pai via
-- dim_conta_azul__categorias_financeiras. Folhas sem pai caem em "(Sem categoria pai)".
{{ config(materialized='view') }}

with n1 as (
    select descricao, codigo, indica_totalizador, subitens,
        row_number() over (order by posicao, indica_totalizador, descricao) as ord_n1
    from {{ source('conta_azul', 'conta_azul__categorias_dre') }}
),

categorias as (
    select * from {{ ref('dim_conta_azul__categorias_financeiras') }}
),

folhas as (
    select n1.ord_n1, sub.idx::int as ord_n2, sub.s->>'descricao' as subgrupo,
           cat.idx::int as leaf_idx, (cat.cf->>'id')::uuid as leaf_id, cat.cf->>'nome' as leaf_nome
    from n1,
         jsonb_array_elements(coalesce(n1.subitens, '[]')) with ordinality as sub(s, idx),
         jsonb_array_elements(coalesce(sub.s->'categorias_financeiras', '[]')) with ordinality as cat(cf, idx)
    where not n1.indica_totalizador
),

folhas_pai as (
    select f.ord_n1, f.ord_n2, f.leaf_idx, f.leaf_id, f.leaf_nome,
           coalesce(fin.categoria_pai_id, '00000000-0000-0000-0000-000000000000'::uuid) as pai_key,
           coalesce(pai.categoria_nome, '(Sem categoria pai)') as pai_nome
    from folhas f
    left join categorias fin on fin.categoria_id = f.leaf_id
    left join categorias pai on pai.categoria_id = fin.categoria_pai_id
),

pai_ord as (
    select ord_n1, ord_n2, pai_key, pai_nome,
           dense_rank() over (partition by ord_n1, ord_n2 order by pai_nome) as ord_n3
    from (select distinct ord_n1, ord_n2, pai_key, pai_nome from folhas_pai) d
),

nodes as (
    -- N1 grupo
    select ord_n1, 0 as ord_n2, 0 as ord_n3, 0 as ord_n4, 1 as nivel,
           'grupo'::text as tipo, descricao, codigo, null::uuid as categoria_id
    from n1 where not indica_totalizador
    union all
    -- N1 totalizador
    select ord_n1, 0, 0, 0, 1, 'totalizador', descricao, codigo, null::uuid
    from n1 where indica_totalizador
    union all
    -- N2 subgrupo
    select n1.ord_n1, sub.idx::int, 0, 0, 2, 'subgrupo', sub.s->>'descricao', sub.s->>'codigo', null::uuid
    from n1, jsonb_array_elements(coalesce(n1.subitens, '[]')) with ordinality as sub(s, idx)
    where not n1.indica_totalizador
    union all
    -- N3 categoria pai
    select po.ord_n1, po.ord_n2, po.ord_n3, 0, 3, 'categoria_pai', po.pai_nome, null, null::uuid
    from pai_ord po
    union all
    -- N4 folha (categoria do lancamento)
    select fp.ord_n1, fp.ord_n2, po.ord_n3, fp.leaf_idx, 4, 'categoria', fp.leaf_nome, null, fp.leaf_id
    from folhas_pai fp
    join pai_ord po
      on po.ord_n1 = fp.ord_n1 and po.ord_n2 = fp.ord_n2 and po.pai_key = fp.pai_key
)

select row_number() over (order by ord_n1, ord_n2, ord_n3, nivel, ord_n4) as seq,
       ord_n1, ord_n2, ord_n3, ord_n4, nivel, tipo, descricao, codigo, categoria_id
from nodes
order by seq
