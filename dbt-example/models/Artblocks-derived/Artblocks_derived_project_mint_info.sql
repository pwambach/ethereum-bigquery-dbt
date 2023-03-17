{{
  config(
    schema="Artblocks_derived",
    alias="project_mint_info",
    materialized="view"
  )
}}


with mintsPerProject as (
  select
    project_id as id,
    COUNT(*) as mint_count
  from
    {{ ref('Artblocks_derived_mints') }}
  group by project_id
)

select
  p.id as project_id,
  p.max_invocations as max_invocations,
  coalesce(m.mint_count, 0) as mint_count
from 
  {{ ref('Artblocks_derived_projects') }} p
    left join mintsPerProject m using (id)