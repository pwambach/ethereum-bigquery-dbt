{{
  config(
    schema="Artblocks_derived",
    alias="projects",
    materialized="table"
  )
}}
with initialProjects as (
  select
    row_number() over (order by block_number, transaction_index, trace_address asc) + 373 as id,
    _projectName as name,
    _artistAddress as artist_address,
    block_timestamp as created_at
  from
    {{ ref('Artblocks_Core-v3_call_addProject') }}
  where
    status = 1
),
updatedNames as (
  select * from (
    select
      row_number() over (partition by _projectId order by block_number, transaction_index, trace_address desc) as row,
      CAST(_projectId as INT64) as id,
      _projectName as new_name
    from
      {{ ref('Artblocks_Core-v3_call_updateProjectName') }}
    where status = 1
  ) 
  where
    row = 1
),
updatedScriptTypes as (
  select * from (
    select
      row_number() over (partition by _projectId order by block_number, transaction_index, trace_address desc) as row,
      CAST(_projectId as INT64) as id,
      {{ target.schema }}.parse_bytes32_string(_scriptTypeAndVersion) as script_type_and_version
    from
      {{ ref('Artblocks_Core-v3_call_updateProjectScriptType') }}
    where status = 1
  ) 
  where
    row = 1
),
updatedDescriptions as (
  select * from (
    select
      row_number() over (partition by _projectId order by block_number, transaction_index, trace_address desc) as row,
      CAST(_projectId as INT64) as id,
      _projectDescription as description
    from
      {{ ref('Artblocks_Core-v3_call_updateProjectDescription') }}
    where status = 1
  ) 
  where
    row = 1
),
updatedMaxInvocations as (
  select * from (
    select
      row_number() over (partition by _projectId order by block_number, transaction_index, trace_address desc) as row,
      CAST(_projectId as INT64) as id,
      _maxInvocations as max_invocations
    from
      {{ ref('Artblocks_Core-v3_call_updateProjectMaxInvocations') }}
    where status = 1
  ) 
  where
    row = 1
)

select 
  i.id as id,
  i.artist_address as artist_address,
  i.created_at as created_at,
  coalesce(up_names.new_name, i.name) as name,
  up_script_types.script_type_and_version as script_type_and_version,
  up_description.description as description,
  up_max_invocations.max_invocations as max_invocations
from
  initialProjects i
    left join updatedNames up_names using (id)
    left join updatedScriptTypes up_script_types using (id)
    left join updatedDescriptions up_description using (id)
    left join updatedMaxInvocations up_max_invocations using (id)
