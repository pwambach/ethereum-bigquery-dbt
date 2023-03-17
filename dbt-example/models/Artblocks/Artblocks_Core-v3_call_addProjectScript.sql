-- generated with https://github.com/pwambach/ethereum-bigquery-dbt
-- Call Signature: function addProjectScript(uint256 _projectId, string _script)

{{
  config(
    schema="Artblocks",
    alias="Core-v3_call_addProjectScript",
    materialized="incremental",
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "block_timestamp",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

with decoded as (
    select
        {{ target.schema }}.decode_call('{"type":"function","name":"addProjectScript","constant":false,"payable":false,"inputs":[{"type":"uint256","name":"_projectId"},{"type":"string","name":"_script"}],"outputs":[]}', input) as call,
        * except(input)
    from
        {{ ref('Artblocks_calls') }}
    where
        lower(to_address) = '0x99a9b7c1116f9ceeb1652de04d5969cce509b069'
        and
        starts_with(input, '0xacad0124')

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(call),
JSON_VALUE(call, '$._projectId') as `_projectId`,
JSON_VALUE(call, '$._script') as `_script`
from decoded
