-- generated with https://github.com/pwambach/ethereum-bigquery-dbt
-- Event Signature: event ProjectUpdated(uint256 indexed _projectId, bytes32 indexed _update)

{{
  config(
    schema="Artblocks",
    alias="Core-v3_event_ProjectUpdated",
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
        {{ target.schema }}.decode_log('{"type":"event","anonymous":false,"name":"ProjectUpdated","inputs":[{"type":"uint256","name":"_projectId","indexed":true},{"type":"bytes32","name":"_update","indexed":true}]}', data, topics) as log,
        * except(data, topics)
    from
        {{ ref('Artblocks_logs') }}
    where
        lower(address) = '0x99a9b7c1116f9ceeb1652de04d5969cce509b069'
        and
        topics[SAFE_OFFSET(0)] = '0xb96a30340e86d03ce4be42f94ac02d7b27b4a4cdae942beb69026718dfe66afc'

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(log),
JSON_VALUE(log, '$._projectId') as `_projectId`,
JSON_VALUE(log, '$._update') as `_update`
from decoded
