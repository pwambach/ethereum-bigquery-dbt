-- generated with https://github.com/pwambach/ethereum-bigquery-dbt
-- Event Signature: event MinterUpdated(address indexed _currentMinter)

{{
  config(
    schema="Artblocks",
    alias="Core-v3_event_MinterUpdated",
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
        {{ target.schema }}.decode_log('{"type":"event","anonymous":false,"name":"MinterUpdated","inputs":[{"type":"address","name":"_currentMinter","indexed":true}]}', data, topics) as log,
        * except(data, topics)
    from
        {{ ref('Artblocks_logs') }}
    where
        lower(address) = '0x99a9b7c1116f9ceeb1652de04d5969cce509b069'
        and
        topics[SAFE_OFFSET(0)] = '0xad0f299ec81a386c98df0ac27dae11dd020ed1b56963c53a7292e7a3a314539a'

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(log),
JSON_VALUE(log, '$._currentMinter') as `_currentMinter`
from decoded
