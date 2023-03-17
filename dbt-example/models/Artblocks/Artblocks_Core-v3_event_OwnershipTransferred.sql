-- generated with https://github.com/pwambach/ethereum-bigquery-dbt
-- Event Signature: event OwnershipTransferred(address indexed previousOwner, address indexed newOwner)

{{
  config(
    schema="Artblocks",
    alias="Core-v3_event_OwnershipTransferred",
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
        {{ target.schema }}.decode_log('{"type":"event","anonymous":false,"name":"OwnershipTransferred","inputs":[{"type":"address","name":"previousOwner","indexed":true},{"type":"address","name":"newOwner","indexed":true}]}', data, topics) as log,
        * except(data, topics)
    from
        {{ ref('Artblocks_logs') }}
    where
        lower(address) = '0x99a9b7c1116f9ceeb1652de04d5969cce509b069'
        and
        topics[SAFE_OFFSET(0)] = '0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0'

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(log),
JSON_VALUE(log, '$.previousOwner') as `previousOwner`,
JSON_VALUE(log, '$.newOwner') as `newOwner`
from decoded
