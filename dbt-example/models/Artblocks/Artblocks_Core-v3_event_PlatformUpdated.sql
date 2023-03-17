-- generated with https://github.com/pwambach/ethereum-bigquery-dbt
-- Event Signature: event PlatformUpdated(bytes32 indexed _field)

{{
  config(
    schema="Artblocks",
    alias="Core-v3_event_PlatformUpdated",
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
        {{ target.schema }}.decode_log('{"type":"event","anonymous":false,"name":"PlatformUpdated","inputs":[{"type":"bytes32","name":"_field","indexed":true}]}', data, topics) as log,
        * except(data, topics)
    from
        {{ ref('Artblocks_logs') }}
    where
        lower(address) = '0x99a9b7c1116f9ceeb1652de04d5969cce509b069'
        and
        topics[SAFE_OFFSET(0)] = '0x8b810f233ce7ee6e962ab4d98bf0277751de1f5589de3dcc812ac2047994d009'

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(log),
JSON_VALUE(log, '$._field') as `_field`
from decoded
