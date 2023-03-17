-- generated with https://github.com/pwambach/ethereum-bigquery-dbt
-- Event Signature: event ApprovalForAll(address indexed owner, address indexed operator, bool approved)

{{
  config(
    schema="Artblocks",
    alias="Core-v3_event_ApprovalForAll",
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
        {{ target.schema }}.decode_log('{"type":"event","anonymous":false,"name":"ApprovalForAll","inputs":[{"type":"address","name":"owner","indexed":true},{"type":"address","name":"operator","indexed":true},{"type":"bool","name":"approved","indexed":false}]}', data, topics) as log,
        * except(data, topics)
    from
        {{ ref('Artblocks_logs') }}
    where
        lower(address) = '0x99a9b7c1116f9ceeb1652de04d5969cce509b069'
        and
        topics[SAFE_OFFSET(0)] = '0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31'

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(log),
JSON_VALUE(log, '$.owner') as `owner`,
JSON_VALUE(log, '$.operator') as `operator`,
JSON_VALUE(log, '$.approved') as `approved`
from decoded
