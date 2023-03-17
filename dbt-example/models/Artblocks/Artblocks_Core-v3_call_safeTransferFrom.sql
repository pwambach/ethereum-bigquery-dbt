-- generated with https://github.com/pwambach/ethereum-bigquery-dbt
-- Call Signature: function safeTransferFrom(address from, address to, uint256 tokenId, bytes data)

{{
  config(
    schema="Artblocks",
    alias="Core-v3_call_safeTransferFrom",
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
        {{ target.schema }}.decode_call('{"type":"function","name":"safeTransferFrom","constant":false,"payable":false,"inputs":[{"type":"address","name":"from"},{"type":"address","name":"to"},{"type":"uint256","name":"tokenId"},{"type":"bytes","name":"data"}],"outputs":[]}', input) as call,
        * except(input)
    from
        {{ ref('Artblocks_calls') }}
    where
        lower(to_address) = '0x99a9b7c1116f9ceeb1652de04d5969cce509b069'
        and
        starts_with(input, '0xb88d4fde')

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(call),
JSON_VALUE(call, '$.from') as `from`,
JSON_VALUE(call, '$.to') as `to`,
JSON_VALUE(call, '$.tokenId') as `tokenId`,
JSON_VALUE(call, '$.data') as `data`
from decoded
