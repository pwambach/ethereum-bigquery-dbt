-- generated with https://github.com/pwambach/ethereum-bigquery-dbt
-- Event Signature: event Mint(address indexed _to, uint256 indexed _tokenId)

{{
  config(
    schema="Artblocks",
    alias="Core-v3_event_Mint",
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
        {{ target.schema }}.decode_log('{"type":"event","anonymous":false,"name":"Mint","inputs":[{"type":"address","name":"_to","indexed":true},{"type":"uint256","name":"_tokenId","indexed":true}]}', data, topics) as log,
        * except(data, topics)
    from
        {{ ref('Artblocks_logs') }}
    where
        lower(address) = '0x99a9b7c1116f9ceeb1652de04d5969cce509b069'
        and
        topics[SAFE_OFFSET(0)] = '0x0f6798a560793a54c3bcfe86a93cde1e73087d944c0ea20544137d4121396885'

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(log),
JSON_VALUE(log, '$._to') as `_to`,
JSON_VALUE(log, '$._tokenId') as `_tokenId`
from decoded
