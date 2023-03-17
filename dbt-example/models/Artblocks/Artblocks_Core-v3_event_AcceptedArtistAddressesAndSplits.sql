-- generated with https://github.com/pwambach/ethereum-bigquery-dbt
-- Event Signature: event AcceptedArtistAddressesAndSplits(uint256 indexed _projectId)

{{
  config(
    schema="Artblocks",
    alias="Core-v3_event_AcceptedArtistAddressesAndSplits",
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
        {{ target.schema }}.decode_log('{"type":"event","anonymous":false,"name":"AcceptedArtistAddressesAndSplits","inputs":[{"type":"uint256","name":"_projectId","indexed":true}]}', data, topics) as log,
        * except(data, topics)
    from
        {{ ref('Artblocks_logs') }}
    where
        lower(address) = '0x99a9b7c1116f9ceeb1652de04d5969cce509b069'
        and
        topics[SAFE_OFFSET(0)] = '0xc582d05e1da854143bd3271ef4529d79cf5a69fc6057ae320f357acfd291b738'

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(log),
JSON_VALUE(log, '$._projectId') as `_projectId`
from decoded
