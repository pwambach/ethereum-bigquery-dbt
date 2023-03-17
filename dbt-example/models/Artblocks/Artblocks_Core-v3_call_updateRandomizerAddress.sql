-- generated with https://github.com/pwambach/ethereum-bigquery-dbt
-- Call Signature: function updateRandomizerAddress(address _randomizerAddress)

{{
  config(
    schema="Artblocks",
    alias="Core-v3_call_updateRandomizerAddress",
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
        {{ target.schema }}.decode_call('{"type":"function","name":"updateRandomizerAddress","constant":false,"payable":false,"inputs":[{"type":"address","name":"_randomizerAddress"}],"outputs":[]}', input) as call,
        * except(input)
    from
        {{ ref('Artblocks_calls') }}
    where
        lower(to_address) = '0x99a9b7c1116f9ceeb1652de04d5969cce509b069'
        and
        starts_with(input, '0x6c907b7f')

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(call),
JSON_VALUE(call, '$._randomizerAddress') as `_randomizerAddress`
from decoded
