-- generated with https://github.com/pwambach/ethereum-bigquery-dbt
-- Call Signature: function updateProjectSecondaryMarketRoyaltyPercentage(uint256 _projectId, uint256 _secondMarketRoyalty)

{{
  config(
    schema="Artblocks",
    alias="Core-v3_call_updateProjectSecondaryMarketRoyaltyPercentage",
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
        {{ target.schema }}.decode_call('{"type":"function","name":"updateProjectSecondaryMarketRoyaltyPercentage","constant":false,"payable":false,"inputs":[{"type":"uint256","name":"_projectId"},{"type":"uint256","name":"_secondMarketRoyalty"}],"outputs":[]}', input) as call,
        * except(input)
    from
        {{ ref('Artblocks_calls') }}
    where
        lower(to_address) = '0x99a9b7c1116f9ceeb1652de04d5969cce509b069'
        and
        starts_with(input, '0xc34a03b5')

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(call),
JSON_VALUE(call, '$._projectId') as `_projectId`,
JSON_VALUE(call, '$._secondMarketRoyalty') as `_secondMarketRoyalty`
from decoded
