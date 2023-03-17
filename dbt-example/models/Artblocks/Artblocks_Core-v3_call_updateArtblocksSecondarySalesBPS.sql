-- generated with https://github.com/pwambach/ethereum-bigquery-dbt
-- Call Signature: function updateArtblocksSecondarySalesBPS(uint256 _artblocksSecondarySalesBPS)

{{
  config(
    schema="Artblocks",
    alias="Core-v3_call_updateArtblocksSecondarySalesBPS",
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
        {{ target.schema }}.decode_call('{"type":"function","name":"updateArtblocksSecondarySalesBPS","constant":false,"payable":false,"inputs":[{"type":"uint256","name":"_artblocksSecondarySalesBPS"}],"outputs":[]}', input) as call,
        * except(input)
    from
        {{ ref('Artblocks_calls') }}
    where
        lower(to_address) = '0x99a9b7c1116f9ceeb1652de04d5969cce509b069'
        and
        starts_with(input, '0x9ab31a2d')

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(call),
JSON_VALUE(call, '$._artblocksSecondarySalesBPS') as `_artblocksSecondarySalesBPS`
from decoded
