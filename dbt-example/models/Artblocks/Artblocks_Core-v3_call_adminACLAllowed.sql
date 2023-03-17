-- generated with https://github.com/pwambach/ethereum-bigquery-dbt
-- Call Signature: function adminACLAllowed(address _sender, address _contract, bytes4 _selector) returns (bool)

{{
  config(
    schema="Artblocks",
    alias="Core-v3_call_adminACLAllowed",
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
        {{ target.schema }}.decode_call('{"type":"function","name":"adminACLAllowed","constant":false,"payable":false,"inputs":[{"type":"address","name":"_sender"},{"type":"address","name":"_contract"},{"type":"bytes4","name":"_selector"}],"outputs":[{"type":"bool"}]}', input) as call,
        * except(input)
    from
        {{ ref('Artblocks_calls') }}
    where
        lower(to_address) = '0x99a9b7c1116f9ceeb1652de04d5969cce509b069'
        and
        starts_with(input, '0x230448b1')

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(call),
JSON_VALUE(call, '$._sender') as `_sender`,
JSON_VALUE(call, '$._contract') as `_contract`,
JSON_VALUE(call, '$._selector') as `_selector`
from decoded
