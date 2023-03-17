-- generated with https://github.com/pwambach/ethereum-bigquery-dbt
-- Call Signature: function proposeArtistPaymentAddressesAndSplits(uint256 _projectId, address _artistAddress, address _additionalPayeePrimarySales, uint256 _additionalPayeePrimarySalesPercentage, address _additionalPayeeSecondarySales, uint256 _additionalPayeeSecondarySalesPercentage)

{{
  config(
    schema="Artblocks",
    alias="Core-v3_call_proposeArtistPaymentAddressesAndSplits",
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
        {{ target.schema }}.decode_call('{"type":"function","name":"proposeArtistPaymentAddressesAndSplits","constant":false,"payable":false,"inputs":[{"type":"uint256","name":"_projectId"},{"type":"address","name":"_artistAddress"},{"type":"address","name":"_additionalPayeePrimarySales"},{"type":"uint256","name":"_additionalPayeePrimarySalesPercentage"},{"type":"address","name":"_additionalPayeeSecondarySales"},{"type":"uint256","name":"_additionalPayeeSecondarySalesPercentage"}],"outputs":[]}', input) as call,
        * except(input)
    from
        {{ ref('Artblocks_calls') }}
    where
        lower(to_address) = '0x99a9b7c1116f9ceeb1652de04d5969cce509b069'
        and
        starts_with(input, '0x2b65e67d')

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(call),
JSON_VALUE(call, '$._projectId') as `_projectId`,
JSON_VALUE(call, '$._artistAddress') as `_artistAddress`,
JSON_VALUE(call, '$._additionalPayeePrimarySales') as `_additionalPayeePrimarySales`,
JSON_VALUE(call, '$._additionalPayeePrimarySalesPercentage') as `_additionalPayeePrimarySalesPercentage`,
JSON_VALUE(call, '$._additionalPayeeSecondarySales') as `_additionalPayeeSecondarySales`,
JSON_VALUE(call, '$._additionalPayeeSecondarySalesPercentage') as `_additionalPayeeSecondarySalesPercentage`
from decoded
