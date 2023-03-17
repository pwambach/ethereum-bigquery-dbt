-- generated with https://github.com/pwambach/ethereum-bigquery-dbt
-- Event Signature: event ProposedArtistAddressesAndSplits(uint256 indexed _projectId, address _artistAddress, address _additionalPayeePrimarySales, uint256 _additionalPayeePrimarySalesPercentage, address _additionalPayeeSecondarySales, uint256 _additionalPayeeSecondarySalesPercentage)

{{
  config(
    schema="Artblocks",
    alias="Core-v3_event_ProposedArtistAddressesAndSplits",
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
        {{ target.schema }}.decode_log('{"type":"event","anonymous":false,"name":"ProposedArtistAddressesAndSplits","inputs":[{"type":"uint256","name":"_projectId","indexed":true},{"type":"address","name":"_artistAddress","indexed":false},{"type":"address","name":"_additionalPayeePrimarySales","indexed":false},{"type":"uint256","name":"_additionalPayeePrimarySalesPercentage","indexed":false},{"type":"address","name":"_additionalPayeeSecondarySales","indexed":false},{"type":"uint256","name":"_additionalPayeeSecondarySalesPercentage","indexed":false}]}', data, topics) as log,
        * except(data, topics)
    from
        {{ ref('Artblocks_logs') }}
    where
        lower(address) = '0x99a9b7c1116f9ceeb1652de04d5969cce509b069'
        and
        topics[SAFE_OFFSET(0)] = '0x6ff7d102bb3657a26dcbbcd299d821a066718a7cf76ae7cd98279f18b74da8ac'

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(log),
JSON_VALUE(log, '$._projectId') as `_projectId`,
JSON_VALUE(log, '$._artistAddress') as `_artistAddress`,
JSON_VALUE(log, '$._additionalPayeePrimarySales') as `_additionalPayeePrimarySales`,
JSON_VALUE(log, '$._additionalPayeePrimarySalesPercentage') as `_additionalPayeePrimarySalesPercentage`,
JSON_VALUE(log, '$._additionalPayeeSecondarySales') as `_additionalPayeeSecondarySales`,
JSON_VALUE(log, '$._additionalPayeeSecondarySalesPercentage') as `_additionalPayeeSecondarySalesPercentage`
from decoded
