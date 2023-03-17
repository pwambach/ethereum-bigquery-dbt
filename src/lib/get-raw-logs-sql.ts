import { ATTRIBUTION } from "../config";

export function getRawLogs(
  moduleName: string,
  contracts: { name: string; address: string }[],
  minDate: Date
) {
  const addressList = contracts
    .map(({ address }) => `'${address.toLowerCase()}'`)
    .join(", ");

  return `-- ${ATTRIBUTION}

{{
  config(
    schema="${moduleName}",
    alias="logs",
    materialized="incremental",
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "block_timestamp",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by="address"
  )
}}

select
  * except(block_hash)
from
  {{ source('crypto_ethereum', 'logs') }}
where
    lower(address) IN (${addressList})

    and
    block_timestamp >= TIMESTAMP('${minDate.toISOString().slice(0, 10)}')

    {% if is_incremental() %}
      and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
    {% endif %}  
  `;
}
