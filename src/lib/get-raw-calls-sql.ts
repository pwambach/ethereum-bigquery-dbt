import { ATTRIBUTION } from "../config";

export function getRawCalls(
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
    alias="calls",
    materialized="incremental",
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "block_timestamp",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by="to_address"
  )
}}

select
  * except(value, output, reward_type, gas, gas_used, subtraces, error, block_hash, trace_id)
from
  {{ source('crypto_ethereum', 'traces') }}
where
    lower(to_address) IN (${addressList})

    and
    block_timestamp >= TIMESTAMP('${minDate.toISOString().slice(0, 10)}')

    {% if is_incremental() %}
      and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
    {% endif %}   
  `;
}
