import { ethers } from "ethers";
import { ATTRIBUTION } from "../config";

export function getEventSql(
  eventFragment: ethers.utils.Fragment,
  moduleName: string,
  contract: { name: string; address: string },
  alias: string
) {
  const inputSelects = eventFragment.inputs.map(
    (input) => `JSON_VALUE(log, '$.${input.name}') as \`${input.name}\``
  );

  return `-- ${ATTRIBUTION}
-- Event Signature: ${eventFragment.format(ethers.utils.FormatTypes.full)}

{{
  config(
    schema="${moduleName}",
    alias="${alias}",
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
        {{ target.schema }}.decode_log('${eventFragment.format(
          ethers.utils.FormatTypes.json
        )}', data, topics) as log,
        * except(data, topics)
    from
        {{ ref('${moduleName}_logs') }}
    where
        lower(address) = '${contract.address.toLowerCase()}'
        and
        topics[SAFE_OFFSET(0)] = '${ethers.utils.id(
          eventFragment.format("sighash")
        )}'

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(log),
${inputSelects.join(",\n")}
from decoded
`;
}
