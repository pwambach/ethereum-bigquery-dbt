import { ethers } from "ethers";
import { ATTRIBUTION } from "../config";

export function getCallSql(
  callFragment: ethers.utils.Fragment,
  moduleName: string,
  contract: { name: string; address: string },
  alias: string
) {
  const inputSelects = callFragment.inputs.map(
    (input) => `JSON_VALUE(call, '$.${input.name}') as \`${input.name}\``
  );

  return `-- ${ATTRIBUTION}
-- Call Signature: ${callFragment.format(ethers.utils.FormatTypes.full)}

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
        {{ target.schema }}.decode_call('${callFragment.format(
          ethers.utils.FormatTypes.json
        )}', input) as call,
        * except(input)
    from
        {{ ref('${moduleName}_calls') }}
    where
        lower(to_address) = '${contract.address.toLowerCase()}'
        and
        starts_with(input, '${ethers.utils
          .id(callFragment.format("sighash"))
          .slice(0, 10)}')

        {% if is_incremental() %}
          and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
        {% endif %}  
)

select
* except(call),
${inputSelects.join(",\n")}
from decoded
`;
}
