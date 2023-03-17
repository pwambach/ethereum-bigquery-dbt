-- generated with https://github.com/pwambach/ethereum-bigquery-dbt

{{
  config(
    schema="Artblocks",
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
    lower(address) IN ('0x99a9b7c1116f9ceeb1652de04d5969cce509b069')

    and
    block_timestamp >= TIMESTAMP('2022-10-11')

    {% if is_incremental() %}
      and date(block_timestamp) >= date_sub(date(_dbt_max_partition), interval 1 day)
    {% endif %}  
  