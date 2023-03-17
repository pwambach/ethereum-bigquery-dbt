{{
  config(
    schema="Artblocks_derived",
    alias="mints",
    materialized="view"
  )
}}

select 
  cast(_tokenId as INT64) as id,
  cast(cast(_tokenId as INT64) / 1e6 as INT64) as project_id,
  _to as receiver_address,
  block_timestamp as minted_at
from
  {{ ref('Artblocks_Core-v3_event_Mint') }}