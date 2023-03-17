# ethereum-bigquery-dbt

A node.js script to create dbt model files to get decoded event and call tables of ethereum smart contracts in Google BigQuery.

![From raw table to decoded tables](/screenshots/img.png?raw=true)

## Usage

### Create a module

A module is set of smart contracts ABI files placed in the `/abis` folder. See the "Artblocks" module in `/abis/Artblocks` for an example. Each ABI filename has to be in the format `$NAME_$ADDRESS.json`.

Example:

```
abis/
├─ Artblocks/
   ├─ Core-v1_0xa7d8d9ef8D8Ce8992Df33D8b8CF4Aebabd5bD270.json
   ├─ Core-v3_0x99a9B7c1116f9ceEB1652de04d5969CcE509B069.json

```

### Create dbt model files

```sh
npm install
npm run create -- --module-name Artblocks --min-date=2022-10-11

# outputs:
# ./output/Artblocks/Artblocks_calls.sql
# ./output/Artblocks/Artblocks_logs.sql
# ...
# ./output/Artblocks/Artblocks_Core-v3_event_Transfer.sql
# ./output/Artblocks/Artblocks_Core-v3_event_Mint.sql
# ...
# ./output/Artblocks/Artblocks_Core-v3_call_transferFrom.sql
# ./output/Artblocks/Artblocks_Core-v3_call_updateProjectName.sql
# ...
```

This will output the dbt model files into the `./output` folder. From there you can copy all the model files which are relevant for your use case to your dbt models folder.

It is recommended to include the `--min-date` parameter to reduce the number of queried bytes when dbt runs its inital query. The min date will be used to limit the queried partitions of the `bigquery-public-data.crypto_ethereum.traces` and `bigquery-public-data.crypto_ethereum.logs` tables. It should be set to the creation date of the oldest contract in your module.

### Setup dbt macros

Additionaly two dbt macros have to be set up to enable data decoding. See `/dbt-example/macros` and `/dbt-example/dbt_project.yml` for how to set up the `create_decode_log_fn` and `create_decode_call_fn` macro. For the macros to work the ethers.js files have to be placed into a GCS bucket. See `https://github.com/pwambach/ethers.js-bigquery` for details.
