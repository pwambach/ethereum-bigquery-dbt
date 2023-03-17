import { ATTRIBUTION } from "../config";

export function getSources() {
  return `# ${ATTRIBUTION}

version: 2

sources:
  - name: crypto_ethereum
    database: bigquery-public-data
    tables:
      - name: logs
        freshness:
          warn_after: { count: 6, period: hour }
          error_after: { count: 24, period: hour }
          filter: block_timestamp >= timestamp_sub(current_timestamp(), INTERVAL 2 HOUR);
        loaded_at_field: block_timestamp
      - name: traces
        freshness:
          warn_after: { count: 6, period: hour }
          error_after: { count: 24, period: hour }
          filter: block_timestamp >= timestamp_sub(current_timestamp(), INTERVAL 2 HOUR);
        loaded_at_field: block_timestamp
  `;
}
