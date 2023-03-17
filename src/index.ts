import fs from "fs";
import path from "path";
import glob from "glob";
import * as ethers from "ethers";
import { program } from "commander";
import { getEventSql } from "./lib/get-event-sql";
import { getCallSql } from "./lib/get-call-sql";
import { getRawLogs } from "./lib/get-raw-logs-sql";
import { getRawCalls } from "./lib/get-raw-calls-sql";
import { getSources } from "./lib/get-sources";

program
  .requiredOption(
    "-m, --module-name <module-mame>",
    "The name of the contracts module (i.e. the abi folder name resulting and the database schema."
  )
  .requiredOption(
    "-d, --min-date <min_date>",
    "The minimum date which is used to query the raw BigQuery tables. Use to save billed bytes. Set this to the creation day of the oldest contractin your module e.g. '2023-01-24'."
  );
program.parse();

const options = program.opts<{ moduleName: string; minDate: string }>();
const { moduleName, minDate } = options;
const parsedMinDate = new Date(minDate);
const outputFolder = path.resolve(__dirname, "../output", moduleName);

console.log(
  `Creating dbt files for module "${moduleName}" and minDate=${parsedMinDate
    .toISOString()
    .slice(0, 10)}`
);

const contracts = glob
  .sync(path.resolve(__dirname, "../abis", moduleName, "./*.json"))
  .map((file) => {
    const splits = path.basename(file).split("_");
    const address = splits.pop()?.replace(".json", "") as string;
    const name = splits.join("_");
    const abi = JSON.parse(fs.readFileSync(file, "utf8"));
    return { name, address, abi };
  });

fs.mkdirSync(outputFolder, { recursive: true });

// Sources
const sourcesFilename = path.resolve(outputFolder, `sources.yml`);
fs.writeFileSync(sourcesFilename, getSources());

// Raw Logs
const logsFilename = path.resolve(outputFolder, `${moduleName}_logs.sql`);
fs.writeFileSync(
  logsFilename,
  getRawLogs(moduleName, contracts, parsedMinDate)
);

// Raw Calls
const callsFilename = path.resolve(outputFolder, `${moduleName}_calls.sql`);
fs.writeFileSync(
  callsFilename,
  getRawCalls(moduleName, contracts, parsedMinDate)
);

for (const contract of contracts) {
  const iface = new ethers.utils.Interface(contract.abi);

  // Events
  for (const event of Object.values(iface.events)) {
    const alias = `${contract.name}_event_${event.name}`;
    const filename = path.resolve(outputFolder, `${moduleName}_${alias}.sql`);
    const sql = getEventSql(event, moduleName, contract, alias);

    fs.writeFileSync(filename, sql);
  }

  // Calls
  for (const call of Object.values(iface.functions)) {
    if (call.stateMutability === "view" || call.stateMutability === "pure") {
      continue;
    }

    const alias = `${contract.name}_call_${call.name}`;
    const filename = path.resolve(outputFolder, `${moduleName}_${alias}.sql`);
    const sql = getCallSql(call, moduleName, contract, alias);

    fs.writeFileSync(filename, sql);
  }
}
