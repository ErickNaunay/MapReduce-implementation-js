import prompt from "prompt-sync";
import MapReduceExecutor from "./MapReduce";

console.info("Initialize environment: Loading!");
require("dotenv").config();

const options: boolean[] = [];
const input = prompt({ sigint: true });

let response: string;
response = input("Test failure on MAPPER node? (Y/N): ");
if (response && response.toLowerCase() === "y") options.push(true);
else options.push(false);
response = input("Test failure on REDUCER node? (Y/N): ");
if (response && response.toLowerCase() === "y") options.push(true);
else options.push(false);
response = input("Test failure on COORDINATOR node? (Y/N): ");
if (response && response.toLowerCase() === "y") options.push(true);
else options.push(false);

console.info("WordCount with MAP-REDUCE: Ready to start!");

const start = process.hrtime();
MapReduceExecutor.MapReduce("./data/iliad.txt", {
  mapper: options[0],
  reducer: options[1],
  coordinator: options[2],
});

const end = process.hrtime(start);
console.info(`EXECUTION TIME: ${end}ms`);

console.info("WordCount with MAP-REDUCE FINISHED");
