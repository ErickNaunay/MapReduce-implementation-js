import MapReduceExecutor from "./MapReduce";

console.info("Initialize environment: READY TO START!");
require("dotenv").config();

const start = process.hrtime();
MapReduceExecutor.MapReduce("./data/iliad.txt");
const end = process.hrtime(start);

console.info(`EXECUTION TIME: ${end}ms`);
console.info("WordCount with MAP-REDUCE FINISHED");
