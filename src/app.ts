import MapReduceExecutor from './MapReduce';

console.log("Initialize environment: READY TO START!");
require('dotenv').config();

MapReduceExecutor.MapReduce('./data/iliad.txt');