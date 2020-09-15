import fs from "fs";
import MapperExecutor, {
  IMapperExecutor,
  StreamServiceType,
} from "./Executors/MapperExecutor";
import WordCountMapper from "./Services/Mapper";
import FileManager, { IFileManager } from "./Services/FIleManager";
import ShufflerExecutor, {
  IShufflerExecutor,
  OutputShufflerType,
} from "./Executors/ShuflerExecutor";
import {
  IReduccerExecutor,
  ReducerExecutor,
} from "./Executors/ReducerExecutor";
import { WordCountReducer } from "./Services/Reducer";
import { Shuffler } from "./Services/Shuffler";

export interface ICoordinator {
  map(files: string[]): Promise<void>;
}

export default class Coordinator implements ICoordinator {
  private readonly maxNumMappers: number;
  private readonly maxNumReducers: number;
  private readonly fileManager: IFileManager;
  private mappers: IMapperExecutor[];
  private reducers: IReduccerExecutor[];
  private shuffler: IShufflerExecutor;
  private mapperDist: string[][];
  private reducerDist: string[];

  constructor(
    maxMappers: number = +process.env.DEFAULT_MAPPERS!,
    maxReducers: number = +process.env.DEFAULT_REDUCERS!
  ) {
    console.log("Coordinator job created");
    this.maxNumMappers = maxMappers;
    this.maxNumReducers = maxReducers;
    this.fileManager = new FileManager(this.maxNumMappers, this.maxNumReducers);
    this.mappers = (() => {
      const mappers: IMapperExecutor[] = [];
      for (let i = 0; i < maxMappers; i++)
        mappers.push(
          new MapperExecutor(new WordCountMapper(), i, this.fileManager)
        );
      return mappers;
    })();
    console.log("Coordinator setup mapper jobs");
    this.reducers = (() => {
      const reducers: IReduccerExecutor[] = [];
      for (let i = 0; i < maxReducers; i++)
        reducers.push(new ReducerExecutor(new WordCountReducer(), i));
      return reducers;
    })();
    console.log("Coordinator setup reducer jobs");
    this.mapperDist = (() => {
      const dist: string[][] = [];
      for (let i = 0; i < this.maxNumMappers; i++) dist.push([]);
      return dist;
    })();
    this.reducerDist = (() => {
      const dist: string[] = [];
      return dist;
    })();
    this.shuffler = new ShufflerExecutor(new Shuffler(), this.fileManager);
  }

  async start(filePath: string): Promise<void> {
    const splitFiles = await this.fileManager.splitFiles(filePath);
    await this.map(splitFiles.files);
    await this.shuffle();
    await this.reduce();
  }

  async map(files: string[]): Promise<void> {
    console.log("Coordinator distributing files into mappers");
    for (let i = 0; i < files.length; i++) {
      const index = i % this.maxNumMappers;
      this.mapperDist[index].push(files[i]);
    }
    console.log(`MAPPERS partition: `);
    console.log(this.mapperDist);

    console.log("Coordinator loading files in memory for mappers");
    const inputSets = this.mapperDist.map((val) => {
      const data: { [key: string]: string } = {};
      val.forEach((file) => (data[file] = fs.readFileSync(file, "utf-8")));
      return data;
    });

    console.log("Coordinator executing mappers jobs");
    const midData = await Promise.all(
      inputSets.map((input, index) => this.mappers[index].execute(input))
    );

    console.log(
      "All mappers jobs finished. Storaging intermidiate files in disk."
    );
    midData.forEach((outputMapper, index) => {
      this.fileManager.storeDataExecutor(outputMapper, index, "mapper");
    });
  }

  async shuffle(): Promise<void> {
    console.log("Coordinator ready to start shuffler job");
    const data = await this.shuffler.execute();

    const saveData: OutputShufflerType[] = (() => {
      const array: OutputShufflerType[] = [];
      for (let i = 0; i < this.maxNumReducers; i++) array.push({});
      return array;
    })();

    let bucketIndex: number;
    let count = 0;

    for (const key in data) {
      bucketIndex = count % this.maxNumReducers;
      saveData[bucketIndex][key] = data[key];
      count++;
    }

    const filesSaved = saveData.map((partition, index) => {
      return this.fileManager.storeDataShufflerExecutor(partition, index);
    });

    filesSaved.forEach((val, index) => {
      this.reducerDist[index] = val;
    });
    console.log(`REDUCERS partition:`);
    console.log(this.reducerDist);
  }

  async reduce(): Promise<void> {
    const result = await Promise.all(
      this.reducerDist.map(async (val, index) => {
        const data = await this.fileManager.retrieveDataShufflerExecutor(val);
        return this.reducers[index].execute(data);
      })
    );

    console.log(
      "All reducers jobs finished. Storaging intermidiate files in disk."
    );

    result.forEach((val, index) => {
      this.fileManager.storeDataExecutor(val, index, "reducer");
    });

    await this.reduceAll(result);
  }

  private async reduceAll(stream: StreamServiceType[]): Promise<void> {
    console.log("ReduceAll task started...");
    const data: [string, number][] = [];
    stream.map((val) => {
      for (const key in val) {
        data.push([key, +val[key]]);
      }
    });
    data.sort(this.comparatorReduceAll);
    console.log("ReduceAll storaging final ouput on disk...");
    this.fileManager.storeReduceAllData(data);
  }
  private comparatorReduceAll(a: [string, number], b: [string, number]) {
    if (a[1] < b[1]) return 1;
    if (a[1] > b[1]) return -1;
    return 0;
  }
}
