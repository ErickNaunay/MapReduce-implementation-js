import fs from "fs";
import MapperExecutor, {
  IMapperExecutor,
  StreamServiceType,
  ExecutorStatusEnum,
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
import ExecutorError, { NodesEnum } from "./Executors/ExecutorError";

export interface optionsFailure {
  mapper: boolean;
  reducer: boolean;
  coordinator: boolean;
}

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
  private options: optionsFailure;

  constructor(options: optionsFailure) {
    console.log("Coordinator job created");
    this.maxNumMappers = +process.env.DEFAULT_MAPPERS!;
    this.maxNumReducers = +process.env.DEFAULT_REDUCERS!;
    this.fileManager = new FileManager(this.maxNumMappers, this.maxNumReducers);
    this.options = options;
    console.log("Failure options: ", options);

    this.mappers = (() => {
      const mappers: IMapperExecutor[] = [];
      let mapperFailIndex = this.maxNumMappers;
      if (this.options.mapper)
        mapperFailIndex = Math.floor(
          Math.random() * Math.floor(this.maxNumMappers)
        );
      for (let i = 0; i < this.maxNumMappers; i++) {
        let fail = false;
        if (i === mapperFailIndex) fail = true;
        mappers.push(
          new MapperExecutor(new WordCountMapper(), i, this.fileManager, fail)
        );
      }
      return mappers;
    })();

    console.log("Coordinator setup mapper jobs");
    this.reducers = (() => {
      let reducerFailIndex = this.maxNumReducers;
      if (this.options.reducer)
        reducerFailIndex = Math.floor(
          Math.random() * Math.floor(this.maxNumReducers)
        );
      const reducers: IReduccerExecutor[] = [];
      for (let i = 0; i < this.maxNumReducers; i++) {
        let fail = false;
        if (i === reducerFailIndex) fail = true;
        reducers.push(new ReducerExecutor(new WordCountReducer(), i, fail));
      }
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
    try {
      if (this.options.coordinator)
        throw new ExecutorError(
          "Coordinator node failure!",
          0,
          NodesEnum.coordinator
        );
      const splitFiles = await this.fileManager.splitFiles(filePath);
      await this.map(splitFiles.files);
      await this.shuffle();
      await this.reduce();
    } catch (err) {
      console.log(
        "Fatal failure on coordinator. Can't recover nor continue. Master STOP"
      );
      throw err;
    }
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

    console.log("Coordinator ready for executing mappers jobs");
    let midData: StreamServiceType[] = [];
    while (true) {
      console.log("MapperExecutors ready to execute...");
      console.log("MapperExecutors set status to inprogress...executing");
      const mapperResult = await Promise.all(
        inputSets.map((input, index) => {
          this.mappers[index].setStatus(ExecutorStatusEnum.inProgress);
          return this.mappers[index].execute(input);
        })
      ).catch((err: ExecutorError) => {
        console.log(err);

        console.log("MapperExecutors set to state inactive...");
        console.log("ReducerExecutors notified...");
        this.mappers.forEach((mapper) =>
          mapper.setStatus(ExecutorStatusEnum.inactive)
        );

        console.log("Creating new mapper and reassamble executor");
        this.mappers[err.id] = new MapperExecutor(
          new WordCountMapper(),
          err.id,
          this.fileManager,
          false
        );
      });

      if (mapperResult) {
        midData = mapperResult;
        break;
      }
    }

    this.mappers.forEach((mapper) =>
      mapper.setStatus(ExecutorStatusEnum.complete)
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
    let result: StreamServiceType[] = [];
    while (true) {
      console.log("ReducerExecutors ready to execute...");
      console.log("ReducerExecutors set status to inprogress...executing");
      const reducersData = await Promise.all(
        this.reducerDist.map(async (val, index) => {
          const data = await this.fileManager.retrieveDataShufflerExecutor(val);
          this.reducers[index].setStatus(ExecutorStatusEnum.inProgress);
          return this.reducers[index].execute(data);
        })
      ).catch((err) => {
        console.log(err);

        console.log("ReducerExecutors set to state inactive...");
        this.reducers.forEach((reducer) =>
          reducer.setStatus(ExecutorStatusEnum.inactive)
        );

        console.log("Creating new reducer and reassamble executor");
        this.reducers[err.id] = new ReducerExecutor(
          new WordCountReducer(),
          err.id,
          false
        );
      });

      if (reducersData) {
        result = reducersData;
        break;
      }
    }

    this.reducers.forEach((reducer) =>
      reducer.setStatus(ExecutorStatusEnum.complete)
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
