import fs from "fs";
import MapperExecutor, {
  IMapperExecutor,
  OutputMapperType,
} from "./Executors/MapperExecutor";
import WordCountMapper from "./Services/Mapper";
import FileManager, { IFileManager } from "./Services/FIleManager";
import ShufflerExecutor, {
  IShufflerExecutor,
  OutputShufflerType,
} from "./Executors/ShuflerExecutor";
import { Shuffler } from "./Services/Shuffler";

export interface ICoordinator {
  map(files: string[]): Promise<void>;
}

export default class Coordinator implements ICoordinator {
  private readonly maxNumMappers: number;
  private readonly maxNumReducers: number;
  private readonly fileManager: IFileManager;
  private mappers: IMapperExecutor[];
  private shuffler: IShufflerExecutor;
  private mapperDist: string[][];
  private reducerDist: string[][];

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
    this.mapperDist = (() => {
      const dist: string[][] = [];
      for (let i = 0; i < this.maxNumMappers; i++) dist.push([]);
      return dist;
    })();
    this.reducerDist = (() => {
      const dist: string[][] = [];
      for (let i = 0; i < this.maxNumReducers; i++) dist.push([]);
      return dist;
    })();
    this.shuffler = new ShufflerExecutor(new Shuffler(), this.fileManager);
  }

  async start(filePath: string): Promise<void> {
    const splitFiles = await this.fileManager.splitFiles(filePath);
    await this.map(splitFiles.files);
    await this.shuffle();
  }

  async map(files: string[]): Promise<void> {
    console.log("Coordinator distributing files into mappers");
    for (let i = 0; i < files.length; i++) {
      const index = i % this.maxNumMappers;
      this.mapperDist[index].push(files[i]);
    }

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
      this.fileManager.storeDataExecutor(outputMapper, index);
    });
  }

  async shuffle(): Promise<void> {
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

    saveData.map((partition, index) => {
      this.fileManager.storeDataShufflerExecutor(partition, index);
    });
  }
}
