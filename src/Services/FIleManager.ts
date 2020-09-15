import fs from "fs";
import lineReader from "line-reader";
import {
  OutputMapperType,
  StreamServiceType,
} from "../Executors/MapperExecutor";
import { OutputShufflerType } from "../Executors/ShuflerExecutor";

type SplitReturn = {
  files: string[];
};

export interface IFileManager {
  splitFiles(filePath: string): Promise<SplitReturn>;
  storeDataService(input: OutputMapperType, index: number): void;
  storeReduceAllData(input: OutputMapperType): void;
  storeDataExecutor(
    input: StreamServiceType,
    index: number,
    type: string
  ): void;
  storeDataShufflerExecutor(input: OutputShufflerType, index: number): string;
  retrieveDataShufflerExecutor(filePath: string): Promise<OutputShufflerType>;
  retrieveDataExecutors(): Promise<OutputMapperType>;
}

export class FileManager implements IFileManager {
  private readonly numMappers: number;
  private readonly numReducers: number;
  constructor(mappers: number, reducers: number) {
    this.numMappers = mappers;
    this.numReducers = reducers;
    console.log("FileManager job created");
  }

  async splitFiles(filePath: string, lineSplit?: number): Promise<SplitReturn> {
    const maxLineSplit = lineSplit || +process.env.DEFAULT_FILE_SPLIT!;
    let countLine = 0;
    let lastSplit = 0;

    let buffer: string = "Hey";
    const result: SplitReturn = { files: [] };

    console.log(`Split file: ${filePath} by ${maxLineSplit} lines. START`);

    return new Promise((resolve, reject) => {
      lineReader.eachLine(filePath, (line, last) => {
        countLine++;
        buffer += " " + line;

        if (countLine % maxLineSplit === 0 || last) {
          lastSplit++;

          const splitDir = `./data/split/split-${lastSplit}.txt`;
          fs.writeFileSync(splitDir, buffer);
          result.files.push(splitDir);

          if (!last) buffer = "";
          else {
            console.log(
              `Split file: ${filePath} into ${lastSplit} files. COMPLETED`
            );
            resolve(result);
            return false;
          }
        }
      });
    });
  }

  storeDataService(input: OutputMapperType, index: number): void {
    let file: string = "";
    input.forEach((tuple, index) => {
      file += `${tuple[0]},${tuple[1]}\n`;
    });
    fs.writeFileSync(`./data/mapper-end/mapper-${index}.txt`, file);
  }

  storeReduceAllData(input: OutputMapperType): void {
    let file: string = "";
    input.forEach((tuple, index) => {
      file += `${tuple[0]},${tuple[1]}\n`;
    });
    fs.writeFileSync(`./data/wordCount.txt`, file);
  }

  storeDataExecutor(
    input: StreamServiceType,
    index: number,
    type: string
  ): void {
    let file: string = "";
    let URL = "";

    if (type === "mapper")
      URL = `./data/mapper-end/combinator-end/mapper-${index}.txt`;
    else URL = `./data/reducer-end/reducer-${index}.txt`;

    for (const key in input) {
      file += `${key},${input[key]}\n`;
    }
    fs.writeFileSync(URL, file);
  }

  storeDataShufflerExecutor(input: OutputShufflerType, index: number): string {
    const filePath = `./data/shuffle/shuffle-partition-${index}.txt`;
    let file: string = "";
    for (const key in input) {
      file += `${key}`;
      input[key].forEach((number) => {
        file += `,${number}`;
      });
      file += "\n";
    }
    fs.writeFileSync(filePath, file);
    return filePath;
  }

  async retrieveDataExecutors(): Promise<OutputMapperType> {
    const result: OutputMapperType = [];
    let filePath = "";
    return new Promise((resolve, reject) => {
      for (let i = 0; i < this.numMappers; i++) {
        filePath = `./data/mapper-end/combinator-end/mapper-${i}.txt`;
        lineReader.eachLine(filePath, (line, last) => {
          const tuple = line.split(",");
          result.push([tuple[0], +tuple[1]]);
          if (last) resolve(result);
        });
      }
    });
  }
  async retrieveDataShufflerExecutor(
    filePath: string
  ): Promise<OutputShufflerType> {
    const result: OutputShufflerType = {};
    return new Promise((resolve, reject) => {
      lineReader.eachLine(filePath, (line, last) => {
        const data = line.split(",");
        result[data[0]] = [];
        data.forEach((val, index) => {
          if (index !== 0) result[data[0]].push(+val);
        });
        if (last) resolve(result);
      });
    });
  }
}

export default FileManager;
