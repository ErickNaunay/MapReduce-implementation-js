import { IFileManager } from "../Services/FIleManager";
import { IShuffler } from "../Services/Shuffler";

export type OutputShufflerType = {
  [key: string]: number[];
};

export interface IShufflerExecutor {
  execute(): Promise<OutputShufflerType>;
}

export class ShufflerExecutor {
  private readonly shuffler: IShuffler;
  private readonly fileManager: IFileManager;

  constructor(shuffler: IShuffler, fileManager: IFileManager) {
    this.shuffler = shuffler;
    this.fileManager = fileManager;
    console.log("Shuffler job created.");
  }

  async execute(): Promise<OutputShufflerType> {
    console.log("Shuffler job starting...");
    const data = await this.fileManager.retrieveDataExecutors();
    const result = await this.shuffler.execute(data);
    console.log("Shuffler job COMPLETE");
    return result;
  }
}

export default ShufflerExecutor;
