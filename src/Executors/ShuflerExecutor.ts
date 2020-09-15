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

  constructor(
    shuffler: IShuffler,
    fileManager: IFileManager
  ) {
    this.shuffler = shuffler;
    this.fileManager = fileManager;
  }

  async execute(): Promise<OutputShufflerType> {
    const data = await this.fileManager.retrieveDataExecutors();
    return await this.shuffler.execute(data)
  }
}

export default ShufflerExecutor;
