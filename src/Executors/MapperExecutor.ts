import { IMapper } from "../Services/Mapper";
import { ICombinator, Combinator } from "../Services/Combinator";
import FileManager, { IFileManager } from '../Services/FIleManager';

export type StreamServiceType = {
  [key: string]: string | number;
};

export type OutputMapperType = [string, number][];

export enum ExecutorStatusEnum {
  inProgress = "inProgress",
  complete = "complete",
  inactive = "inactive",
}

export interface IMapperExecutor {
  execute(input: StreamServiceType): Promise<StreamServiceType>;
  getStatus(): ExecutorStatusEnum;
  setStatus(status: ExecutorStatusEnum): void;
}

export class MapperExecutor implements IMapperExecutor {
  private status: ExecutorStatusEnum;
  private readonly fileManager: IFileManager;
  private mapper: IMapper;
  private combinator: ICombinator;
  private index: number;

  constructor(mapper: IMapper, index: number, fileManager: IFileManager) {
    this.status = ExecutorStatusEnum.inactive;
    this.mapper = mapper;
    this.combinator = new Combinator();
    this.index = index;
    this.fileManager = fileManager;
  }

  async execute(input: StreamServiceType): Promise<StreamServiceType> {
    const outputMapper = await this.executeMapper(input);
    this.fileManager.storeDataService(outputMapper, this.index);
    const result = await this.executeCombinator(outputMapper);
    console.log(`Mapper Executor job with id = '${this.index}' COMPLETE`)
    return result
  }

  private async executeMapper(input: StreamServiceType): Promise<OutputMapperType> {
    return this.mapper.execute(input);
  }

  private async executeCombinator(input: OutputMapperType): Promise<StreamServiceType> {
    return this.combinator.execute(input);
  }

  setStatus(status: ExecutorStatusEnum) {
    this.status = status;
  }

  getStatus() {
    return this.status;
  }
}

export default MapperExecutor;
