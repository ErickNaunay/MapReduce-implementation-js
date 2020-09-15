import { IMapper } from "../Services/Mapper";
import { ICombinator, Combinator } from "../Services/Combinator";
import { IFileManager } from "../Services/FIleManager";
import ExecutorError, { NodesEnum } from "./ExecutorError";

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
  private readonly failure: boolean = false;

  constructor(
    mapper: IMapper,
    index: number,
    fileManager: IFileManager,
    failure: boolean
  ) {
    this.status = ExecutorStatusEnum.inactive;
    this.failure = failure;
    this.mapper = mapper;
    this.combinator = new Combinator();
    this.index = index;
    this.fileManager = fileManager;
    console.log(
      `Mapper Executor (fail=${this.failure}) job with id = '${this.index}' CREATED`
    );
  }

  async execute(input: StreamServiceType): Promise<StreamServiceType> {
    if (this.failure)
      throw new ExecutorError(
        "MapperExecutor job failure!",
        this.index,
        NodesEnum.mapper
      );

    let result: StreamServiceType = {};

    try {
      console.log(
        `Mapper Executor job with id = '${this.index}' START WORKING`
      );
      const outputMapper = await this.executeMapper(input);
      this.fileManager.storeDataService(outputMapper, this.index);
      result = await this.executeCombinator(outputMapper);
      console.log(`Mapper Executor job with id = '${this.index}' COMPLETE`);
    } catch (err) {
      throw new ExecutorError(
        "MapperExecutor job failure!\n" + err,
        this.index,
        NodesEnum.mapper
      );
    }
    return result;
  }

  private async executeMapper(
    input: StreamServiceType
  ): Promise<OutputMapperType> {
    return this.mapper.execute(input);
  }

  private async executeCombinator(
    input: OutputMapperType
  ): Promise<StreamServiceType> {
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
