import { OutputShufflerType } from "../Executors/ShuflerExecutor";
import {
  StreamServiceType,
  ExecutorStatusEnum,
} from "../Executors/MapperExecutor";
import { IReducer } from "../Services/Reducer";
import ExecutorError, { NodesEnum } from "./ExecutorError";

export interface IReduccerExecutor {
  execute(input: OutputShufflerType): Promise<StreamServiceType>;
  getStatus(): ExecutorStatusEnum;
  setStatus(status: ExecutorStatusEnum): void;
}

export class ReducerExecutor implements IReduccerExecutor {
  private status: ExecutorStatusEnum;
  private index: number;
  private reducer: IReducer;
  private readonly failure: boolean;

  constructor(reducer: IReducer, index: number, failure: boolean) {
    this.status = ExecutorStatusEnum.inactive;
    this.index = index;
    this.reducer = reducer;
    this.failure = failure;
    console.log(
      `Reducer Executor (fail=${this.failure}) job with id = '${this.index}' CREATED`
    );
  }

  async execute(input: OutputShufflerType): Promise<StreamServiceType> {
    if (this.failure)
      throw new ExecutorError(
        "Reducer node failure!",
        this.index,
        NodesEnum.reducer
      );
    let result: StreamServiceType = {};
    try {
      console.log(
        `Reducer Executor job with id = '${this.index}' START WORKING`
      );
      result = await this.reducer.execute(input);
      console.log(`Reducer Executor job with id = '${this.index}' COMPLETE`);
    } catch (err) {
      throw new ExecutorError(
        "Reducer Executor job failure!\n" + err,
        this.index,
        NodesEnum.reducer
      );
    }
    return result;
  }

  setStatus(status: ExecutorStatusEnum) {
    this.status = status;
  }

  getStatus() {
    return this.status;
  }
}

export default ReducerExecutor;
