import { OutputShufflerType } from "../Executors/ShuflerExecutor";
import {
  StreamServiceType,
  ExecutorStatusEnum,
} from "../Executors/MapperExecutor";
import { IReducer } from "../Services/Reducer";

export interface IReduccerExecutor {
  execute(input: OutputShufflerType): Promise<StreamServiceType>;
  getStatus(): ExecutorStatusEnum;
  setStatus(status: ExecutorStatusEnum): void;
}

export class ReducerExecutor implements IReduccerExecutor {
  private status: ExecutorStatusEnum;
  private index: number;
  private reducer: IReducer;

  constructor(reducer: IReducer, index: number) {
    this.status = ExecutorStatusEnum.inactive;
    this.index = index;
    this.reducer = reducer;
    console.log(`Reducer Executor job with id = '${this.index}' CREATED`)
  }

  async execute(input: OutputShufflerType): Promise<StreamServiceType> {
    console.log(`Reducer Executor job with id = '${this.index}' START WORKING`)
    const result = await this.reducer.execute(input);
    console.log(`Reducer Executor job with id = '${this.index}' COMPLETE`)
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
