import { OutputMapperType } from "../Executors/MapperExecutor";
import { OutputShufflerType } from "../Executors/ShuflerExecutor";

export interface IShuffler {
  execute(input: OutputMapperType): Promise<OutputShufflerType>;
}

export class Shuffler implements IShuffler {
  async execute(input: OutputMapperType): Promise<OutputShufflerType> {
    const result: OutputShufflerType = {};
    input = input.sort(this.comparator);
    input.forEach((tuple) => {
      if (result[tuple[0]]) result[tuple[0]].push(tuple[1]);
      else result[tuple[0]] = [tuple[1]];
    });
    return result;
  }

  comparator(a: [string, number], b: [string, number]) {
    if (a[0] < b[0]) return -1;
    if (a[0] > b[0]) return 1;
    return 0;
  }
}
