import {
  OutputMapperType as InputMapperType,
  StreamServiceType,
} from "../Executors/MapperExecutor";

export interface ICombinator {
  execute(input: InputMapperType): Promise<StreamServiceType>;
}

export class Combinator {
  async execute(input: InputMapperType): Promise<StreamServiceType> {
    const combinedData: StreamServiceType = {};
    input.forEach((tuple) => {
      if (combinedData[tuple[0]]) (combinedData[tuple[0]] as number) += 1;
      else combinedData[tuple[0]] = 1;
    });
    return combinedData;
  }
}

export default Combinator;
