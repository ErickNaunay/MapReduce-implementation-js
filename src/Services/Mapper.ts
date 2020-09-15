import {
  StreamServiceType as InputServiceType,
  OutputMapperType,
} from "../Executors/MapperExecutor";

export interface IMapper {
  execute(input: InputServiceType): Promise<OutputMapperType>;
}

export class WordCountMapper implements IMapper {
  private readonly regexExpr = /[a-zA-Z']+/g;
  async execute(input: InputServiceType): Promise<OutputMapperType> {
    const dataProcessed: OutputMapperType = [];
    for (const filePath in input) {
      const setInFile = (input[filePath] as string).match(this.regexExpr)?.map((word) => {
        const set: [string, number] = [word, 1];
        return set;
      });

      setInFile && dataProcessed.push(...setInFile);
    }
    return dataProcessed;
  }
}

export default WordCountMapper;
