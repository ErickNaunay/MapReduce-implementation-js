import { OutputShufflerType } from "../Executors/ShuflerExecutor";
import { StreamServiceType } from "../Executors/MapperExecutor";
export interface IReducer {
  execute(input: OutputShufflerType): Promise<StreamServiceType>;
}

export class WordCountReducer implements IReducer {
    async execute(input: OutputShufflerType): Promise<StreamServiceType>{
        const result: StreamServiceType = {};
        for(const key in input){
            let total = 0;
            input[key].forEach((val)=>{total+= val});
            result[key] = total;
        }
        return result;
    }
}
