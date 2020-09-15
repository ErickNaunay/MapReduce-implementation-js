import Coordinator, { optionsFailure } from "./Coordinator";

export default class MapReduceExecutor {
  static MapReduce(filePath: string, options: optionsFailure): void {
    console.log("MapReduce Task created");
    const coordinator = new Coordinator(options);
    coordinator
      .start(filePath)
      .then(() => {
        console.log("Coordinator job finished.");
      })
      .catch((err) => {
        console.log("Coordinator node finished with error.");
        console.log(err);
      });
  }
}

export { optionsFailure };
