import FileManager, { IFileManager } from "./Services/FIleManager";
import Coordinator, { ICoordinator } from "./Coordinator";

export default class MapReduceExecutor {

  static MapReduce(filePath: string): void {
    console.log("MapReduce Task created");
    const coordinator = new Coordinator();
    coordinator.start(filePath).then(()=> {
      console.log('Coordinator job finished.')
    });
  }
}
