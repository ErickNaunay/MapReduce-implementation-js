export enum NodesEnum {
  coordinator = "Coordinator",
  mapper = "Mapper",
  combinator = "Combinator",
  reducer = "Reducer",
  shuffler = "Shuffler",
}

export class NodeError extends Error {
  readonly id: number;
  readonly node: NodesEnum;

  constructor(message: string, id: number, node: NodesEnum) {
    super(message);
    this.node = node;
    this.name = `${this.node}Error`;
    this.id = id;
  }
}
export default NodeError;
