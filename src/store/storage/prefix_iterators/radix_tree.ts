import { orderBytes, type Path } from "@earthstar/willow-utils";
import type { PrefixIterator } from "./types.ts";
import { encodeBase64 } from "@std/encoding/base64";

type RootNode<ValueType> = {
  value: ValueType | null;
  children: Map<string, MemoryNode<ValueType>>;
};

type MemoryNode<ValueType> = {
  pathComponent: Uint8Array;
  /** Null signifies there is no actual entry here. */
  value: ValueType | null;
  /** The keys are the base64 encoding of the path component. */
  children: Map<string, MemoryNode<ValueType>>;
};

export class RadixTree<ValueType> implements PrefixIterator<ValueType> {
  private root: RootNode<ValueType> = {
    value: null,
    children: new Map<string, MemoryNode<ValueType>>(),
  };

  print() {
    const printNode = (node: RootNode<ValueType> | MemoryNode<ValueType>) => {
      if ("pathComponent" in node) {
        console.group(node.pathComponent, node.value);
      } else {
        console.group("Empty string", node.value);
      }

      for (const [key, child] of node.children) {
        console.log(key, "->");
        printNode(child);
      }

      console.groupEnd();
    };

    printNode(this.root);
  }

  insert(path: Path, value: ValueType): Promise<void> {
    // Start at root node
    if (path.length === 0) {
      this.root.value = value;
      return Promise.resolve();
    }

    let node: RootNode<ValueType> | MemoryNode<ValueType> = this.root;

    for (let i = 0; i < path.length; i++) {
      const isLast = i === path.length - 1;

      const component = path[i];

      const componentKey = encodeBase64(component);

      const edge: MemoryNode<ValueType> | undefined = node.children.get(
        componentKey,
      );

      if (!edge) {
        const newNode = {
          pathComponent: component,
          value: isLast ? value : null,
          children: new Map(),
        };

        node.children.set(componentKey, newNode);

        node = newNode;

        continue;
      }

      if (isLast) {
        edge.value = value;
      }

      node = edge;
    }

    return Promise.resolve();
  }

  remove(path: Path): Promise<boolean> {
    if (path.length === 0) {
      this.root.value = null;
      return Promise.resolve(true);
    }

    let node = this.root;

    let remove: (() => void) | null = null;

    for (let i = 0; i < path.length; i++) {
      const isLast = i === path.length - 1;

      const component = path[i];

      const componentKey = encodeBase64(component);

      const edge = node.children.get(componentKey);

      const thisIterationsNode = node;

      // If there's no edge, there's nothing to remove.
      if (!edge) {
        break;
      }

      if (!isLast) {
        if (edge.value === null && edge.children.size === 1) {
          remove = () => {
            thisIterationsNode.children.delete(componentKey);
          };
        } else {
          remove = null;
        }

        node = edge;

        continue;
      }

      if (edge.children.size > 0) {
        edge.value = null;
        return Promise.resolve(true);
      }

      if (remove) {
        remove();

        return Promise.resolve(true);
      }

      node.children.delete(componentKey);

      return Promise.resolve(true);
    }

    return Promise.resolve(false);
  }

  async *prefixesOf(path: Path): AsyncIterable<[Path, ValueType]> {
    if (path.length === 0) {
      return;
    }

    let node = this.root;

    for (let i = 0; i < path.length; i++) {
      const currentComponent = path[i];

      if (node.value) {
        yield [path.slice(0, i), node.value];
      }

      const edge = node.children.get(encodeBase64(currentComponent));

      if (!edge) {
        break;
      }

      node = edge;
    }
  }

  async *prefixedBy(path: Path): AsyncIterable<[Path, ValueType]> {
    // Find the node

    let result: MemoryNode<ValueType> | null = null;
    let node = this.root;

    if (path.length === 0) {
      result = this.root as MemoryNode<ValueType>;
    } else {
      for (let i = 0; i < path.length; i++) {
        const component = path[i];
        const isLast = i === path.length - 1;
        const edge = node.children.get(encodeBase64(component));

        if (!edge) {
          break;
        }

        if (isLast) {
          result = edge;
        }

        node = edge;
      }
    }

    if (!result) {
      return;
    }

    if (result.value) {
      yield [path, result.value];
    }

    for (
      const [prefixedByPath, prefixedByValue] of this.allChildPrefixes(result)
    ) {
      yield [[...path, ...prefixedByPath], prefixedByValue];
    }
  }

  private *allChildPrefixes(
    node: MemoryNode<ValueType>,
  ): Iterable<[Path, ValueType]> {
    const childrenArr = Array.from(node.children.values());

    childrenArr.sort((a, b) => {
      return orderBytes(
        a.pathComponent,
        b.pathComponent,
      );
    });

    for (const child of childrenArr) {
      if (child.value) {
        yield [[child.pathComponent], child.value];
      }

      for (
        const [childChildPath, childChildValue] of this.allChildPrefixes(child)
      ) {
        yield [[child.pathComponent, ...childChildPath], childChildValue];
      }
    }
  }
}
