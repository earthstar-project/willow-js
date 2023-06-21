import { bytesConcat } from "../../../../deps.ts";
import { find as binarySearch } from "https://deno.land/x/binary_search/mod.ts";

//

class AdaptiveRadixTree<ValueType> {
  root: ArtNode | null = null;

  // Insert a node

  // Delete a node

  // Iterator of all items prefixing a given key.
  itemsPrefixing(key: Uint8Array): AsyncIterable<[Uint8Array, ValueType]> {
  }

  // Iterator of all items this is a prefix of.
  itemsPrefixedBy(key: Uint8Array): AsyncIterable<[Uint8Array, ValueType]> {
  }
}

// Keys are hashes of the full path?

// 32 bytes...

class KVArtNode {
  constructor(
    /** The key of the node. A full concrete key */
    readonly key: Uint8Array,
    /** Header + children */
    readonly value: Uint8Array,
    readonly getNode: (key: Uint8Array) => Promise<KVArtNode | undefined>,
  ) {
    // Look at the header to determine
    // node type (leaf / inner)
    // number of children (4 / 16 / 48 / 256)
  }

  prefixLength() {
    return this.key.byteLength;
  }

  findChild(byte: number): Promise<ArtNode | undefined> {
    console.warn("ArtNode.findChild being used, should be overriden.");
    return Promise.resolve(undefined);
  }
}

class ArtNodeHeader {
  constructor(readonly bytes: Uint8Array) {
  }

  // node type (leaf / inner)
  // number of children (4 / 16 / 48 / 256)
  // compressed path
}

class ArtNode4 extends ArtNode {
  // 4 keys
  // For 0 - 4 keys

  // Just an array of 4 possible keys, for scanning.

  private keys: Uint8Array;

  constructor({ prefix, keys, getNode }: {
    prefix: Uint8Array;
    keys: Uint8Array;
    getNode: (key: Uint8Array) => Promise<ArtNode>;
  }) {
    super(prefix, getNode);

    this.keys = keys;
  }

  override findChild(byte: number): Promise<ArtNode | undefined> {
    for (let i = 0; i < this.keys.byteLength; i++) {
      if (this.keys[i] === byte) {
        const childKey = bytesConcat(
          this.prefix,
          new Uint8Array([this.keys[i]]),
        );
        return this.getNode(childKey);
      }
    }

    return Promise.resolve(undefined);
  }
}

class ArtNode16 extends ArtNode {
  // 16 keys.
  // for 5 - 16 keys

  // Just an array of 16 things, for scanning
  private keys: Uint8Array;

  constructor({ prefix, keys, getNode }: {
    prefix: Uint8Array;
    keys: Uint8Array;
    getNode: (key: Uint8Array) => Promise<ArtNode>;
  }) {
    super(prefix, getNode);

    this.keys = keys;
  }

  override findChild(byte: number): Promise<ArtNode | undefined> {
    const index = binarySearch(Array.from(this.keys), byte, (a, b) => a - b);

    if (index >= 0) {
      const childKey = bytesConcat(
        this.prefix,
        new Uint8Array([this.keys[index]]),
      );
      return this.getNode(childKey);
    }

    return Promise.resolve(undefined);
  }
}

class ArtNode48 {
  // For 17 - 48 keys

  // An index of 256 items, followed by an array of 48 keys

  private index = new Array(256);
  private keys = new Array(48);
}

class ArtNode256 {
  // For 49 - 256 keys

  // An array of 256 keys, direct lookup.

  private keys = new Array(256);
}
