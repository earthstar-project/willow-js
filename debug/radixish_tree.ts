import { RadixishTree } from "../src/replica/storage/prefix_iterators/radixish_tree.ts";

const rtree = new RadixishTree();

/*

const p3 = new Uint8Array([23, 9]);
const p2 = new Uint8Array([23, 163]);
const p1 = new Uint8Array([23]);
const p4 = new Uint8Array([23, 163, 9]);

const paths = [p3, p2, p1, p4];

for (const path of paths) {
  await rtree.insert(path, path);

  console.group("after insert", path);
  rtree.print();
  console.groupEnd();
}

await rtree.remove(p2);

rtree.print();

*/

await rtree.insert(
  new Uint8Array([128, 186, 15, 86]),
  new Uint8Array([128, 186, 15, 86]),
);

await rtree.insert(new Uint8Array([128, 186]), new Uint8Array([128, 186]));

await rtree.insert(
  new Uint8Array([128, 186, 15, 86, 190]),
  new Uint8Array([128, 186, 15, 86, 190]),
);

await rtree.insert(
  new Uint8Array([128, 186, 15, 86, 190, 15]),
  new Uint8Array([128, 186, 15, 86, 190, 15]),
);

await rtree.remove(new Uint8Array([128, 186, 15, 86, 190]));

rtree.print();

for await (
  const entry of rtree.prefixedBy(new Uint8Array([128, 186, 15, 86, 190]))
) {
  console.log(entry);
}
