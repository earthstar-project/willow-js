export async function collectUint8Arrays(
  it: AsyncIterable<Uint8Array>,
): Promise<Uint8Array> {
  const chunks = [];
  let length = 0;
  for await (const chunk of it) {
    chunks.push(chunk);
    length += chunk.length;
  }
  if (chunks.length === 1) {
    // No need to copy.
    return chunks[0];
  }
  const collected = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    collected.set(chunk, offset);
    offset += chunk.length;
  }
  return collected;
}
