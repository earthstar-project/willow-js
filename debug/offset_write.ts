const temp = await Deno.makeTempFile({ dir: "./debug" });

const encoded = new TextEncoder().encode("Hello world");

const file = await Deno.open(temp, { write: true });

await file.write(encoded);

await file.seek(6, Deno.SeekMode.Start);

const encoded2 = new TextEncoder().encode("moons");

const writer = file.writable.getWriter();
await writer.write(encoded2);
