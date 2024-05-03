//export * from "https://deno.land/x/willow_utils@0.7.0/mod.ts";
export * from "../willow_utils/mod.ts";
export { FIFO } from "https://deno.land/x/fifo@v0.2.2/mod.ts";
export {
  type Deferred,
  deferred,
} from "https://deno.land/std@0.202.0/async/deferred.ts";
export { concat } from "https://deno.land/std@0.202.0/bytes/concat.ts";
export { equals as equalsBytes } from "https://deno.land/std@0.202.0/bytes/equals.ts";
export { encode as encodeBase32 } from "https://deno.land/std@0.202.0/encoding/base32.ts";
export { encode as encodeBase64 } from "https://deno.land/std@0.202.0/encoding/base64.ts";
export { toArrayBuffer } from "https://deno.land/std@0.202.0/streams/to_array_buffer.ts";
