// deno-lint-ignore-file

/**  
   The following borrows code from:
   https://github.com/josephg/fdb-tuple
    
   This file implements the tuple layer. More details are here:
   https://apple.github.io/foundationdb/data-modeling.html#tuples

   And the type-codes are here:
   https://github.com/apple/foundationdb/blob/master/design/tuple.md

   This code supports KeyParts:
   - Buffer (Uint8Array)
   - String (including unicode)
   - BigInt
   - Number (double)
   - True, 
   - False

   Note: javascript number types don't neatly match the number types used in
   FDB-Tuple encoding. 

   For DenoKv, I'm follow the DenoKv KeyCodec semantics.
   Note: When encoding bigints, the tuple encoding does not differentiate between
   the encoding for an integer and a bigint. Any integer inside the JS safe
   range for a number will be decoded to a 'double' rather than a 'bigint'.
*/

/** exports pack - a DenoKv Key encoder */
export * from './encoder.ts'

/** exports unpack - a DenoKv Key decoder */
export * from './decoder.ts'
