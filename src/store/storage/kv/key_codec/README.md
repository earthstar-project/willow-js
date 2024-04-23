# DenoKv key-codec

This is an encoder and decoder for the Deno-Kv multipart key format (FDB-Tuple).

## Example
```javascript
import { pack, unpack } from './src/mod.ts'

const packed = pack(["app", "users", 1, true])

console.log(packed) 
// returns - [2,97,112,112,0,2,117,115,101,114,115,0,33,191,240,0,0,0,0,0,0,39]

console.log(unpack(packed))
// ["app", "users", 1, true]
```
I've provided the Typescript source code in the **_./src/_** folder.    
A bundled/minified browser compatible version is in **_/dist/kvKeyCodec.js_**.     
The **_./index.html_** will exercise the above js version. 


### Testing:
A full test set can be found in both **_./KeySet_test.ts_**, and in **_./testKeySet.ts_**.    
The expected values were taken from DenoKv-key-codec source.    
These values were then validated by setting and examining keys in a local DenoKv SQLite file.
<br/>

## About this codec
In order to support FoundationDB in Deploy, Deno adopted the FDB-Tuple encoding format for KvKeys. This is a dynamically typed binary format. Its kind of like JSON, but it's binary and doesn't support associative objects.   

This format has some distinct advantages compared to json or msgpack when encoding the keys of a key-value database like DenoKv.

This format is not specific to FoundationDB nor DenoKv. It can be used in many of other places in place of other encoding methods.

The specification for the FDB-Tuple encoding format itself is [documented here](https://github.com/apple/foundationdb/blob/master/design/tuple.md). 

The deno specific kv-codec can be gleened from the rust source code at:    
https://github.com/denoland/denokv/blob/main/proto/codec.rs
    
### Note: 
DenoKv uses a subset of the above FDB specification. (see `Valid KvKeyParts` below)    
The **_./src/doubleCodec.ts_** may seem overkill, but is correct, and supports older browsers.    

## API
This library provides only two public methods - **pack** and **unpack**.   

### pack(key: KvKey) -> Buffer
Pack the specified KvKey into a buffer. A Uint8Array buffer is returned.    
The key param must be a valid Deno.KvKey - an array of Deno.KvKeyParts.

Valid KvKeyParts are:
- bytes (Uint8Array)
- strings - ( including any unicode characters )
- numbers - ( encoded as a double )
- bigints up to 255 bytes long
- Boolean false
- Boolean true

### unpack(val: Uint8Array) -> KvKey
Unpacks the values in a buffer back into an array of KvKeyParts.   
A KvKey is returned.

This method throws an exception if the buffer does not contain a valid KvKey.
