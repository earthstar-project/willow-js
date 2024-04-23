
/** test buffer element */
const testBuf = new Uint8Array([0, 1, 2, 3, 0xff, 0x00, 0xff, 0x00])
const trueBigInt = 9007199254740992n

//======================================
//           test keys
//======================================

// non array pack test
// string 
export const NotAnArray = { 
   name: 'String', 
   key: "Foo", 
   expect: `[2,70,111,111,0]` 
}


// raw byte key
export const Bytes = { 
   name: 'Raw Bytes', 
   key: [testBuf], 
   expect: `[1,0,255,1,2,3,255,0,255,255,0,255,0] note zero escapes`
}

// string key
export const String = { 
   name: 'String', 
   key: ["Foo"], 
   expect: `[2,70,111,111,0]` 
}

// BigInt keys
export const BigIntNeg1k = { 
   name: 'BigInt(-1000)', 
   key: [BigInt(-10000)], 
   expect: `[18,216,239]` 
}

export const BigIntNeg1 = { 
   name: 'BigInt(-1)', 
   key: [BigInt(-1)], 
   expect: `[19,254]` 
}

export const BigInt0 = { 
   name: 'BigInt(0)', 
   key: [BigInt(0)], 
   expect: `[20]` 
}

export const BigIntPos1 = { 
   name: 'BigInt(1)', 
   key: [BigInt(1)], 
   expect: `[21,1]` 
}

export const BigIntPos1k = { 
   name: 'BigInt(1000)', 
   key: [BigInt(10000)], 
   expect: `[22,39,16]` 
}

export const TrueBigInt = { 
   name: 'Very BigInt', 
   key: [trueBigInt], 
   expect: `[27,32,0,0,0,0,0,0]` 
}

// number (double) keys
export const NumberNeg42 = { 
   name: 'Number -42.1', 
   key: [-42.1], 
   expect: `[33,63,186,243,51,51,51,51,50]` 
}

export const NumberNeg0 = { 
   name: 'Number -0.0', 
   key: [-0.0], 
   expect: `[33,127,255,255,255,255,255,255,255]` 
}

export const NumberPos0 = { 
   name: 'Number 0.0', 
   key: [0.0], 
   expect: `[33,128,0,0,0,0,0,0,0]` 
}

export const NumberPos42 = { 
   name: 'Number 42.1', 
   key: [42.1], 
   expect: `[33,192,69,12,204,204,204,204,205]` 
}

export const NumberInfinity = { 
   name: 'Number.Infinity', 
   key: [Number.POSITIVE_INFINITY], 
   expect: `[33,255,240,0,0,0,0,0,0]` 
}

export const NumberNaN =  { 
   name: 'Number.NaN', 
   key: [Number.NaN], 
   expect: `[33,255,248,0,0,0,0,0,0]` //todo currently wrong 
}

// boolean keys
export const False = { 
   name: 'False', 
   key: [false], 
   expect: `[38]` 
}
export const True = { 
   name: 'True', 
   key: [true], 
   expect: `[39]` 
}

// multipart key
export const MultiPart = { 
   name: 'MultiPart', 
   key: ["user", 1], 
   expect: `[2,117,115,101,114,0,33,191,240,0,0,0,0,0,0]` 
}