export type SignFn<KeypairType> = (
  keypair: KeypairType,
  encodedEntry: ArrayBuffer,
) => Promise<ArrayBuffer>;

export type VerifyFn = (
  publicKey: ArrayBuffer,
  signature: ArrayBuffer,
  signed: ArrayBuffer,
) => Promise<boolean>;
