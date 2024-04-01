import { encodeBase64, PathScheme } from "../../deps.ts";
import { EntryDriver, PayloadDriver } from "../store/storage/types.ts";
import { Store } from "../store/store.ts";
import {
  FingerprintScheme,
  PayloadScheme,
  StoreSchemes,
  SubspaceScheme,
} from "../store/types.ts";

export type StoreDriverCallback<
  Fingerprint,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
> = (namespace: NamespaceId, schemes: {
  payload: PayloadScheme<PayloadDigest>;
  fingerprint: FingerprintScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Fingerprint
  >;
  subspace: SubspaceScheme<SubspaceId>;
  path: PathScheme;
}) => {
  entryDriver: EntryDriver<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Fingerprint
  >;
  payloadDriver?: PayloadDriver<PayloadDigest>;
};

export type StoreHouseOpts<
  Fingerprint,
  AuthorisationToken,
  AuthorisationOpts,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
> = {
  getStoreDrivers: StoreDriverCallback<
    Fingerprint,
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >;
  schemes: StoreSchemes<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Fingerprint
  >;
};

/** A mapping of namespace IDs to stores */
export class StoreMap<
  Fingerprint,
  AuthorisationToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
> {
  private map = new Map<
    string,
    Store<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationOpts,
      AuthorisationToken,
      Fingerprint
    >
  >();

  private schemes: StoreSchemes<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Fingerprint
  >;

  private getStoreDrivers: StoreDriverCallback<
    Fingerprint,
    NamespaceId,
    SubspaceId,
    PayloadDigest
  >;

  constructor(
    opts: StoreHouseOpts<
      Fingerprint,
      AuthorisationToken,
      AuthorisationOpts,
      NamespaceId,
      SubspaceId,
      PayloadDigest
    >,
  ) {
    this.schemes = opts.schemes;
    this.getStoreDrivers = opts.getStoreDrivers;
  }

  private getKey(namespace: NamespaceId): string {
    const encoded = this.schemes.namespace.encode(namespace);
    return encodeBase64(encoded);
  }

  get(
    namespace: NamespaceId,
  ): Store<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Fingerprint
  > {
    const key = this.getKey(namespace);

    const store = this.map.get(key);

    if (store) {
      return store;
    }

    const drivers = this.getStoreDrivers(namespace, this.schemes);

    const newStore = new Store({
      namespace,
      entryDriver: drivers.entryDriver,
      payloadDriver: drivers.payloadDriver,
      schemes: this.schemes,
    });

    this.map.set(key, newStore);

    return newStore;
  }
}
