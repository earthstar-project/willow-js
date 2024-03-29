import {
  AreaOfInterest,
  FIFO,
  intersectRange3d,
  Range3d,
} from "../../../deps.ts";
import { WillowError } from "../../errors.ts";
import { Store } from "../../store/store.ts";
import { FingerprintScheme, SubspaceScheme } from "../../store/types.ts";
import { IS_ALFIE, SyncRole } from "../types.ts";

export type ReconcilerOpts<
  Fingerprint,
  AuthorisationToken,
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
> = {
  role: SyncRole;
  subspaceScheme: SubspaceScheme<SubspaceId>;
  fingerprintScheme: FingerprintScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Fingerprint
  >;
  namespace: NamespaceId;
  aoiOurs: AreaOfInterest<SubspaceId>;
  aoiTheirs: AreaOfInterest<SubspaceId>;
  store: Store<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Fingerprint
  >;
};

const SEND_ENTRIES_THRESHOLD = 16;

export class Reconciler<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Fingerprint,
> {
  private subspaceScheme: SubspaceScheme<SubspaceId>;
  private fingerprintScheme: FingerprintScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Fingerprint
  >;
  private store: Store<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Fingerprint
  >;

  private fingerprintQueue = new FIFO<
    { range: Range3d<SubspaceId>; fingerprint: Fingerprint }
  >();

  private announceQueue = new FIFO<{
    range: Range3d<SubspaceId>;
    count: number;
    wantResponse: boolean;
  }>();

  constructor(
    opts: ReconcilerOpts<
      Fingerprint,
      AuthorisationToken,
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationOpts
    >,
  ) {
    this.fingerprintScheme = opts.fingerprintScheme;
    this.subspaceScheme = opts.subspaceScheme;
    this.store = opts.store;

    if (opts.role === IS_ALFIE) {
      this.initiate(opts.aoiOurs, opts.aoiTheirs);
    }
  }

  async initiate(
    aoi1: AreaOfInterest<SubspaceId>,
    aoi2: AreaOfInterest<SubspaceId>,
  ) {
    // Remove the interest from both.
    const range1 = await this.store.areaOfInterestToRange(aoi1);
    const range2 = await this.store.areaOfInterestToRange(aoi2);

    const intersection = intersectRange3d(
      this.subspaceScheme.order,
      range1,
      range2,
    );

    // Intersect the de-interested ranges
    if (!intersection) {
      throw new WillowError(
        "There was no intersection between two range-ified AOIs. That shouldn't happen...",
      );
    }

    // Initialise sync with that first range.
    const { fingerprint } = await this.store.summarise(intersection);

    this.fingerprintQueue.push({ range: intersection, fingerprint });
  }

  async respond(
    range: Range3d<SubspaceId>,
    fingerprint: Fingerprint,
  ) {
    const { fingerprint: fingerprintOurs, size } = await this.store.summarise(
      range,
    );

    if (this.fingerprintScheme.isEqual(fingerprint, fingerprintOurs)) {
      this.announceQueue.push({
        range,
        count: 0,
        wantResponse: false,
      });
    } else if (size <= SEND_ENTRIES_THRESHOLD) {
      this.announceQueue.push({
        range,
        count: size,
        wantResponse: true,
      });

      return;
    } else {
      const [left, right] = await this.store.splitRange(range, size);

      const { fingerprint: fingerprintLeft } = await this.store
        .summarise(left);

      this.fingerprintQueue.push({
        fingerprint: fingerprintLeft,
        range: left,
      });

      const { fingerprint: fingerprintRight } = await this.store
        .summarise(right);

      this.fingerprintQueue.push({
        fingerprint: fingerprintRight,
        range: right,
      });
    }
  }

  async *fingerprints() {
    for await (const details of this.fingerprintQueue) {
      yield details;
    }
  }

  async *entryAnnouncements() {
    for await (const announcement of this.announceQueue) {
      yield announcement;
    }
  }
}
