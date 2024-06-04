import {
  type AreaOfInterest,
  intersectRange3d,
  type Range3d,
} from "@earthstar/willow-utils";
import { FIFO } from "@korkje/fifo";
import { WillowError } from "../../errors.ts";
import type { Store } from "../../store/store.ts";
import type { FingerprintScheme, SubspaceScheme } from "../../store/types.ts";
import { COVERS_NONE, IS_ALFIE, type SyncRole } from "../types.ts";

export type ReconcilerOpts<
  Prefingerprint,
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
    Prefingerprint,
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
    Prefingerprint,
    Fingerprint
  >;
};

const SEND_ENTRIES_THRESHOLD = 8;

export class Reconciler<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Prefingerprint,
  Fingerprint,
> {
  private subspaceScheme: SubspaceScheme<SubspaceId>;
  private fingerprintScheme: FingerprintScheme<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    Prefingerprint,
    Fingerprint
  >;

  store: Store<
    NamespaceId,
    SubspaceId,
    PayloadDigest,
    AuthorisationOpts,
    AuthorisationToken,
    Prefingerprint,
    Fingerprint
  >;

  private fingerprintQueue = new FIFO<
    {
      range: Range3d<SubspaceId>;
      fingerprint: Fingerprint;
      covers: bigint | typeof COVERS_NONE;
    }
  >();

  private announceQueue = new FIFO<{
    range: Range3d<SubspaceId>;
    count: number;
    wantResponse: boolean;
    covers: bigint | typeof COVERS_NONE;
  }>();

  range = Promise.withResolvers<Range3d<SubspaceId>>();

  constructor(
    opts: ReconcilerOpts<
      Prefingerprint,
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

    this.determineRange(opts.aoiOurs, opts.aoiTheirs);

    if (opts.role === IS_ALFIE) {
      this.initiate();
    }
  }

  private async determineRange(
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

    this.range.resolve(intersection);
  }

  async initiate() {
    const intersection = await this.range.promise;

    // Initialise sync with that first range.
    const { fingerprint } = await this.store.summarise(intersection);

    const finalised = await this.fingerprintScheme.fingerprintFinalise(
      fingerprint,
    );

    this.fingerprintQueue.push({
      range: intersection,
      fingerprint: finalised,
      covers: COVERS_NONE,
    });
  }

  async respond(
    range: Range3d<SubspaceId>,
    fingerprint: Fingerprint,
    yourRangeCounter: number,
  ) {
    const { fingerprint: fingerprintOurs, size } = await this.store.summarise(
      range,
    );

    const fingerprintOursFinal = await this.fingerprintScheme
      .fingerprintFinalise(fingerprintOurs);

    if (this.fingerprintScheme.isEqual(fingerprint, fingerprintOursFinal)) {
      this.announceQueue.push({
        range,
        count: 0,
        wantResponse: false,
        covers: BigInt(yourRangeCounter),
      });
    } else if (size <= SEND_ENTRIES_THRESHOLD) {
      this.announceQueue.push({
        range,
        count: size,
        wantResponse: true,
        covers: BigInt(yourRangeCounter),
      });

      return;
    } else {
      const [left, right] = await this.store.splitRange(range, size);

      const { fingerprint: fingerprintLeft } = await this.store
        .summarise(left);

      const leftFinal = await this.fingerprintScheme.fingerprintFinalise(
        fingerprintLeft,
      );

      this.fingerprintQueue.push({
        fingerprint: leftFinal,
        range: left,
        covers: COVERS_NONE,
      });

      const { fingerprint: fingerprintRight } = await this.store
        .summarise(right);

      const rightFinal = await this.fingerprintScheme.fingerprintFinalise(
        fingerprintRight,
      );

      this.fingerprintQueue.push({
        fingerprint: rightFinal,
        range: right,
        covers: BigInt(yourRangeCounter),
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
