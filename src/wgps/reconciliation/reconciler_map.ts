import { Deferred, deferred } from "../../../deps.ts";
import { WgpsMessageValidationError, WillowError } from "../../errors.ts";
import { Reconciler } from "./reconciler.ts";

export class ReconcilerMap<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
  Prefingerprint,
  Fingerprint,
> {
  private map = new Map<
    /** Our AOI handle */
    bigint,
    Map<
      /** Their AOI handle */
      bigint,
      Reconciler<
        NamespaceId,
        SubspaceId,
        PayloadDigest,
        AuthorisationOpts,
        AuthorisationToken,
        Prefingerprint,
        Fingerprint
      >
    >
  >();

  private eventuallyMap = new Map<
    string,
    Deferred<
      Reconciler<
        NamespaceId,
        SubspaceId,
        PayloadDigest,
        AuthorisationOpts,
        AuthorisationToken,
        Prefingerprint,
        Fingerprint
      >
    >
  >();

  addReconciler(
    aoiHandleOurs: bigint,
    aoiHandleTheirs: bigint,
    reconciler: Reconciler<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationOpts,
      AuthorisationToken,
      Prefingerprint,
      Fingerprint
    >,
  ) {
    const existingInnerMap = this.map.get(aoiHandleOurs);

    if (existingInnerMap) {
      existingInnerMap.set(aoiHandleTheirs, reconciler);
    }

    const newInnerMap = new Map<
      /** Their AOI handle */
      bigint,
      Reconciler<
        NamespaceId,
        SubspaceId,
        PayloadDigest,
        AuthorisationOpts,
        AuthorisationToken,
        Prefingerprint,
        Fingerprint
      >
    >();

    newInnerMap.set(aoiHandleTheirs, reconciler);

    this.map.set(aoiHandleOurs, newInnerMap);

    const maybeEventually = this.eventuallyMap.get(
      `${aoiHandleOurs}_${aoiHandleTheirs}`,
    );

    if (maybeEventually) {
      maybeEventually.resolve(reconciler);
    }
  }

  getReconcilerEventually(
    aoiHandleOurs: bigint,
    aoiHandleTheirs: bigint,
  ): Promise<
    Reconciler<
      NamespaceId,
      SubspaceId,
      PayloadDigest,
      AuthorisationOpts,
      AuthorisationToken,
      Prefingerprint,
      Fingerprint
    >
  > {
    try {
      return Promise.resolve(
        this.getReconciler(aoiHandleOurs, aoiHandleTheirs),
      );
    } catch {
      const promise = deferred<
        Reconciler<
          NamespaceId,
          SubspaceId,
          PayloadDigest,
          AuthorisationOpts,
          AuthorisationToken,
          Prefingerprint,
          Fingerprint
        >
      >();

      this.eventuallyMap.set(`${aoiHandleOurs}_${aoiHandleTheirs}`, promise);

      return promise;
    }
  }

  getReconciler(aoiHandleOurs: bigint, aoiHandleTheirs: bigint) {
    const innerMap = this.map.get(aoiHandleOurs);

    if (!innerMap) {
      console.log("oh shit!!!");

      throw new WillowError(
        "Could not dereference one of our AOI handles to a reconciler",
      );
    }

    const reconciler = innerMap.get(aoiHandleTheirs);

    if (!reconciler) {
      throw new WgpsMessageValidationError(
        "Could not dereference one of their AOI handles to a reconciler",
      );
    }

    return reconciler;
  }
}
