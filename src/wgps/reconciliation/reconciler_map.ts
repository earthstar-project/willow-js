import { WgpsMessageValidationError, WillowError } from "../../errors.ts";
import { Reconciler } from "./reconciler.ts";

export class ReconcilerMap<
  NamespaceId,
  SubspaceId,
  PayloadDigest,
  AuthorisationOpts,
  AuthorisationToken,
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
        Fingerprint
      >
    >();

    newInnerMap.set(aoiHandleTheirs, reconciler);

    this.map.set(aoiHandleOurs, newInnerMap);
  }

  getReconciler(aoiHandleOurs: bigint, aoiHandleTheirs: bigint) {
    const innerMap = this.map.get(aoiHandleOurs);

    if (!innerMap) {
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
