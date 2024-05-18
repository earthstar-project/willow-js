import { type AreaOfInterest, intersectArea } from "@earthstar/willow-utils";
import { FIFO } from "fifo";
import { WillowError } from "../../errors.ts";
import type { NamespaceScheme, SubspaceScheme } from "../../store/types.ts";
import type { HandleStore } from "../handle_store.ts";

export type AoiIntersectionFinderOpts<NamespaceId, SubspaceId> = {
  namespaceScheme: NamespaceScheme<NamespaceId>;
  subspaceScheme: SubspaceScheme<SubspaceId>;
  handlesOurs: HandleStore<AreaOfInterest<SubspaceId>>;
  handlesTheirs: HandleStore<AreaOfInterest<SubspaceId>>;
};

export class AoiIntersectionFinder<NamespaceId, SubspaceId> {
  private namespaceScheme: NamespaceScheme<NamespaceId>;
  private subspaceScheme: SubspaceScheme<SubspaceId>;

  private handlesOurs: HandleStore<AreaOfInterest<SubspaceId>>;
  private handlesTheirs: HandleStore<AreaOfInterest<SubspaceId>>;

  private handlesOursNamespaceMap = new Map<bigint, NamespaceId>();
  private handlesTheirsNamespaceMap = new Map<bigint, NamespaceId>();

  private intersectingAoiQueue = new FIFO<
    { namespace: NamespaceId; ours: bigint; theirs: bigint }
  >();

  constructor(
    opts: AoiIntersectionFinderOpts<NamespaceId, SubspaceId>,
  ) {
    this.namespaceScheme = opts.namespaceScheme;
    this.subspaceScheme = opts.subspaceScheme;

    this.handlesOurs = opts.handlesOurs;
    this.handlesTheirs = opts.handlesTheirs;
  }

  addAoiHandleForNamespace(
    handle: bigint,
    namespace: NamespaceId,
    ours: boolean,
  ): void {
    const handleNamespaceMap = ours
      ? this.handlesOursNamespaceMap
      : this.handlesTheirsNamespaceMap;
    const otherHandleNamespaceMap = ours
      ? this.handlesTheirsNamespaceMap
      : this.handlesOursNamespaceMap;

    const handleStore = ours ? this.handlesOurs : this.handlesTheirs;
    const otherHandleStore = ours ? this.handlesTheirs : this.handlesOurs;

    handleNamespaceMap.set(handle, namespace);

    // Now check for all other AOIs with the same namespace.
    for (const [otherHandle, otherNamespace] of otherHandleNamespaceMap) {
      if (!this.namespaceScheme.isEqual(namespace, otherNamespace)) {
        continue;
      }

      const aoi = handleStore.get(handle);

      if (!aoi) {
        throw new WillowError("Could not dereference an AOI handle");
      }

      const aoiOther = otherHandleStore.get(otherHandle);

      if (!aoiOther) {
        throw new WillowError("Could not dereference an AOI handle");
      }

      const intersection = intersectArea(
        this.subspaceScheme.order,
        aoi.area,
        aoiOther.area,
      );

      if (!intersection) {
        continue;
      }

      this.intersectingAoiQueue.push({
        namespace,
        ours: ours ? handle : otherHandle,
        theirs: ours ? otherHandle : handle,
      });
    }
  }

  handleToNamespaceId(handle: bigint, ours: boolean): NamespaceId | undefined {
    const handleNamespaceMap = ours
      ? this.handlesOursNamespaceMap
      : this.handlesTheirsNamespaceMap;

    return handleNamespaceMap.get(handle);
  }

  async *intersections() {
    for await (const intersection of this.intersectingAoiQueue) {
      yield intersection;
    }
  }
}
