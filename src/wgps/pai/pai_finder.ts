import {
  ANY_SUBSPACE,
  Area,
  FIFO,
  OPEN_END,
  Path,
  prefixesOf,
} from "../../../deps.ts";
import { WgpsMessageValidationError, WillowError } from "../../errors.ts";
import { NamespaceScheme } from "../../store/types.ts";
import { HandleStore } from "../handle_store.ts";
import { ReadAuthorisation, ReadAuthorisationSubspace } from "../types.ts";
import { isSubspaceReadAuthorisation } from "../util.ts";
import {
  Fragment,
  FragmentKit,
  FragmentPair,
  FragmentSet,
  FragmentsSelective,
  FragmentTriple,
  Intersection,
  PaiScheme,
} from "./types.ts";

export type PaiFinderOpts<
  ReadCapability,
  PsiGroup,
  PsiScalar,
  NamespaceId,
  SubspaceId,
> = {
  namespaceScheme: NamespaceScheme<NamespaceId>;
  paiScheme: PaiScheme<
    ReadCapability,
    PsiGroup,
    PsiScalar,
    NamespaceId,
    SubspaceId
  >;
  intersectionHandlesOurs: HandleStore<Intersection<PsiGroup>>;
  intersectionHandlesTheirs: HandleStore<Intersection<PsiGroup>>;
};

const DO_NOTHING = Symbol("do_nothing");
const BIND_READ_CAP = Symbol("bind_read_cap");
const REQUEST_SUBSPACE_CAP = Symbol("req_subspace_cap");

/** Some locally stored information about a given fragment group */
type LocalFragmentInfo<
  ReadCapability,
  SubspaceReadCapability,
  SyncSignature,
  SyncSubspaceSignature,
  NamespaceId,
  SubspaceId,
> = {
  onIntersection: typeof DO_NOTHING;
  authorisation: ReadAuthorisation<
    ReadCapability,
    SubspaceReadCapability,
    SyncSignature,
    SyncSubspaceSignature
  >;
  path: Path;
  namespace: NamespaceId;
  subspace: SubspaceId | typeof ANY_SUBSPACE;
} | {
  onIntersection: typeof BIND_READ_CAP;
  authorisation: {
    capability: ReadCapability;
    signature: SyncSignature;
  };
  path: Path;
  namespace: NamespaceId;
  subspace: SubspaceId | typeof ANY_SUBSPACE;
} | {
  onIntersection: typeof REQUEST_SUBSPACE_CAP;
  authorisation: {
    capability: ReadCapability;
    subspaceCapability: SubspaceReadCapability;
    signature: SyncSignature;
    subspaceSignature: SyncSubspaceSignature;
  };
  path: Path;
  namespace: NamespaceId;
  subspace: typeof ANY_SUBSPACE;
};

/** Given `ReadAuthorisation`s, emits intersected  */
export class PaiFinder<
  ReadCapability,
  SyncSignature,
  PsiGroup,
  PsiScalar,
  SubspaceReadCapability,
  SyncSubspaceSignature,
  NamespaceId,
  SubspaceId,
> {
  private intersectionHandlesOurs: HandleStore<Intersection<PsiGroup>>;
  private intersectionHandlesTheirs: HandleStore<Intersection<PsiGroup>>;

  /** Queue of: a read capability to bind, and a handle of our own intersection this is related to, and the outer area to encode against.  */
  private intersectionQueue = new FIFO<
    [
      ReadAuthorisation<
        ReadCapability,
        SubspaceReadCapability,
        SyncSignature,
        SyncSubspaceSignature
      >,
      bigint,
      Area<SubspaceId>,
    ]
  >();

  /** Queue of: a fragment group to bind, and whether it was derived from a secondary fragment. */
  private bindFragmentQueue = new FIFO<[PsiGroup, boolean]>();
  /** Queue of: the fragment group being replied to, and the multiplied result */
  private replyFragmentQueue = new FIFO<[bigint, PsiGroup]>();
  /** Queue of: the handle of the intersecting handle we would like a subspace cap for. */
  private subspaceCapRequestQueue = new FIFO<bigint>();
  /** Queue of: the handle of the intersection being rseponded to, and the subspace cap. */
  private subspaceCapReplyQueue = new FIFO<[bigint, SubspaceReadCapability]>();

  private fragmentsInfo = new Map<
    bigint,
    LocalFragmentInfo<
      ReadCapability,
      SubspaceReadCapability,
      SyncSignature,
      SyncSubspaceSignature,
      NamespaceId,
      SubspaceId
    >
  >();

  private namespaceScheme: NamespaceScheme<NamespaceId>;

  private paiScheme: PaiScheme<
    ReadCapability,
    PsiGroup,
    PsiScalar,
    NamespaceId,
    SubspaceId
  >;

  private scalar: PsiScalar;

  private requestedSubspaceCapHandles = new Set<bigint>();

  constructor(
    opts: PaiFinderOpts<
      ReadCapability,
      PsiGroup,
      PsiScalar,
      NamespaceId,
      SubspaceId
    >,
  ) {
    this.namespaceScheme = opts.namespaceScheme;
    this.paiScheme = opts.paiScheme;
    this.scalar = opts.paiScheme.getScalar();
    this.intersectionHandlesOurs = opts.intersectionHandlesOurs;
    this.intersectionHandlesTheirs = opts.intersectionHandlesTheirs;
  }

  /** Submit a ReadAuthorisation for private set intersection. */
  async submitAuthorisation(
    authorisation: ReadAuthorisation<
      ReadCapability,
      SubspaceReadCapability,
      SyncSignature,
      SyncSubspaceSignature
    >,
  ) {
    const fragmentKit = this.paiScheme.getFragmentKit(authorisation.capability);

    const fragments = createFragmentSet(fragmentKit);

    const submitFragment = async (
      fragment: Fragment<NamespaceId, SubspaceId>,
      isSecondary: boolean,
    ): Promise<bigint> => {
      // Hash the fragment to a group.
      const unmixed = await this.paiScheme.fragmentToGroup(fragment);
      // Multiply it using the scalar.
      const multiplied = this.paiScheme.scalarMult(unmixed, this.scalar);
      // Put into our intersection store with state pending.
      const handle = this.intersectionHandlesOurs.bind({
        group: multiplied,
        isComplete: false,
        isSecondary,
      });
      // Send the group to the bind queue.
      this.bindFragmentQueue.push([multiplied, isSecondary]);

      return handle;
    };

    if (!isSelectiveFragmentKit(fragments)) {
      for (let i = 0; i < fragments.length; i++) {
        const fragment = fragments[i];
        const [namespace, path] = fragment;
        const isMostSpecific = i === fragments.length - 1;

        const groupHandle = await submitFragment(fragment, false);

        if (isMostSpecific) {
          this.fragmentsInfo.set(groupHandle, {
            onIntersection: BIND_READ_CAP,
            authorisation: authorisation,
            namespace,
            path,
            subspace: ANY_SUBSPACE,
          });
        } else {
          this.fragmentsInfo.set(groupHandle, {
            onIntersection: DO_NOTHING,
            authorisation: authorisation,
            namespace,
            path,
            subspace: ANY_SUBSPACE,
          });
        }
      }
    } else {
      for (let i = 0; i < fragments.primary.length; i++) {
        const fragment = fragments.primary[i];
        const [namespace, subspace, path] = fragment;
        const isMostSpecific = i === fragments.primary.length - 1;

        const groupHandle = await submitFragment(fragment, false);

        if (isMostSpecific) {
          this.fragmentsInfo.set(groupHandle, {
            onIntersection: BIND_READ_CAP,
            authorisation: authorisation,
            namespace,
            path,
            subspace,
          });
        } else {
          this.fragmentsInfo.set(groupHandle, {
            onIntersection: DO_NOTHING,
            authorisation: authorisation,
            namespace,
            path,
            subspace,
          });
        }
      }

      for (let i = 0; i < fragments.secondary.length; i++) {
        const fragment = fragments.secondary[i];
        const [namespace, path] = fragment;
        const isMostSpecific = i === fragments.secondary.length - 1;

        const groupHandle = await submitFragment(fragment, true);

        if (isMostSpecific) {
          this.fragmentsInfo.set(groupHandle, {
            onIntersection: REQUEST_SUBSPACE_CAP,
            authorisation: authorisation as ReadAuthorisationSubspace<
              ReadCapability,
              SubspaceReadCapability,
              SyncSignature,
              SyncSubspaceSignature
            >,
            namespace,
            path,
            subspace: ANY_SUBSPACE,
          });
        } else {
          this.fragmentsInfo.set(groupHandle, {
            onIntersection: DO_NOTHING,
            authorisation: authorisation,
            namespace,
            path,
            subspace: ANY_SUBSPACE,
          });
        }
      }
    }
  }

  receivedBind(groupMember: PsiGroup, isSecondary: boolean) {
    const multiplied = this.paiScheme.scalarMult(groupMember, this.scalar);

    const handle = this.intersectionHandlesTheirs.bind({
      group: multiplied,
      isComplete: true,
      isSecondary,
    });

    this.replyFragmentQueue.push([handle, multiplied]);

    this.checkForIntersections(handle, false);
  }

  receivedReply(handle: bigint, groupMember: PsiGroup) {
    const intersection = this.intersectionHandlesOurs.get(handle);

    if (!intersection) {
      throw new WgpsMessageValidationError(
        "Got a reply for a non-existent intersection handle",
      );
    }

    this.intersectionHandlesOurs.update(handle, {
      group: groupMember,
      isComplete: true,
      isSecondary: intersection.isSecondary,
    });

    this.checkForIntersections(handle, true);
  }

  receivedSubspaceCapRequest(handle: bigint) {
    const result = this.intersectionHandlesTheirs.get(handle);

    if (!result) {
      throw new WgpsMessageValidationError(
        "PAI: partner requested subspace capability using unknown handle.",
      );
    }

    for (const [ourHandle, intersection] of this.intersectionHandlesOurs) {
      if (!intersection.isComplete) {
        continue;
      }

      // Check for equality.
      if (
        !this.paiScheme.isGroupEqual(
          intersection.group,
          result.group,
        )
      ) {
        continue;
      }

      const fragmentInfo = this.fragmentsInfo.get(ourHandle);

      if (!fragmentInfo) {
        throw new WillowError(
          "Couldn't dereference a known intersection handle's associated info. Whoops.",
        );
      }

      if (!isSubspaceReadAuthorisation(fragmentInfo.authorisation)) {
        continue;
      }

      this.subspaceCapReplyQueue.push([
        handle,
        fragmentInfo.authorisation.subspaceCapability,
      ]);
    }
  }

  receivedVerifiedSubspaceCapReply(
    handle: bigint,
    namespace: NamespaceId,
  ) {
    if (this.requestedSubspaceCapHandles.has(handle) === false) {
      throw new WgpsMessageValidationError(
        "PAI: Partner replied with subspace cap for handle which we never sent a request for.",
      );
    }

    this.requestedSubspaceCapHandles.delete(handle);

    const result = this.intersectionHandlesOurs.get(handle);

    if (!result) {
      throw new WgpsMessageValidationError(
        "PAI: partner replied with subspace capability using unknown handle.",
      );
    }

    const fragmentInfo = this.fragmentsInfo.get(handle);

    if (!fragmentInfo) {
      throw new WillowError(
        "Couldn't dereference a known intersection handle's associated info. Whoops.",
      );
    }

    if (
      this.namespaceScheme.isEqual(fragmentInfo.namespace, namespace) === false
    ) {
      throw new WgpsMessageValidationError(
        "PAI: partner replied with subspace capability for the wrong namespace.",
      );
    }

    const outer = this.getHandleOuterArea(handle);

    this.intersectionQueue.push([
      fragmentInfo.authorisation,
      handle,
      outer,
    ]);
  }

  private checkForIntersections(handle: bigint, ours: boolean) {
    const storeToGetHandleFrom = ours
      ? this.intersectionHandlesOurs
      : this.intersectionHandlesTheirs;
    const storeToCheckAgainst = ours
      ? this.intersectionHandlesTheirs
      : this.intersectionHandlesOurs;

    const intersection = storeToGetHandleFrom.get(handle);

    if (!intersection) {
      throw new WillowError("Tried to get a handle we don't have");
    }

    if (!intersection.isComplete) {
      return;
    }

    // Here we are looping through the whole contents of the handle store because...
    // otherwise we need to build a special handle store just for intersections.
    // Which we might do one day, but I'm not convinced it's worth it yet.
    for (
      const [otherHandle, otherIntersection] of storeToCheckAgainst
    ) {
      if (!otherIntersection.isComplete) {
        continue;
      }

      // Continue here to avoid the false positive of same namespace + path but different subspaces.
      if (intersection.isSecondary && otherIntersection.isSecondary) {
        continue;
      }

      // Check for equality.
      if (
        !this.paiScheme.isGroupEqual(
          intersection.group,
          otherIntersection.group,
        )
      ) {
        continue;
      }

      // If there is an intersection, check what we have to do!
      const ourHandle = ours ? handle : otherHandle;

      const fragmentInfo = this.fragmentsInfo.get(ourHandle);

      if (!fragmentInfo) {
        throw new WillowError("Had no fragment info!");
      }

      const outer = this.getHandleOuterArea(ourHandle);

      if (fragmentInfo.onIntersection === BIND_READ_CAP) {
        this.intersectionQueue.push([
          fragmentInfo.authorisation,
          ourHandle,
          outer,
        ]);
      } else if (
        fragmentInfo.onIntersection === REQUEST_SUBSPACE_CAP
      ) {
        this.requestedSubspaceCapHandles.add(ourHandle);
        this.subspaceCapRequestQueue.push(handle);
      }
    }
  }

  private getHandleOuterArea(handle: bigint): Area<SubspaceId> {
    const fragmentInfo = this.fragmentsInfo.get(handle);

    if (!fragmentInfo) {
      throw new WillowError("Had no fragment info!");
    }

    return {
      includedSubspaceId: fragmentInfo.subspace,
      pathPrefix: fragmentInfo.path,
      timeRange: {
        start: BigInt(0),
        end: OPEN_END,
      },
    };
  }

  getIntersectionPrivy(
    handle: bigint,
  ): { namespace: NamespaceId; outer: Area<SubspaceId> } {
    // This handle is theirs.
    // Find which one of ours it intersects.
    // Return the namespace and outer area.
    const theirIntersection = this.intersectionHandlesTheirs.get(handle);

    if (theirIntersection === undefined) {
      throw new WgpsMessageValidationError(
        "Partner tried to bind read capability for unknown intersection handle",
      );
    }

    for (const [ourHandle, ourIntersection] of this.intersectionHandlesOurs) {
      if (!ourIntersection.isComplete) {
        continue;
      }

      // Continue here to avoid the false positive of same namespace + path but different subspaces.
      if (ourIntersection.isSecondary && theirIntersection.isSecondary) {
        continue;
      }

      // Check for equality.
      if (
        !this.paiScheme.isGroupEqual(
          ourIntersection.group,
          theirIntersection.group,
        )
      ) {
        continue;
      }

      const fragmentInfo = this.fragmentsInfo.get(ourHandle);

      if (!fragmentInfo) {
        throw new WillowError("Had no fragment info!");
      }

      const outer = this.getHandleOuterArea(ourHandle);

      return {
        namespace: fragmentInfo.namespace,
        outer,
      };
    }

    throw new WgpsMessageValidationError(
      "Partner tried to bind read capability for a handle with no intersection to ours",
    );
  }

  async *fragmentBinds() {
    for await (const [group, isSecondary] of this.bindFragmentQueue) {
      yield {
        group,
        isSecondary,
      };
    }
  }

  async *fragmentReplies() {
    for await (const [handle, groupMember] of this.replyFragmentQueue) {
      yield {
        handle,
        groupMember,
      };
    }
  }

  async *intersections() {
    for await (
      const [authorisation, handle, outer] of this
        .intersectionQueue
    ) {
      yield {
        authorisation,
        handle,
        outer,
      };
    }
  }

  async *subspaceCapRequests() {
    for await (const handle of this.subspaceCapRequestQueue) {
      yield handle;
    }
  }

  async *subspaceCapReplies() {
    for await (const [handle, subspaceCap] of this.subspaceCapReplyQueue) {
      yield {
        handle,
        subspaceCap,
      };
    }
  }
}

/** Returns a fragment set, where fragments are ordered from least to most specific. */
function createFragmentSet<NamespaceId, SubspaceId>(
  kit: FragmentKit<NamespaceId, SubspaceId>,
): FragmentSet<NamespaceId, SubspaceId> {
  if ("grantedSubspace" in kit) {
    const primaryFragments: FragmentTriple<NamespaceId, SubspaceId>[] = [];
    const secondaryFragments: FragmentPair<NamespaceId>[] = [];

    const prefixes = prefixesOf(kit.grantedPath);

    for (const prefix of prefixes) {
      primaryFragments.push([
        kit.grantedNamespace,
        kit.grantedSubspace,
        prefix,
      ]);
      secondaryFragments.push([kit.grantedNamespace, prefix]);
    }

    return {
      primary: primaryFragments,
      secondary: secondaryFragments,
    };
  }

  const prefixes = prefixesOf(kit.grantedPath);

  const pairs: FragmentPair<NamespaceId>[] = [];

  for (const prefix of prefixes) {
    pairs.push([kit.grantedNamespace, prefix]);
  }

  return pairs;
}

function isSelectiveFragmentKit<NamespaceId, SubspaceId>(
  set: FragmentSet<NamespaceId, SubspaceId>,
): set is FragmentsSelective<NamespaceId, SubspaceId> {
  if (Array.isArray(set)) {
    return false;
  }

  return true;
}

export function isFragmentTriple<NamespaceId, SubspaceId>(
  fragment: Fragment<NamespaceId, SubspaceId>,
): fragment is FragmentTriple<NamespaceId, SubspaceId> {
  if (fragment.length === 2) {
    return false;
  }

  return true;
}
