import { EncodingScheme, Path } from "../../../deps.ts";

export type FragmentTriple<NamespaceId, SubspaceId> = [
  NamespaceId,
  SubspaceId,
  Path,
];
export type FragmentPair<NamespaceId> = [NamespaceId, Path];

export type Fragment<NamespaceId, SubspaceId> =
  | FragmentTriple<NamespaceId, SubspaceId>
  | FragmentPair<NamespaceId>;

export type FragmentsComplete<NamespaceId> = FragmentPair<
  NamespaceId
>[];

export type FragmentsSelective<NamespaceId, SubspaceId> = {
  primary: FragmentTriple<NamespaceId, SubspaceId>[];
  secondary: FragmentPair<NamespaceId>[];
};

export type FragmentSet<NamespaceId, SubspaceId> =
  | FragmentsComplete<NamespaceId>
  | FragmentsSelective<NamespaceId, SubspaceId>;

export type FragmentKitComplete<NamespaceId> = {
  grantedNamespace: NamespaceId;
  grantedPath: Path;
};

export type FragmentKitSelective<NamespaceId, SubspaceId> = {
  grantedNamespace: NamespaceId;
  grantedSubspace: SubspaceId;
  grantedPath: Path;
};

export type FragmentKit<NamespaceId, SubspaceId> =
  | FragmentKitComplete<NamespaceId>
  | FragmentKitSelective<NamespaceId, SubspaceId>;

export type PaiScheme<
  ReadCapability,
  PsiGroup,
  PsiScalar,
  NamespaceId,
  SubspaceId,
> = {
  fragmentToGroup: (
    fragment: Fragment<NamespaceId, SubspaceId>,
  ) => Promise<PsiGroup>;
  getScalar: () => PsiScalar;
  scalarMult: (group: PsiGroup, scalar: PsiScalar) => PsiGroup;
  isGroupEqual: (a: PsiGroup, b: PsiGroup) => boolean;
  getFragmentKit: (cap: ReadCapability) => FragmentKit<NamespaceId, SubspaceId>;
  groupMemberEncoding: EncodingScheme<PsiGroup>;
};

export type Intersection<PsiGroup> = {
  group: PsiGroup;
  isComplete: boolean;
  isSecondary: boolean;
};
