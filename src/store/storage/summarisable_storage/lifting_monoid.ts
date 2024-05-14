/**
 * This interface combines two bits of functionality:
 *
 * 1. Lifting values of some type `BaseType` into the universe of a monoid (`LiftedType`).
 * 2. Information about the moniod of universe `LiftedType` (the neutral element and the combine function).
 */
export type LiftingMonoid<BaseType, LiftedType> = {
  lift: (base: BaseType) => Promise<LiftedType>;
  combine: (
    a: LiftedType,
    b: LiftedType,
  ) => LiftedType;
  neutral: LiftedType;
};

/** Combine two lifting monoids into a new one. */
export function combineMonoid<V, AL, BL>(
  a: LiftingMonoid<V, AL>,
  b: LiftingMonoid<V, BL>,
): LiftingMonoid<V, [AL, BL]> {
  return {
    lift: async (base) => {
      return [await a.lift(base), await b.lift(base)];
    },
    combine: (ia, ib) => {
      const fst = a.combine(ia[0], ib[0]);
      const snd = b.combine(ia[1], ib[1]);

      return [fst, snd] as [AL, BL];
    },
    neutral: [a.neutral, b.neutral],
  };
}

/** A monoid which lifts the member as 1, and combines by adding together. */
export const sizeMonoid: LiftingMonoid<unknown, number> = {
  lift: (_a: unknown) => Promise.resolve(1),
  combine: (a: number, b: number) => a + b,
  neutral: 0,
};
