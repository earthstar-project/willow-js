export type LiftingMonoid<ValueType, LiftedType> = {
  lift: (i: ValueType) => LiftedType;
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
    lift: (i) => {
      return [a.lift(i), b.lift(i)];
    },
    combine: (ia, ib) => {
      const fst = a.combine(ia[0], ib[0]);
      const snd = b.combine(ia[1], ib[1]);

      return [fst, snd] as [AL, BL];
    },
    neutral: [a.neutral, b.neutral],
  };
}

/** A monoid which lifts the member as a string, and combines by concatenating together. */
export const concatMonoid: LiftingMonoid<string, string> = {
  lift: (a: string) => a,
  combine: (a: string, b: string) => {
    return a + b;
  },
  neutral: "",
};

/** A monoid which lifts the member as 1, and combines by adding together. */
export const sizeMonoid: LiftingMonoid<unknown, number> = {
  lift: (_a: unknown) => 1,
  combine: (a: number, b: number) => a + b,
  neutral: 0,
};
