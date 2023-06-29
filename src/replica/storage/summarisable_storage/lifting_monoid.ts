export type LiftingMonoid<ValueType, LiftedType> = {
  lift: (i: ValueType) => Promise<LiftedType>;
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
    lift: async (i) => {
      return [await a.lift(i), await b.lift(i)];
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
  lift: (a: string) => Promise.resolve(a),
  combine: (a: string, b: string) => {
    return a + b;
  },
  neutral: "",
};

/** A monoid which lifts the member as 1, and combines by adding together. */
export const sizeMonoid: LiftingMonoid<unknown, number> = {
  lift: (_a: unknown) => Promise.resolve(1),
  combine: (a: number, b: number) => a + b,
  neutral: 0,
};

/** A monoid which lifts using SHA-256, and combines the resulting hash using a bitwise XOR.*/
export const sha256XorMonoid: LiftingMonoid<Uint8Array, Uint8Array> = {
  lift: async (v: Uint8Array) => {
    return new Uint8Array(await crypto.subtle.digest("SHA-256", v));
  },
  combine: (a: Uint8Array, b: Uint8Array) => {
    const xored = [];

    for (let i = 0; i < a.length; i++) {
      xored.push(a[i] ^ b[i]);
    }

    return new Uint8Array(xored);
  },
  neutral: new Uint8Array(8),
};
