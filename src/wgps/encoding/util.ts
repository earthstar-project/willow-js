const compactWidthEndMasks: Record<1 | 2 | 4 | 8, number> = {
  1: 0x0,
  2: 0x1,
  4: 0x2,
  8: 0x3,
};

export function compactWidthOr(
  byte: number,
  compactWidth: 1 | 2 | 4 | 8,
): number {
  return byte | compactWidthEndMasks[compactWidth];
}
