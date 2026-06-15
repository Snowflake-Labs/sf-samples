export const FPS = 15;
export const TOTAL_MS = 44_000;
export const TOTAL_FRAMES = Math.round((TOTAL_MS * FPS) / 1000);

export type BeatId = 'b0' | 'b1' | 'b2' | 'b3' | 'b4' | 'b5' | 'b6' | 'b7';

// Beats are act-aligned with the time bands t1, t2, t3 in the storyboard.
// Total ~44s (2x slower than original).
export const BEATS: { id: BeatId; startMs: number; endMs: number; seed: number }[] = [
  { id: 'b0', startMs: 0,      endMs: 4_000,  seed: 1001 }, // intro / setup
  { id: 'b1', startMs: 4_000,  endMs: 9_000,  seed: 1002 }, // t1: R1 → C1 (new)
  { id: 'b2', startMs: 9_000,  endMs: 16_000, seed: 1003 }, // t2a: R2 → C1 (R10 DET)
  { id: 'b3', startMs: 16_000, endMs: 21_000, seed: 1004 }, // t2b: R3 → C2 (new)
  { id: 'b4', startMs: 21_000, endMs: 27_000, seed: 1005 }, // t3a: R4 → C1 (R16 ML 0.90)
  { id: 'b5', startMs: 27_000, endMs: 35_000, seed: 1006 }, // t3b: R5 → C2 (R16 ML 0.80 → R17 LLM → HITL)
  { id: 'b6', startMs: 35_000, endMs: 39_000, seed: 1007 }, // t3c: R6 → C3 (new)
  { id: 'b7', startMs: 39_000, endMs: 44_000, seed: 1008 }, // hold final state
];

export function frameToMs(i: number): number {
  return (i / FPS) * 1000;
}

export function msToBeat(ms: number) {
  for (const b of BEATS) {
    if (ms >= b.startMs && ms < b.endMs) {
      return { ...b, t: (ms - b.startMs) / (b.endMs - b.startMs) };
    }
  }
  const last = BEATS[BEATS.length - 1];
  return { ...last, t: 1 };
}
