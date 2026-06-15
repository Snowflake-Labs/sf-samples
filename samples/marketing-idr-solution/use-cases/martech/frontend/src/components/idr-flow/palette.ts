export type Palette = {
  text: string;
  textMuted: string;
  surface: string;
  panel: string;

  pillBase: string;
  pillBaseFill: string;

  det: string;
  fuzzy: string;
  canon: string;
  ml: string;
  llm: string;
  hitl: string;

  clusterMergedStroke: string;
  clusterMergedFill: string;
  clusterSeparateStroke: string;
  clusterSeparateFill: string;

  memberStroke: string;
  memberFill: string;
  edgePositive: string;
  edgeNegative: string;
  labelBg: string;
};

export const LIGHT: Palette = {
  text: '#0f172a',
  textMuted: '#475569',
  surface: '#ffffff',
  panel: '#fafaf7',

  pillBase: '#94a3b8',
  pillBaseFill: 'rgba(148,163,184,0.10)',

  det: '#1f6feb',
  fuzzy: '#16a34a',
  canon: '#9333ea',
  ml: '#c98a08',
  llm: '#0d9488',
  hitl: '#0d9488',

  clusterMergedStroke: '#6b7280',
  clusterMergedFill: 'rgba(107,114,128,0.08)',
  clusterSeparateStroke: '#6b7280',
  clusterSeparateFill: 'rgba(107,114,128,0.08)',

  memberStroke: '#1f2937',
  memberFill: '#ffffff',
  edgePositive: '#1f6feb',
  edgeNegative: '#94a3b8',
  labelBg: '#ffffff',
};
