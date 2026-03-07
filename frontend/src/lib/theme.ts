// Centralized color theme — single source of truth
export const T = {
  navy: '#1B3A5C',
  navyLight: '#2C5F8A',
  navyDark: '#15304D',
  olive: '#4A5D23',
  oliveDark: '#3D4F1E',
  oliveLight: '#6B8C2A',
  sand: '#F5F3EE',
  sandDark: '#EAE6DC',
  sandDeep: '#D4CFC3',
  red: '#8B1A1A',
  steel: '#5A5F63',
  muted: '#8B9298',
  khaki: '#7D6B3A',
  black: '#1A1A1A',
  white: '#FAFAF8',
} as const

export type ThemeColor = keyof typeof T
