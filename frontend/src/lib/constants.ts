// Palette militare italiana
export const MILITARY = {
  olive: '#4A5D23',
  oliveDark: '#3D4F1E',
  oliveLight: '#6B8C2A',
  navy: '#1B3A5C',
  navyLight: '#2C5F8A',
  sand: '#F5F3EE',
  sandDark: '#EAE6DC',
  sandDeep: '#D4CFC3',
  red: '#8B1A1A',
  steel: '#5A5F63',
  steelLight: '#8B9298',
  khaki: '#7D6B3A',
  black: '#1A1A1A',
  white: '#FAFAF8',
}

export const ORG_COLORS: Record<string, string> = {
  ONU: '#1B3A5C',
  NATO: '#4A5D23',
  UE: '#2C5F8A',
  ITA: '#8B1A1A',
  Bilateral: '#7D6B3A',
  Multinational: '#5A5F63',
  Coalizione: '#6B8C2A',
  Altro: '#8B9298',
}

export const REGION_COLORS: Record<string, string> = {
  Europa: '#1B3A5C',
  'Medio Oriente': '#8B1A1A',
  Africa: '#7D6B3A',
  Asia: '#4A5D23',
  America: '#2C5F8A',
  'Non specificata': '#8B9298',
}

export const GEOCODING: Record<string, [number, number]> = {
  'Roma': [41.9028, 12.4964],
  'Libano': [33.8547, 35.8623],
  'Kosovo': [42.6026, 20.9020],
  'Afghanistan': [34.5553, 69.2075],
  'Iraq': [33.3152, 44.3661],
  'Somalia': [2.0469, 45.3182],
  'Bosnia ed Erzegovina': [43.8563, 18.4131],
  'Mediterraneo': [35.5, 18.0],
  'Egitto': [26.8206, 30.8025],
  'Cipro': [35.1264, 33.4299],
  'India/Pakistan': [33.7294, 73.0931],
  'Sahara Occidentale': [24.2155, -12.8858],
  'Niger': [13.5117, 2.1254],
  'Libia': [32.8872, 13.1913],
  'Palestina': [31.9522, 35.2332],
  'Malta': [35.9375, 14.3754],
  'Gibuti': [11.5721, 43.1456],
  'Lettonia': [56.9496, 24.1052],
  'Estonia': [59.4370, 24.7536],
  'Bulgaria': [42.6977, 23.3219],
  'Ungheria': [47.4979, 19.0402],
  'Mar Rosso': [20.0, 38.0],
  'Stretto di Hormuz': [26.5, 56.5],
  'Oceano Indiano': [-5.0, 60.0],
  'Armenia': [40.1792, 44.4991],
  'Mozambico': [-25.9692, 32.5732],
  'Serbia': [44.7866, 20.4489],
  'Israele': [31.7683, 35.2137],
}

export const ROMA: [number, number] = [41.9028, 12.4964]

export const HISTORICAL_EVENTS: { year: number; label: string }[] = [
  { year: 1948, label: 'Nascita ONU Peacekeeping' },
  { year: 1949, label: 'Fondazione NATO' },
  { year: 1956, label: 'Crisi di Suez' },
  { year: 1982, label: 'Libano (MNF)' },
  { year: 1991, label: 'Guerra del Golfo' },
  { year: 1992, label: 'Somalia (UNOSOM)' },
  { year: 1995, label: 'Bosnia (IFOR)' },
  { year: 1999, label: 'Kosovo (KFOR)' },
  { year: 2001, label: '11 Settembre' },
  { year: 2003, label: 'Iraq (Antica Babilonia)' },
  { year: 2011, label: 'Primavera Araba' },
  { year: 2014, label: 'ISIS / Inherent Resolve' },
  { year: 2022, label: 'Ucraina / NATO eFP' },
  { year: 2024, label: 'Mar Rosso (Aspides)' },
]

export const COUNTRY_FLAGS: Record<string, string> = {
  'Libano': '🇱🇧', 'Kosovo': '🇽🇰', 'Iraq': '🇮🇶', 'Somalia': '🇸🇴',
  'Bosnia ed Erzegovina': '🇧🇦', 'Egitto': '🇪🇬', 'Cipro': '🇨🇾',
  'Niger': '🇳🇪', 'Libia': '🇱🇾', 'Palestina': '🇵🇸', 'Malta': '🇲🇹',
  'Gibuti': '🇩🇯', 'Lettonia': '🇱🇻', 'Estonia': '🇪🇪', 'Bulgaria': '🇧🇬',
  'Ungheria': '🇭🇺', 'Armenia': '🇦🇲', 'Mozambico': '🇲🇿', 'Serbia': '🇷🇸',
  'Israele': '🇮🇱', 'India/Pakistan': '🇮🇳', 'Sahara Occidentale': '🇲🇦',
  'Mediterraneo': '🌊', 'Mar Rosso': '🌊', 'Oceano Indiano': '🌊',
  'Stretto di Hormuz': '🌊', 'Afghanistan': '🇦🇫', 'Siria': '🇸🇾',
  'Mali': '🇲🇱', 'Ucraina': '🇺🇦', 'Romania': '🇷🇴', 'Polonia': '🇵🇱',
}
