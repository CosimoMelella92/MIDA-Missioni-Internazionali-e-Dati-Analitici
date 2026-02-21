export interface Mission {
  nome: string
  paese: string
  regione: string
  tipo_missione: string
  commitment: string
  data_inizio: string
  data_fine: string | null
  personale_militare: number
  personale_civile: number
  personale_totale: number
  costo_totale: number
  is_active: boolean
  tipo_partecipazione: string
  sub_regione: string
  fonte: string
}

export interface Stats {
  total: number
  active: number
  personnel: number
  countries: number
  organizations: number
  regions: number
  by_org: Record<string, number>
  by_region: Record<string, number>
  by_decade: Record<string, number>
}

export type OrgType = 'ONU' | 'NATO' | 'UE' | 'ITA' | 'Bilateral' | 'Multinational' | 'Coalizione'
