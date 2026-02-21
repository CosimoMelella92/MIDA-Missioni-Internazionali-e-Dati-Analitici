import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return n.toLocaleString('it-IT')
}

export function formatCurrency(n: number): string {
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B €`
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(0)}M €`
  if (n >= 1_000) return `${(n / 1_000).toFixed(0)}K €`
  return `${n.toLocaleString('it-IT')} €`
}

export function yearFromDate(d: string | null): number | null {
  if (!d || d === 'NaT') return null
  const year = new Date(d).getFullYear()
  return isNaN(year) ? null : year
}
