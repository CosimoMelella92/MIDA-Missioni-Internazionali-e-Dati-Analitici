import { useState, useEffect } from 'react'

export function useAnimatedCounter(target: number, duration = 1500): number {
  const [count, setCount] = useState(0)

  useEffect(() => {
    if (target === 0) return
    const start = performance.now()
    const step = (now: number) => {
      const progress = Math.min((now - start) / duration, 1)
      const eased = 1 - Math.pow(1 - progress, 3)
      setCount(Math.round(eased * target))
      if (progress < 1) requestAnimationFrame(step)
    }
    requestAnimationFrame(step)
  }, [target, duration])

  return count
}
