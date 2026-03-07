import { lazy, Suspense } from 'react'
import { Routes, Route } from 'react-router-dom'
import Navbar from './components/layout/Navbar'
import Footer from './components/layout/Footer'
import CommandPalette from './components/ui/CommandPalette'

const HomePage = lazy(() => import('./pages/HomePage'))
const DashboardPage = lazy(() => import('./pages/DashboardPage'))
const MissionsPage = lazy(() => import('./pages/MissionsPage'))
const TimelinePage = lazy(() => import('./pages/TimelinePage'))
const MapPage = lazy(() => import('./pages/MapPage'))
const AboutPage = lazy(() => import('./pages/AboutPage'))

function PageLoader() {
  return (
    <div className="flex items-center justify-center h-96 bg-[#F5F3EE]">
      <div className="text-center">
        <div className="animate-spin rounded-full h-8 w-8 border-2 border-[#4A5D23] border-t-transparent mx-auto" />
        <p className="mt-3 text-[10px] text-[#8B9298] uppercase tracking-[0.15em]">Caricamento...</p>
      </div>
    </div>
  )
}

export default function App() {
  return (
    <div className="flex flex-col min-h-screen">
      <Navbar />
      <CommandPalette />
      <main className="flex-1">
        <Suspense fallback={<PageLoader />}>
          <Routes>
            <Route path="/" element={<HomePage />} />
            <Route path="/dashboard" element={<DashboardPage />} />
            <Route path="/missions" element={<MissionsPage />} />
            <Route path="/timeline" element={<TimelinePage />} />
            <Route path="/map" element={<MapPage />} />
            <Route path="/about" element={<AboutPage />} />
          </Routes>
        </Suspense>
      </main>
      <Footer />
    </div>
  )
}
