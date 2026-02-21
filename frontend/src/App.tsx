import { Routes, Route } from 'react-router-dom'
import Navbar from './components/layout/Navbar'
import Footer from './components/layout/Footer'
import HomePage from './pages/HomePage'
import DashboardPage from './pages/DashboardPage'
import MissionsPage from './pages/MissionsPage'
import TimelinePage from './pages/TimelinePage'
import MapPage from './pages/MapPage'

export default function App() {
  return (
    <div className="flex flex-col min-h-screen">
      <Navbar />
      <main className="flex-1">
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/dashboard" element={<DashboardPage />} />
          <Route path="/missions" element={<MissionsPage />} />
          <Route path="/timeline" element={<TimelinePage />} />
          <Route path="/map" element={<MapPage />} />
        </Routes>
      </main>
      <Footer />
    </div>
  )
}
