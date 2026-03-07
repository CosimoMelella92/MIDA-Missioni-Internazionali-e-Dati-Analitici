import { motion } from 'framer-motion'

export default function AboutPage() {
  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ duration: 0.2 }} className="max-w-3xl mx-auto px-4 py-6 md:py-10 space-y-8">
      <div>
        <h1 className="text-[14px] font-bold uppercase tracking-[0.12em] text-[#1B3A5C] border-b border-[#D4CFC3] pb-2">
          Informazioni
        </h1>
      </div>

      <section className="space-y-3">
        <h2 className="text-[13px] font-bold uppercase tracking-[0.1em] text-[#1B3A5C]">Il Progetto</h2>
        <p className="text-[12px] text-[#5A5F63] leading-relaxed">
          <b>MIDA — Missioni Internazionali e Dati Analitici</b> è una piattaforma di analisi
          delle missioni internazionali italiane dal 1948 al 2026. Aggrega dati da 5 fonti ufficiali,
          producendo un dataset unificato di 234 missioni dopo deduplicazione automatica, correzione
          dati e validazione.
        </p>
      </section>

      <section className="space-y-3">
        <h2 className="text-[13px] font-bold uppercase tracking-[0.1em] text-[#1B3A5C]">Metodologia</h2>
        <div className="bg-white border border-[#D4CFC3] rounded p-4 space-y-2">
          {[
            ['1. Acquisizione', '5 fonti (CSV + Excel) con 384 righe raw'],
            ['2. Normalizzazione', 'Nomi missione, organizzazioni (ONU/NATO/UE), regioni, commitment'],
            ['3. Deduplicazione', 'Chiave = nome normalizzato strict, vince la fonte con più dati'],
            ['4. Arricchimento', 'Correzioni ufficiali difesa.it, cross-reference 40 missioni attive 2026'],
            ['5. Validazione', 'Ogni record passa per il modello Pydantic Mission — 0 errori'],
            ['6. Output', '234 missioni, 18 colonne canoniche, 188 test automatici'],
          ].map(([step, desc]) => (
            <div key={step} className="flex gap-3">
              <span className="text-[10px] font-bold text-[#4A5D23] uppercase tracking-[0.1em] w-32 flex-shrink-0">{step}</span>
              <span className="text-[11px] text-[#5A5F63]">{desc}</span>
            </div>
          ))}
        </div>
      </section>

      <section className="space-y-3">
        <h2 className="text-[13px] font-bold uppercase tracking-[0.1em] text-[#1B3A5C]">Fonti Dati</h2>
        <div className="bg-white border border-[#D4CFC3] rounded overflow-hidden">
          <table className="w-full text-[11px]">
            <thead>
              <tr className="bg-[#1B3A5C] text-white">
                <th className="px-3 py-2 text-left text-[9px] uppercase tracking-[0.1em]">Fonte</th>
                <th className="px-3 py-2 text-left text-[9px] uppercase tracking-[0.1em]">Tipo</th>
              </tr>
            </thead>
            <tbody>
              {[
                ['difesa.it', 'Scraper + manuale'],
                ['analisidifesa.it', 'Manuale'],
                ['Camera dei Deputati', 'Scraper'],
                ['Senato della Repubblica', 'Scraper'],
                ['EEAS (UE) / NATO / ONU', 'Scraper'],
              ].map(([fonte, tipo], i) => (
                <tr key={fonte} className={`border-b border-[#EAE6DC] ${i % 2 ? 'bg-[#F5F3EE]' : ''}`}>
                  <td className="px-3 py-1.5 font-medium text-[#1B3A5C]">{fonte}</td>
                  <td className="px-3 py-1.5 text-[#5A5F63]">{tipo}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="space-y-3">
        <h2 className="text-[13px] font-bold uppercase tracking-[0.1em] text-[#1B3A5C]">Stack Tecnologico</h2>
        <div className="grid grid-cols-2 gap-3">
          {[
            ['Frontend', 'React 18, TypeScript, Vite 5, Tailwind CSS, Recharts, Leaflet'],
            ['Backend', 'Python 3.11+, Pandas, Pydantic, pytest (188 test)'],
            ['Dashboard', 'Streamlit, Plotly, Folium'],
            ['CI/CD', 'GitHub Actions (lint + test + scraping settimanale)'],
          ].map(([cat, tech]) => (
            <div key={cat} className="border border-[#D4CFC3] rounded p-3">
              <p className="text-[9px] font-bold uppercase tracking-[0.1em] text-[#8B9298]">{cat}</p>
              <p className="text-[11px] text-[#1B3A5C] mt-1">{tech}</p>
            </div>
          ))}
        </div>
      </section>

      <section className="space-y-3">
        <h2 className="text-[13px] font-bold uppercase tracking-[0.1em] text-[#1B3A5C]">Crediti</h2>
        <div className="text-[12px] text-[#5A5F63] leading-relaxed space-y-1">
          <p><b>Autore:</b> Cosimo Melella — Università di Catania</p>
          <p><b>Dati:</b> Ministero della Difesa — <a href="https://www.difesa.it/operazionimilitari/" target="_blank" rel="noopener" className="text-[#1B3A5C] underline">difesa.it</a></p>
          <p><b>Codice sorgente:</b> <a href="https://github.com/CosimoMelella92/MIDA-Missioni-Internazionali-e-Dati-Analitici" target="_blank" rel="noopener" className="text-[#1B3A5C] underline">GitHub</a></p>
          <p><b>Licenza:</b> MIT</p>
        </div>
      </section>

      <div className="border-t border-[#D4CFC3] pt-4">
        <p className="text-[9px] text-[#8B9298] uppercase tracking-[0.15em] text-center">
          MIDA — Missioni Internazionali e Dati Analitici · Università di Catania
        </p>
      </div>
    </motion.div>
  )
}
