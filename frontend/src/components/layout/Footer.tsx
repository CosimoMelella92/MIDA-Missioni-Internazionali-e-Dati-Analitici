export default function Footer() {
  return (
    <footer className="border-t-2 border-mil-olive bg-mil-olive-dark py-4 mt-auto">
      <div className="max-w-7xl mx-auto px-4 flex flex-col sm:flex-row items-center justify-between gap-2 text-[11px] text-mil-sand-deep uppercase tracking-wider">
        <p>MIDA — Missioni Internazionali e Dati Analitici</p>
        <p>Università di Catania · <a href="https://www.difesa.it/operazionimilitari/" className="text-mil-sand-dark hover:text-white underline" target="_blank" rel="noopener">Ministero della Difesa</a></p>
      </div>
    </footer>
  )
}
