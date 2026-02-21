"""
MIDA — Generatore Report PDF.
Genera un report sintetico delle missioni internazionali italiane.
Usa fpdf2 per la creazione del PDF.
"""

import io
from datetime import datetime

import pandas as pd
from fpdf import FPDF


class MIDAReport(FPDF):
    """PDF report con header/footer MIDA."""

    def header(self):
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(61, 79, 30)  # Verde oliva #3D4F1E
        self.cell(0, 10, "MIDA - Missioni Internazionali e Dati Analitici", align="C", new_x="LMARGIN", new_y="NEXT")
        self.set_font("Helvetica", "", 8)
        self.set_text_color(90, 95, 99)  # Grigio acciaio
        self.cell(0, 5, f"Report generato il {datetime.now().strftime('%d/%m/%Y %H:%M')}", align="C", new_x="LMARGIN", new_y="NEXT")
        self.ln(3)
        # Linea separatrice
        self.set_draw_color(74, 93, 35)  # #4A5D23
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(139, 146, 152)
        self.cell(0, 10, f"MIDA - Universita di Catania | Dati: difesa.it | Pag. {self.page_no()}/{{nb}}", align="C")


def generate_report(df: pd.DataFrame) -> bytes:
    """Genera un report PDF dal DataFrame delle missioni e restituisce i bytes."""
    pdf = MIDAReport(orientation="P", unit="mm", format="A4")
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # ── 1. KPI Summary ──
    pdf.set_font("Helvetica", "B", 12)
    pdf.set_text_color(27, 58, 92)  # Blu marina
    pdf.cell(0, 8, "Riepilogo", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    totali = len(df)
    attive = int(df["is_active"].sum()) if "is_active" in df.columns else 0
    personale = df["personale_totale"].sum() if "personale_totale" in df.columns else 0
    costo = df["costo_totale"].sum() if "costo_totale" in df.columns else 0
    paesi = df["paese"].nunique() if "paese" in df.columns else 0

    kpis = [
        ("Missioni totali", str(totali)),
        ("Missioni attive", str(attive)),
        ("Personale totale", f"{personale:,.0f}"),
        ("Paesi coinvolti", str(paesi)),
        ("Costo complessivo", _fmt_currency(costo)),
    ]

    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(44, 44, 44)
    for label, value in kpis:
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(50, 6, label + ":", new_x="RIGHT")
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(40, 6, value, new_x="LMARGIN", new_y="NEXT")
    pdf.ln(5)

    # ── 2. Distribuzione per Organizzazione ──
    if "tipo_missione" in df.columns:
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_text_color(27, 58, 92)
        pdf.cell(0, 8, "Distribuzione per Organizzazione", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        org_counts = df["tipo_missione"].value_counts()
        _draw_table(pdf, ["Organizzazione", "Missioni", "%"],
                    [[org, str(count), f"{count/totali*100:.1f}%"]
                     for org, count in org_counts.items()])
        pdf.ln(5)

    # ── 3. Distribuzione per Regione ──
    if "regione" in df.columns:
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_text_color(27, 58, 92)
        pdf.cell(0, 8, "Distribuzione per Regione", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        reg_counts = df["regione"].value_counts()
        _draw_table(pdf, ["Regione", "Missioni", "%"],
                    [[reg, str(count), f"{count/totali*100:.1f}%"]
                     for reg, count in reg_counts.items()])
        pdf.ln(5)

    # ── 4. Missioni Attive ──
    if "is_active" in df.columns:
        active_df = df[df["is_active"] == True].copy()
        if not active_df.empty:
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 12)
            pdf.set_text_color(27, 58, 92)
            pdf.cell(0, 8, f"Missioni Attive ({len(active_df)})", new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)

            cols = ["Nome", "Paese", "Org.", "Personale"]
            rows = []
            for _, r in active_df.sort_values("personale_totale", ascending=False).iterrows():
                rows.append([
                    str(r.get("nome", ""))[:35],
                    str(r.get("paese", ""))[:20],
                    str(r.get("tipo_missione", "")),
                    f"{r.get('personale_totale', 0):,.0f}" if pd.notna(r.get("personale_totale")) else "N/D",
                ])
            _draw_table(pdf, cols, rows, col_widths=[70, 40, 30, 30])

    # ── 5. Top 10 missioni per personale ──
    if "personale_totale" in df.columns:
        top10 = df.nlargest(10, "personale_totale")
        if not top10.empty:
            pdf.add_page()
            pdf.set_font("Helvetica", "B", 12)
            pdf.set_text_color(27, 58, 92)
            pdf.cell(0, 8, "Top 10 Missioni per Personale", new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)

            rows = []
            for _, r in top10.iterrows():
                rows.append([
                    str(r.get("nome", ""))[:35],
                    str(r.get("paese", ""))[:20],
                    f"{r.get('personale_totale', 0):,.0f}",
                    "Attiva" if r.get("is_active") else "Conclusa",
                ])
            _draw_table(pdf, ["Nome", "Paese", "Personale", "Stato"], rows,
                        col_widths=[70, 40, 35, 25])

    # Output
    return pdf.output()


def _draw_table(pdf: FPDF, headers: list, rows: list, col_widths: list = None):
    """Disegna una tabella con header colorato."""
    if col_widths is None:
        n = len(headers)
        col_widths = [190 // n] * n

    # Header
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(74, 93, 35)  # Verde oliva
    pdf.set_text_color(255, 255, 255)
    for i, h in enumerate(headers):
        pdf.cell(col_widths[i], 7, h, border=1, fill=True, align="C", new_x="RIGHT")
    pdf.ln()

    # Rows
    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(44, 44, 44)
    fill = False
    for row in rows:
        if pdf.get_y() > 265:
            pdf.add_page()
            # Re-draw header
            pdf.set_font("Helvetica", "B", 8)
            pdf.set_fill_color(74, 93, 35)
            pdf.set_text_color(255, 255, 255)
            for i, h in enumerate(headers):
                pdf.cell(col_widths[i], 7, h, border=1, fill=True, align="C", new_x="RIGHT")
            pdf.ln()
            pdf.set_font("Helvetica", "", 8)
            pdf.set_text_color(44, 44, 44)
            fill = False

        if fill:
            pdf.set_fill_color(234, 230, 220)  # Sabbia chiaro
        else:
            pdf.set_fill_color(255, 255, 255)

        for i, cell in enumerate(row):
            align = "L" if i == 0 else "C"
            pdf.cell(col_widths[i], 6, str(cell), border=1, fill=True, align=align, new_x="RIGHT")
        pdf.ln()
        fill = not fill


def _fmt_currency(value: float) -> str:
    if value >= 1e9:
        return f"{value / 1e9:.1f} Mld EUR"
    elif value >= 1e6:
        return f"{value / 1e6:.1f} Mln EUR"
    elif value >= 1e3:
        return f"{value / 1e3:.0f}K EUR"
    return f"{value:.0f} EUR"
