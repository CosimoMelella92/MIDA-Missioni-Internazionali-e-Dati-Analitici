#!/usr/bin/env python3
"""
Script per generare report PDF delle missioni internazionali
"""
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import os
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
import io
import base64

def create_pdf_report(df, output_path="report_missioni.pdf"):
    """Crea un report PDF completo delle missioni"""
    
    # Crea il documento PDF
    doc = SimpleDocTemplate(output_path, pagesize=A4)
    story = []
    styles = getSampleStyleSheet()
    
    # Titolo principale
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        spaceAfter=30,
        alignment=1  # Centrato
    )
    
    story.append(Paragraph("🌍 MIDA - Report Missioni Internazionali", title_style))
    story.append(Spacer(1, 20))
    
    # Informazioni generali
    story.append(Paragraph(f"<b>Data Report:</b> {datetime.now().strftime('%d/%m/%Y %H:%M')}", styles['Normal']))
    story.append(Paragraph(f"<b>Numero Missioni Analizzate:</b> {len(df)}", styles['Normal']))
    story.append(Paragraph(f"<b>Periodo:</b> {df['data_inizio'].min().strftime('%Y')} - {df['data_fine'].max().strftime('%Y')}", styles['Normal']))
    story.append(Spacer(1, 20))
    
    # Statistiche principali
    story.append(Paragraph("<b>📊 Statistiche Principali</b>", styles['Heading2']))
    story.append(Spacer(1, 12))
    
    stats_data = [
        ['Metrica', 'Valore'],
        ['Missioni Totali', str(len(df))],
        ['Personale Totale', f"{df['personale_totale'].sum():,.0f}"],
        ['Costo Totale', f"€{df['costo_totale'].sum():,.0f}"],
        ['Missioni Attive', str(len(df[df['data_fine'] > pd.Timestamp.now()]))],
        ['Organizzazioni', str(len(df['tipo_missione'].unique()))],
        ['Regioni', str(len(df['regione'].unique()))]
    ]
    
    stats_table = Table(stats_data)
    stats_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 14),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    
    story.append(stats_table)
    story.append(Spacer(1, 20))
    
    # Analisi per organizzazione
    story.append(Paragraph("<b>🏛️ Analisi per Organizzazione</b>", styles['Heading2']))
    story.append(Spacer(1, 12))
    
    org_stats = df.groupby('tipo_missione').agg({
        'nome': 'count',
        'personale_totale': 'sum',
        'costo_totale': 'sum'
    }).reset_index()
    
    org_data = [['Organizzazione', 'Missioni', 'Personale', 'Costo (€)']]
    for _, row in org_stats.iterrows():
        org_data.append([
            row['tipo_missione'],
            str(row['nome']),
            f"{row['personale_totale']:,.0f}",
            f"{row['costo_totale']:,.0f}"
        ])
    
    org_table = Table(org_data)
    org_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    
    story.append(org_table)
    story.append(Spacer(1, 20))
    
    # Top 10 missioni per personale
    story.append(Paragraph("<b>👥 Top 10 Missioni per Personale</b>", styles['Heading2']))
    story.append(Spacer(1, 12))
    
    top_missions = df.nlargest(10, 'personale_totale')[['nome', 'paese', 'tipo_missione', 'personale_totale']]
    
    top_data = [['Missione', 'Paese', 'Organizzazione', 'Personale']]
    for _, row in top_missions.iterrows():
        top_data.append([
            row['nome'],
            row['paese'],
            row['tipo_missione'],
            f"{row['personale_totale']:,.0f}"
        ])
    
    top_table = Table(top_data)
    top_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    
    story.append(top_table)
    story.append(Spacer(1, 20))
    
    # Footer
    story.append(Paragraph("<i>Report generato automaticamente da MIDA - Missioni Internazionali e Dati Analitici</i>", styles['Italic']))
    
    # Genera il PDF
    doc.build(story)
    return output_path

def main():
    """Funzione principale per generare il report"""
    print("=== Generazione Report PDF ===")
    
    # Carica i dati
    try:
        df = pd.read_csv('data/processed/missioni_complete.csv')
        print(f"📁 Caricati {len(df)} missioni")
    except FileNotFoundError:
        print("❌ File missioni_complete.csv non trovato")
        return
    
    # Converti le date
    df['data_inizio'] = pd.to_datetime(df['data_inizio'], errors='coerce')
    df['data_fine'] = pd.to_datetime(df['data_fine'], errors='coerce')
    
    # Genera il report
    output_file = f"report_missioni_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    report_path = create_pdf_report(df, output_file)
    
    print(f"✅ Report PDF generato: {report_path}")
    print(f"📄 File salvato in: {os.path.abspath(report_path)}")

if __name__ == '__main__':
    main() 