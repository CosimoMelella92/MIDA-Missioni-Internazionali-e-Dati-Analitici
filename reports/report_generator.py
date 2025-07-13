import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from pathlib import Path
import yaml
import pdfkit
from jinja2 import Environment, FileSystemLoader
import logging
import os

class ReportGenerator:
    def __init__(self):
        """Inizializza il generatore di report"""
        with open('config/config.yaml', 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.setup_logging()
        self.template_env = Environment(
            loader=FileSystemLoader('scripts/reports/templates')
        )
        
    def setup_logging(self):
        """Configura il sistema di logging"""
        log_dir = Path(self.config['percorsi']['logs'])
        log_dir.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_dir / f'report_{datetime.now().strftime("%Y%m%d")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def carica_dati(self):
        """Carica i dati più recenti"""
        data_dir = Path(self.config['percorsi']['final_data'])
        files = list(data_dir.glob('missioni_internazionali_*.csv'))
        if not files:
            self.logger.error("Nessun file dati trovato")
            return pd.DataFrame()
        
        latest_file = max(files, key=os.path.getctime)
        return pd.read_csv(latest_file)
    
    def genera_grafici(self, df):
        """Genera i grafici per il report"""
        grafici = {}
        
        # Distribuzione per tipo missione
        fig_tipo = px.pie(
            df,
            names='tipo_missione',
            title='Distribuzione per Tipo Missione'
        )
        grafici['tipo_missione'] = fig_tipo.to_html(full_html=False)
        
        # Distribuzione per paese
        fig_paese = px.bar(
            df['paese'].value_counts().reset_index(),
            x='index',
            y='paese',
            title='Numero di Missioni per Paese'
        )
        grafici['paese'] = fig_paese.to_html(full_html=False)
        
        # Timeline
        fig_timeline = px.timeline(
            df.sort_values('data_inizio'),
            x_start='data_inizio',
            x_end='data_fine',
            y='nome_missione',
            color='tipo_missione',
            title='Timeline delle Missioni'
        )
        grafici['timeline'] = fig_timeline.to_html(full_html=False)
        
        return grafici
    
    def calcola_statistiche(self, df):
        """Calcola le statistiche per il report"""
        return {
            'totale_missioni': len(df),
            'missioni_attive': len(df[df['data_fine'].isna()]),
            'totale_personale': df['personale_totale'].sum(),
            'costo_totale': df['costo_totale'].sum(),
            'media_personale': df['personale_totale'].mean(),
            'media_costo': df['costo_totale'].mean(),
            'tipi_missione': df['tipo_missione'].value_counts().to_dict(),
            'paesi': df['paese'].value_counts().to_dict()
        }
    
    def genera_report_html(self, df, grafici, statistiche):
        """Genera il report in formato HTML"""
        template = self.template_env.get_template('report_template.html')
        
        return template.render(
            data_aggiornamento=datetime.now().strftime('%d/%m/%Y'),
            statistiche=statistiche,
            grafici=grafici,
            missioni=df.to_dict('records')
        )
    
    def genera_report_pdf(self, html_content):
        """Converte il report HTML in PDF"""
        report_dir = Path('reports')
        report_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        pdf_path = report_dir / f'report_missioni_{timestamp}.pdf'
        
        try:
            pdfkit.from_string(html_content, str(pdf_path))
            self.logger.info(f"Report PDF generato: {pdf_path}")
            return pdf_path
        except Exception as e:
            self.logger.error(f"Errore nella generazione del PDF: {str(e)}")
            return None
    
    def genera_report(self):
        """Genera il report completo"""
        try:
            # Carica dati
            df = self.carica_dati()
            if df.empty:
                self.logger.error("Impossibile generare il report: nessun dato disponibile")
                return None
            
            # Genera grafici
            grafici = self.genera_grafici(df)
            
            # Calcola statistiche
            statistiche = self.calcola_statistiche(df)
            
            # Genera HTML
            html_content = self.genera_report_html(df, grafici, statistiche)
            
            # Genera PDF
            pdf_path = self.genera_report_pdf(html_content)
            
            return pdf_path
            
        except Exception as e:
            self.logger.error(f"Errore nella generazione del report: {str(e)}")
            return None

if __name__ == "__main__":
    generator = ReportGenerator()
    generator.genera_report() 