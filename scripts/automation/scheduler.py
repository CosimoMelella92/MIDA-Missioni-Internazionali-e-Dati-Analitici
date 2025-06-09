import schedule
import time
import logging
from datetime import datetime
from pathlib import Path
import yaml
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os
import shutil
import sys
import traceback

# Aggiungi la directory principale al path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from scripts.main import main as run_scraper
from scripts.reports.report_generator import ReportGenerator

class AutomationScheduler:
    def __init__(self):
        """Inizializza lo scheduler"""
        with open('config/config.yaml', 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.setup_logging()
        self.setup_directories()
        
    def setup_logging(self):
        """Configura il sistema di logging"""
        log_dir = Path(self.config['percorsi']['logs'])
        log_dir.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_dir / f'scheduler_{datetime.now().strftime("%Y%m%d")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def setup_directories(self):
        """Crea le directory necessarie"""
        for dir_path in ['backups', 'reports']:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    def run_scraping(self):
        """Esegue lo scraping dei dati"""
        try:
            self.logger.info("Avvio processo di scraping")
            run_scraper()
            self.logger.info("Processo di scraping completato")
            return True
        except Exception as e:
            self.logger.error(f"Errore durante lo scraping: {str(e)}")
            self.send_notification("Errore Scraping", str(e))
            return False
    
    def generate_report(self):
        """Genera il report"""
        try:
            self.logger.info("Generazione report")
            generator = ReportGenerator()
            report_path = generator.genera_report()
            if report_path:
                self.logger.info(f"Report generato: {report_path}")
                self.send_notification("Report Generato", f"Report disponibile in: {report_path}")
            return True
        except Exception as e:
            self.logger.error(f"Errore nella generazione del report: {str(e)}")
            self.send_notification("Errore Report", str(e))
            return False
    
    def backup_data(self):
        """Esegue il backup dei dati"""
        try:
            self.logger.info("Avvio backup dati")
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_dir = Path('backups') / timestamp
            backup_dir.mkdir(parents=True)
            
            # Backup dati
            for dir_name in ['data', 'logs', 'reports']:
                src_dir = Path(dir_name)
                if src_dir.exists():
                    dst_dir = backup_dir / dir_name
                    shutil.copytree(src_dir, dst_dir)
            
            # Backup configurazione
            shutil.copy2('config/config.yaml', backup_dir / 'config.yaml')
            
            self.logger.info(f"Backup completato: {backup_dir}")
            return True
        except Exception as e:
            self.logger.error(f"Errore durante il backup: {str(e)}")
            self.send_notification("Errore Backup", str(e))
            return False
    
    def cleanup_old_data(self):
        """Pulisce i dati vecchi"""
        try:
            self.logger.info("Pulizia dati vecchi")
            
            # Mantieni solo gli ultimi 30 giorni di log
            log_dir = Path(self.config['percorsi']['logs'])
            for log_file in log_dir.glob('*.log'):
                if (datetime.now() - datetime.fromtimestamp(log_file.stat().st_mtime)).days > 30:
                    log_file.unlink()
            
            # Mantieni solo gli ultimi 7 giorni di backup
            backup_dir = Path('backups')
            for backup in backup_dir.iterdir():
                if (datetime.now() - datetime.fromtimestamp(backup.stat().st_mtime)).days > 7:
                    shutil.rmtree(backup)
            
            self.logger.info("Pulizia dati completata")
            return True
        except Exception as e:
            self.logger.error(f"Errore durante la pulizia dei dati: {str(e)}")
            return False
    
    def send_notification(self, subject, message, attachment_path=None):
        """Invia una notifica via email"""
        try:
            if 'email' not in self.config:
                self.logger.warning("Configurazione email non trovata")
                return
            
            msg = MIMEMultipart()
            msg['From'] = self.config['email']['from']
            msg['To'] = self.config['email']['to']
            msg['Subject'] = f"MIDA - {subject}"
            
            msg.attach(MIMEText(message, 'plain'))
            
            if attachment_path and os.path.exists(attachment_path):
                with open(attachment_path, 'rb') as f:
                    part = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))
                    part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
                    msg.attach(part)
            
            with smtplib.SMTP(self.config['email']['smtp_server'], self.config['email']['smtp_port']) as server:
                server.starttls()
                server.login(self.config['email']['username'], self.config['email']['password'])
                server.send_message(msg)
            
            self.logger.info(f"Notifica inviata: {subject}")
            return True
        except Exception as e:
            self.logger.error(f"Errore nell'invio della notifica: {str(e)}")
            return False
    
    def run_scheduled_tasks(self):
        """Esegue tutte le attività programmate"""
        try:
            self.logger.info("Avvio attività programmate")
            
            # Esegui scraping
            if not self.run_scraping():
                return
            
            # Genera report
            if not self.generate_report():
                return
            
            # Esegui backup
            if not self.backup_data():
                return
            
            # Pulisci dati vecchi
            self.cleanup_old_data()
            
            self.logger.info("Attività programmate completate")
            self.send_notification(
                "Attività Completate",
                "Tutte le attività programmate sono state completate con successo."
            )
            
        except Exception as e:
            self.logger.error(f"Errore durante l'esecuzione delle attività: {str(e)}")
            self.send_notification(
                "Errore Attività",
                f"Errore durante l'esecuzione delle attività:\n{str(e)}\n\n{traceback.format_exc()}"
            )
    
    def start(self):
        """Avvia lo scheduler"""
        self.logger.info("Avvio scheduler")
        
        # Esegui attività ogni giorno alle 2:00
        schedule.every().day.at("02:00").do(self.run_scheduled_tasks)
        
        # Esegui backup ogni domenica alle 3:00
        schedule.every().sunday.at("03:00").do(self.backup_data)
        
        # Pulisci dati vecchi ogni lunedì alle 4:00
        schedule.every().monday.at("04:00").do(self.cleanup_old_data)
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)
            except Exception as e:
                self.logger.error(f"Errore nello scheduler: {str(e)}")
                time.sleep(300)  # Attendi 5 minuti in caso di errore

if __name__ == "__main__":
    scheduler = AutomationScheduler()
    scheduler.start() 