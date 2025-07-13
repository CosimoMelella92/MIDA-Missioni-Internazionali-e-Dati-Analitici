import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import sys
import os
import logging
from pathlib import Path

class MidaService(win32serviceutil.ServiceFramework):
    _svc_name_ = "MidaScheduler"
    _svc_display_name_ = "MIDA Scheduler Service"
    _svc_description_ = "Servizio per l'automazione delle attività MIDA"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)
        self.is_alive = True

    def SvcStop(self):
        """Ferma il servizio"""
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)
        self.is_alive = False

    def SvcDoRun(self):
        """Esegue il servizio"""
        try:
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, '')
            )
            
            # Configura il logging
            log_dir = Path('logs')
            log_dir.mkdir(parents=True, exist_ok=True)
            
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(log_dir / 'service.log'),
                    logging.StreamHandler()
                ]
            )
            self.logger = logging.getLogger(__name__)
            
            # Avvia lo scheduler
            from scheduler import AutomationScheduler
            scheduler = AutomationScheduler()
            scheduler.start()
            
        except Exception as e:
            self.logger.error(f"Errore nel servizio: {str(e)}")
            servicemanager.LogErrorMsg(f"Errore nel servizio: {str(e)}")

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(MidaService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(MidaService) 