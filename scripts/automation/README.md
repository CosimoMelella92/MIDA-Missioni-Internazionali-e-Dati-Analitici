# Sistema di Automazione MIDA

Questo modulo implementa un sistema di automazione per le attività MIDA, inclusi scraping dei dati, generazione di report, backup e pulizia dei dati.

## Componenti

- `scheduler.py`: Lo scheduler principale che gestisce tutte le attività automatizzate
- `install_service.py`: Script per installare lo scheduler come servizio Windows
- `config/scheduler_config.yaml`: File di configurazione per lo scheduler

## Requisiti

```bash
pip install schedule pywin32
```

## Configurazione

1. Modifica il file `config/scheduler_config.yaml` con le tue impostazioni:
   - Configura le credenziali email per le notifiche
   - Imposta i percorsi delle directory
   - Personalizza gli orari di esecuzione delle attività

2. Assicurati che tutte le directory necessarie esistano:
   - `logs/`
   - `backups/`
   - `reports/`
   - `data/`
   - `config/`

## Installazione come Servizio Windows

1. Apri PowerShell come amministratore
2. Naviga alla directory del progetto
3. Esegui i seguenti comandi:

```powershell
# Installa il servizio
python scripts/automation/install_service.py install

# Avvia il servizio
python scripts/automation/install_service.py start
```

## Comandi del Servizio

```powershell
# Avvia il servizio
python scripts/automation/install_service.py start

# Ferma il servizio
python scripts/automation/install_service.py stop

# Riavvia il servizio
python scripts/automation/install_service.py restart

# Rimuovi il servizio
python scripts/automation/install_service.py remove
```

## Attività Programmate

Lo scheduler esegue le seguenti attività:

1. **Scraping dei dati**
   - Eseguito ogni giorno alle 2:00
   - Aggiorna i dati delle missioni

2. **Generazione report**
   - Eseguito dopo lo scraping
   - Genera report HTML e PDF

3. **Backup dei dati**
   - Eseguito ogni domenica alle 3:00
   - Crea backup di dati, log, report e configurazione

4. **Pulizia dei dati**
   - Eseguito ogni lunedì alle 4:00
   - Rimuove log più vecchi di 30 giorni
   - Rimuove backup più vecchi di 7 giorni

## Notifiche

Il sistema invia notifiche email per:
- Completamento delle attività
- Errori durante l'esecuzione
- Generazione di nuovi report

## Log

I log sono salvati in:
- `logs/scheduler_YYYYMMDD.log`: Log dello scheduler
- `logs/service.log`: Log del servizio Windows

## Risoluzione dei Problemi

1. **Il servizio non si avvia**
   - Verifica i log in `logs/service.log`
   - Controlla che tutte le directory esistano
   - Verifica le credenziali email

2. **Le attività non vengono eseguite**
   - Controlla i log dello scheduler
   - Verifica la configurazione in `scheduler_config.yaml`
   - Assicurati che il servizio sia in esecuzione

3. **Errori di backup**
   - Verifica lo spazio su disco
   - Controlla i permessi delle directory
   - Verifica la configurazione dei percorsi 