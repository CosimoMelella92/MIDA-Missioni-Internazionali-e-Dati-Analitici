# MIDA - Missioni Internazionali e Dati Analitici

Sistema per l'analisi e l'elaborazione dei dati relativi alle missioni internazionali dell'Unione Europea.

## Struttura del Progetto

```
.
├── config/
│   └── config.yaml         # Configurazione del sistema
├── data/
│   ├── raw/               # Dati grezzi
│   ├── processed/         # Dati elaborati
│   └── documents/         # Documenti PDF/Word
├── src/
│   ├── data_processor.py  # Gestione dati Excel
│   ├── document_processor.py  # Elaborazione documenti
│   └── main.py           # Script principale
├── requirements.txt       # Dipendenze Python
└── README.md             # Documentazione
```

## Installazione

1. Clona il repository:
```bash
git clone [URL_REPOSITORY]
cd MIDA-Missioni-Internazionali-e-Dati-Analitici
```

2. Crea un ambiente virtuale:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

3. Installa le dipendenze:
```bash
pip install -r requirements.txt
```

## Utilizzo

1. Configura il file `config/config.yaml` con i percorsi corretti:
```yaml
configurazione:
  excel_path: "data/raw/Matrice dati 1AGG.xlsx"
  documenti: "data/documents"
  processed_data: "data/processed"
```

2. Esegui lo script principale:
```bash
python src/main.py
```

## Funzionalità

- Lettura e pulizia del file Excel principale
- Estrazione dati da documenti PDF e Word
- Arricchimento dei dati con informazioni aggiuntive
- Salvataggio dei dati elaborati

## Struttura dei Dati

Il file Excel principale contiene:
- Informazioni anagrafiche delle missioni
- Dati temporali (date di inizio/fine)
- Informazioni sulla partecipazione
- Indicatori di engagement
- Note e riferimenti

## Contribuire

1. Fork del repository
2. Crea un branch per la tua feature
3. Commit delle modifiche
4. Push al branch
5. Crea una Pull Request

## Licenza

[Inserire tipo di licenza] 