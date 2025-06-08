
# 🇮🇹 Missioni Internazionali - Data Scraper & Integrator

Questo progetto Python consente di:
1. Estrarre automaticamente dati da PDF istituzionali (es. relazioni parlamentari italiane sulle missioni internazionali).
2. Pulire, salvare e strutturare i dati in formato `.csv`.
3. Integrare questi dati in un dataset Excel già esistente (es. `Matrice dati 1AGG.xlsx`) per aggiornare colonne come `Personale` e `Costo`.

## 📦 Struttura del progetto

```
Missioni_Internazionali_Scraper/
├── scripts/
│   ├── extract_missioni_camera.py     # Estrae dati da PDF delle relazioni analitiche Camera/Senato
│   └── merge_into_excel.py            # Unisce i dati estratti con il file Excel master
├── data/
│   ├── raw/                           # PDF, HTML e dati grezzi
│   ├── processed/                     # CSV con dati puliti
│   └── final/                         # Dataset Excel finale integrato
├── config/                            # Parametri scraping e matching
├── logs/                              # Log delle operazioni eseguite
├── notebooks/                         # Analisi o ispezione manuale
└── README.md
```

---

## ▶️ Come usare

### 1. Estrazione PDF

Esegui `extract_missioni_camera.py` per ottenere un CSV con i dati strutturati.

```bash
python scripts/extract_missioni_camera.py
```

Output:
- `data/processed/extracted_missioni_camera_2024_2025.csv`

---

### 2. Merge con Excel esistente

Modifica il file `merge_into_excel.py` per inserire il path del tuo file Excel (`Matrice dati 1AGG.xlsx`) e lancia:

```bash
python scripts/merge_into_excel.py
```

Il codice effettuerà il match sul nome missione o altra colonna univoca, aggiornando le colonne desiderate (es. `Personale`, `Costo_€`).

---

## ⚙️ Dipendenze

Installabili via pip:

```bash
pip install pdfplumber pandas openpyxl
```

---

## ✍️ Autori
Cosimo Melella & GPT-4 - 2025
