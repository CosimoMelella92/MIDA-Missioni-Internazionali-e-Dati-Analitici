![Sponsorship Banner](docs/images/banner_sponsor.png)

<div align="center">
  <b>Progetto finanziato dall'Unione Europea – NextGenerationEU, Ministero dell'Università e della Ricerca, Italia Domani – PNRR</b>
</div>

# Analisi Commitment - Dashboard MIDA

## 🎯 Nuove Funzionalità Aggiunte

### 1. Classificazione del Commitment
È stata aggiunta una nuova classificazione delle missioni basata sul tipo di commitment:

- **Head of Mission**: Missioni con personale principalmente civile o di supporto, spesso missioni di training, monitoraggio o assistenza tecnica
- **Troops**: Missioni con significativo dispiegamento di forze militari, incluse operazioni di peacekeeping, sicurezza e supporto logistico

### 2. Nuova Sezione Dashboard
La dashboard ora include una sezione dedicata all'analisi del commitment con:

- **Grafico a barre**: Numero di missioni per tipo di commitment
- **Grafico a torta**: Distribuzione missioni per tipo di commitment  
- **Grafico personale**: Personale totale per tipo di commitment
- **Grafico costi**: Costo totale per tipo di commitment
- **Tabella dettagliata**: Riepilogo completo per tipo di commitment

### 3. Filtro Aggiuntivo
- Nuovo filtro nella sidebar per selezionare il tipo di commitment
- Permette di filtrare le missioni per "Head of Mission" o "Troops"
- Combina con altri filtri esistenti

### 4. Gestione Missioni Storiche
- La missione UNIFIL (iniziata nel 1978) è stata mantenuta perché ancora attiva
- Le missioni che iniziano prima del 1991 ma sono ancora attive vengono incluse nell'analisi
- Il filtro per anno di inizio ora include tutti gli anni disponibili

## 📊 Risultati dell'Analisi

### Distribuzione per Commitment:
- **Head of Mission**: 17 missioni (38.6%)
- **Troops**: 27 missioni (61.4%)

### Personale per Commitment:
- **Head of Mission**: 3,115 personale (17.7%)
- **Troops**: 14,450 personale (82.3%)

### Costi per Commitment:
- **Head of Mission**: €580M (19.6%)
- **Troops**: €2,385M (80.4%)

## 🔧 Modifiche Tecniche

### File Modificati:
1. `src/missioni_dashboard.py` - Aggiunta sezione commitment e logica di pulizia
2. `data/processed/missioni_complete.csv` - Aggiunta colonna commitment
3. `test_commitment_dashboard.py` - Test per verificare le nuove funzionalità

### Nuove Funzioni:
- `create_commitment_analysis()` - Analisi statistica per commitment
- Pulizia automatica dei valori commitment (rimozione spazi extra)
- Gestione missioni storiche ancora attive

## 🚀 Come Utilizzare

1. **Avvia la dashboard**:
   ```bash
   streamlit run src/missioni_dashboard.py
   ```

2. **Testa le funzionalità**:
   ```bash
   python test_commitment_dashboard.py
   ```

3. **Utilizza i filtri**:
   - Seleziona "Tipo di Commitment" nella sidebar
   - Filtra per "Head of Mission" o "Troops"
   - Combina con altri filtri esistenti

## 📈 Interpretazione dei Dati

### Head of Mission:
- Missioni più piccole con focus su assistenza tecnica
- Personale principalmente civile
- Costi relativamente bassi
- Esempi: EUTM, EUCAP, missioni di monitoraggio

### Troops:
- Missioni di peacekeeping e sicurezza
- Significativo dispiegamento militare
- Costi elevati
- Esempi: UNIFIL, KFOR, ISAF

## 🔍 Note Importanti

- La classificazione è basata sui dati disponibili e può essere raffinata
- Missioni storiche ancora attive (come UNIFIL dal 1978) sono incluse
- I costi e il personale sono aggregati per tipo di commitment
- La dashboard mantiene tutti i filtri esistenti per compatibilità 