# Fix Conteggio Missioni Attive: MIDA vs Sito Difesa

## Problema Identificato

**MIDA conta 123 missioni "attive"** mentre **il sito Difesa elenca 37 operazioni in corso**.

### Causa Root
MIDA ha **dati sporchi** nel campo `data_fine`:
- Molte missioni terminate hanno `data_fine = NaT` (Not a Time)
- La query "missioni attive" conta tutto ciò che ha `data_fine >= oggi OR data_fine IS NULL`
- Risultato: missioni terminate da anni (UNPROFOR, IFOR, Ocean Shield, ecc.) vengono conteggiate come "attive"

## Missioni Mancanti in MIDA (presenti su Difesa)

Le seguenti **19 missioni** sono sul sito Difesa ma NON in MIDA:

1. **MINURSO** (Sahara Occidentale)
2. **UNIFIL** (Libano) - PRESENTE ma nome diverso
3. **UNFICYP** (Cipro)
4. **UNMOGIP** (India/Pakistan)
5. **EUTM Somalia** - PRESENTE ma nome diverso
6. **EULEX** (Kosovo)
7. **MTC4L** (Libano)
8. **MIBIL** (Libano)
9. **EMASOH** (Stretto di Hormuz)
10. **EUCAP Somalia**
11. **EUBAM Rafah**
12. **EUPOL COPPS** (Cisgiordania)
13. **EUMAM Mozambico**
14. **Operazione LEVANTE**
15. **CTF153** (Combined Task Force Mar Rosso)
16. **MIASIT** (Libia)
17. **Mediterraneo Sicuro**
18. **NATO HQ Sarajevo** (Bosnia)
19. **Sea Guardian** (Mediterraneo)

## Azioni Correttive Necessarie

### 1. Pulizia Dati (PRIORITÀ ALTA)
- Aggiornare `data_fine` per tutte le missioni terminate (UNPROFOR, IFOR, SFOR, Ocean Shield, Unified Protector, ecc.)
- Verificare manualmente le ~123 missioni con `data_fine = NaT`
- Impostare date di fine corrette basandosi su fonti ufficiali

### 2. Integrazione Missioni Mancanti (PRIORITÀ ALTA)
- Aggiungere le 19 missioni presenti su Difesa ma assenti in MIDA
- Verificare nomenclatura (es. UNIFIL vs "Libano - UNIFIL")
- Standardizzare nomi per matching automatico

### 3. Logica di Conteggio (PRIORITÀ MEDIA)
- Cambiare definizione "missione attiva":
  - **Opzione A**: `data_fine >= 2025-01-01` (anno corrente)
  - **Opzione B**: `data_fine >= oggi AND personale_totale > 0`
  - **Opzione C**: Flag esplicito `is_active = TRUE` gestito manualmente

### 4. Riconciliazione con Fonte Ufficiale (PRIORITÀ ALTA)
- Usare il sito Difesa come **fonte di verità** per missioni attive
- Creare script di scraping/aggiornamento automatico
- Validare periodicamente contro elenco ufficiale

## Impatto a Cascata

Con 123 missioni "attive" invece di ~37-51:
- ❌ **Dashboard**: totali sballati
- ❌ **Report**: statistiche errate (personale, costi, distribuzione geografica)
- ❌ **Mappe**: marker per missioni terminate
- ❌ **Analisi**: trend falsati

## Raccomandazione Immediata

**BLOCCARE** l'uso del conteggio "missioni attive" finché non si completa:
1. Pulizia campo `data_fine` (1-2 giorni di lavoro manuale)
2. Integrazione 19 missioni mancanti (4-6 ore)
3. Validazione contro sito Difesa (2 ore)

**Numero corretto stimato**: ~45-51 missioni effettivamente attive (da validare dopo pulizia).
