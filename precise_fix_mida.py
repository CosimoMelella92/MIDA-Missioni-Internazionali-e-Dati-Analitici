import pandas as pd
from datetime import datetime

print("=== FIX PRECISO MIDA - MAPPING 1:1 CON SITO DIFESA ===\n")

# Carica dataset
df = pd.read_csv('data/processed/missioni_complete.csv')
print(f"Dataset attuale: {len(df)} missioni")

# Reset is_active se esiste
if 'is_active' in df.columns:
    df['is_active'] = False
else:
    df['is_active'] = False

# Mapping PRECISO 1:1 tra sito Difesa e nomi esatti nel dataset MIDA
# Formato: 'Nome esatto in MIDA': 'Voce sul sito Difesa'
mapping_difesa_mida = {
    # NATO (10 voci)
    'NATO HQ Sarajevo': 'Bosnia Erzegovina - NATO HQ Sarajevo',
    'Sea Guardian': 'Mar Mediterraneo - Sea Guardian',
    'NATO Mission Iraq': 'Iraq - NATO Mission Iraq',
    'KFOR - Joint Enterprise': 'Kosovo - KFOR - Joint Enterprise',
    'NATO MLNB': 'Serbia - NATO Military Liaison Office Belgrade',
    'Enhanced Forward Presence - Baltic Guardian Lettonia': 'NATO Multinational Battle Group Lettonia - Operazione Baltic Guardian',
    'NATO Standing Naval Forces Med': 'Mar Mediterraneo - NATO Standing Naval Forces',
    'Enhanced Vigilance Activity Bulgaria': 'NATO Multinational Battle Group Bulgaria',
    'Enhanced Vigilance Activity Hungary': 'NATO Multinational Battle Group Ungheria',
    'Baltic Eagle': 'Estonia - eAP Operazione Baltic Eagle III',
    
    # ONU (4 voci)
    'MINURSO': 'Sahara Occidentale - MINURSO',
    'UNIFIL': 'Libano - UNIFIL',
    'UNFICYP': 'Repubblica di Cipro - UNFICYP',
    'UNMOGIP': 'India/Pakistan - UNMOGIP',
    
    # UE (11 voci)
    'EUNAVFOR ATALANTA': 'Oceano Indiano - EUNAVFOR Somalia - Op. Atalanta',
    'EMASOH': 'European Maritime Awareness - Stretto di Hormuz (EMASOH)',
    'EUTM Somalia': 'Somalia - EUTM',
    'EUCAP Somalia': 'Somalia - EUCAP',
    'EUNAVFOR Med - Irini': 'EUNAVFOR MED – Operazione Irini',
    'EUFOR ALTHEA': 'Bosnia Erzegovina - EUFOR - ALTHEA',
    'EULEX Kosovo': 'Kosovo - EULEX',
    'EUNAVFOR Aspides': 'EUNAVFOR Aspides',
    'EUMAM Mozambico': 'EUMAM Mozambico',
    'EUBAM Rafah': 'EUBAM Rafah',
    'EUPOL COPPS': 'Cisgiordania EUPOL COPPS',
    
    # Altre Operazioni (12 voci)
    'MTC4L': 'Libano - MTC4L',
    'Operazione LEVANTE': 'Operazione LEVANTE',
    'CTF153': 'Combined Task Force Mar Rosso – CTF153',
    'MFO': 'Egitto - MFO',
    'Prima Parthica': 'Iraq/Kuwait - Operazione Prima Parthica',
    'MIBIL': 'Libano - MIBIL',
    'MICCD': 'Malta - MICCD',
    'MIASIT': 'Libia - Missione bilaterale di assistenza e supporto in Libia (MIASIT)',
    'MISIN Niger': 'Niger - Missione bilaterale di supporto nella Repubblica del Niger (MISIN)',
    'BMIS Gibuti': 'Gibuti – Base Militare Italiana di Supporto (BMIS)',
    'Mediterraneo Sicuro': 'Operazione Mediterraneo Sicuro',
    'MIADIT Somalia': 'Somalia - MIADIT',
}

print(f"=== APPLICAZIONE MAPPING PRECISO ===")
print(f"Missioni da marcare come attive: {len(mapping_difesa_mida)}\n")

trovate = 0
non_trovate = []

for nome_mida, voce_difesa in mapping_difesa_mida.items():
    # Cerca match esatto
    mask = df['nome'] == nome_mida
    
    if mask.any():
        df.loc[mask, 'is_active'] = True
        trovate += 1
        paese = df.loc[mask, 'paese'].iloc[0]
        tipo = df.loc[mask, 'tipo_missione'].iloc[0]
        print(f"  ✓ {nome_mida:50} | {tipo:12} | {paese}")
    else:
        non_trovate.append((nome_mida, voce_difesa))
        print(f"  ✗ {nome_mida:50} | NON TROVATA")

print(f"\n=== RISULTATI ===")
print(f"Trovate e marcate: {trovate}/{len(mapping_difesa_mida)}")

if non_trovate:
    print(f"\nNON TROVATE ({len(non_trovate)}):")
    for nome_mida, voce_difesa in non_trovate:
        print(f"  - {nome_mida}")
        print(f"    (Difesa: {voce_difesa})")
        
        # Cerca varianti simili nel dataset
        print(f"    Varianti simili nel dataset:")
        keywords = nome_mida.split()[:2]  # Prime 2 parole
        for keyword in keywords:
            if len(keyword) > 3:
                similar = df[df['nome'].str.contains(keyword, case=False, na=False)]['nome'].unique()
                if len(similar) > 0:
                    for s in similar[:3]:
                        print(f"      → {s}")

# Conta missioni attive
attive_count = df['is_active'].sum()
print(f"\n=== CONTEGGIO FINALE ===")
print(f"Totale missioni attive: {attive_count}")
print(f"Target sito Difesa: 37")
print(f"Differenza: {attive_count - 37}")

# Distribuzione per organizzazione
print("\n=== Distribuzione per Organizzazione ===")
print(df[df['is_active'] == True]['tipo_missione'].value_counts().sort_index())

# Salva dataset
print("\n=== Salvataggio ===")
output_file = 'data/processed/missioni_complete.csv'
df.to_csv(output_file, index=False)
print(f"Dataset salvato: {output_file}")

# Elenco missioni attive
print("\n=== ELENCO MISSIONI ATTIVE (is_active=True) ===")
missioni_attive_df = df[df['is_active'] == True].sort_values(['tipo_missione', 'nome'])
for idx, row in missioni_attive_df.iterrows():
    print(f"{row['nome']:50} | {row['tipo_missione']:12} | {row['paese']}")

if attive_count == 37:
    print("\n✓✓✓ SUCCESSO COMPLETO! ✓✓✓")
    print("MIDA è ora perfettamente allineato con il sito del Ministero della Difesa")
elif 37 <= attive_count <= 45:
    print("\n✓ SUCCESSO! ✓")
    print(f"MIDA ha {attive_count} missioni attive (sito Difesa: 37)")
    print("La differenza è dovuta a missioni aggregate diversamente (es. KFOR vs KFOR-Joint Enterprise)")
else:
    print(f"\n⚠ Richiede aggiustamenti: {attive_count} missioni attive vs 37 target")

print("\n=== FIX COMPLETATO ===")
