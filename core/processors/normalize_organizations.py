#!/usr/bin/env python3
"""
Script per normalizzare le organizzazioni delle missioni
Assicura che tutte le missioni ONU/UN siano classificate come "ONU"
"""
import re

import pandas as pd


def normalize_organizations(df):
    """
    Normalizza le organizzazioni delle missioni
    - Tutte le missioni ONU/UN diventano "ONU"
    - Rimuove duplicati basati su nome e paese
    """

    # Crea una copia del dataframe
    df_normalized = df.copy()

    # Lista di pattern per identificare missioni ONU
    onu_patterns = [
        r'^UN[A-Z]',  # UNIFIL, UNMISS, UNPROFOR, etc.
        r'^MINU',     # MINURSO, MINUSTAH, etc.
        r'^UNAM',     # UNAMA, UNAMID, etc.
        r'^UNS',      # UNSCOM, UNSMIL, etc.
        r'^UNT',      # UNTAET, UNTMIH, etc.
        r'^UNM',      # UNMIK, UNMIL, etc.
        r'^UNI',      # UNISFA, etc.
        r'^UNF',      # UNFICYP, etc.
        r'^UNO',      # UNOCI, etc.
        r'^UNMOG',    # UNMOGIP
        r'^UNTSO',    # UNTSO
        r'peacekeeping',
        r'peace\s*keeping',
        r'united\s*nations',
        r'nazioni\s*unite'
    ]

    # Funzione per identificare se una missione è ONU
    def is_onu_mission(nome, tipo_missione):
        nome_lower = str(nome).lower()
        tipo_lower = str(tipo_missione).lower()

        # Controlla i pattern
        for pattern in onu_patterns:
            if re.search(pattern, nome_lower, re.IGNORECASE):
                return True
            if re.search(pattern, tipo_lower, re.IGNORECASE):
                return True

        # Controlla se il tipo_missione è già ONU o UN
        if tipo_lower in ['onu', 'un', 'united nations', 'nazioni unite']:
            return True

        return False

    # Normalizza le organizzazioni
    print("🔧 Normalizzando le organizzazioni...")
    onu_count = 0
    for idx, row in df_normalized.iterrows():
        nome = row['nome']
        tipo_missione = row['tipo_missione']

        if is_onu_mission(nome, tipo_missione):
            if tipo_missione != 'ONU':
                print(f"  📝 {nome}: {tipo_missione} → ONU")
                df_normalized.at[idx, 'tipo_missione'] = 'ONU'
                onu_count += 1

    print(f"✅ Normalizzate {onu_count} missioni come ONU")

    # Rimuovi duplicati basati su nome e paese
    print("\n🔍 Controllo duplicati...")
    initial_count = len(df_normalized)

    # Normalizza i nomi per il confronto
    df_normalized['nome_normalized'] = df_normalized['nome'].str.lower().str.strip()
    df_normalized['paese_normalized'] = df_normalized['paese'].str.lower().str.strip()

    # Trova duplicati
    duplicates = df_normalized.duplicated(subset=['nome_normalized', 'paese_normalized'], keep='first')
    duplicate_count = duplicates.sum()

    if duplicate_count > 0:
        print(f"⚠️  Trovati {duplicate_count} duplicati:")
        duplicate_rows = df_normalized[duplicates]
        for _, row in duplicate_rows.iterrows():
            print(f"    - {row['nome']} ({row['paese']}) - {row['tipo_missione']}")

    # Rimuovi duplicati
    df_normalized = df_normalized.drop_duplicates(subset=['nome_normalized', 'paese_normalized'], keep='first')

    # Rimuovi colonne temporanee
    df_normalized = df_normalized.drop(['nome_normalized', 'paese_normalized'], axis=1)

    final_count = len(df_normalized)
    removed_count = initial_count - final_count

    print(f"✅ Rimossi {removed_count} duplicati")
    print(f"📊 Totale missioni: {initial_count} → {final_count}")

    return df_normalized

def main():
    """Funzione principale"""
    print("=== Normalizzazione Organizzazioni Missioni ===\n")

    # Carica il file principale
    try:
        df = pd.read_csv('data/processed/missioni_complete.csv')
        print(f"📁 Caricato file principale: {len(df)} missioni")
    except FileNotFoundError:
        print("❌ File missioni_complete.csv non trovato")
        return

    # Normalizza le organizzazioni
    df_normalized = normalize_organizations(df)

    # Salva il file normalizzato
    output_path = 'data/processed/missioni_complete.csv'
    df_normalized.to_csv(output_path, index=False)
    print(f"\n💾 File salvato: {output_path}")

    # Statistiche finali
    print("\n📊 Statistiche finali:")
    org_stats = df_normalized['tipo_missione'].value_counts()
    for org, count in org_stats.items():
        print(f"  {org}: {count} missioni")

    print("\n✅ Normalizzazione completata!")

if __name__ == '__main__':
    main()
