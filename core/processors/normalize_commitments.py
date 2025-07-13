#!/usr/bin/env python3
"""
Script per normalizzare i commitment e unificare le varianti
"""

import pandas as pd
import re

def normalize_commitments():
    """Normalizza i commitment nel dataset"""
    
    # Carica il dataset
    df = pd.read_csv('data/processed/missioni_complete.csv')
    
    print(f"Dataset caricato: {len(df)} missioni")
    print("Commitment attuali:")
    print(df['commitment'].value_counts())
    
    # Normalizza i commitment
    def normalize_commitment(commitment):
        if pd.isna(commitment):
            return commitment
        
        commitment_str = str(commitment).strip()
        
        # Normalizza "Troops (ground forces)" e varianti
        if re.search(r'troops\s*\(ground\s*forces\)', commitment_str, re.IGNORECASE):
            return 'Troops (Ground Forces)'
        
        # Normalizza altri commitment comuni
        commitment_lower = commitment_str.lower()
        
        if 'head of mission' in commitment_lower:
            return 'Head of Mission'
        elif 'advisory' in commitment_lower and 'training' in commitment_lower:
            return 'Advisory/Training'
        elif 'logistical' in commitment_lower and 'support' in commitment_lower:
            return 'Logistical Support & Advisory'
        elif 'naval' in commitment_lower:
            return 'Troops (Naval)'
        elif 'air' in commitment_lower:
            return 'Troops (Air)'
        elif 'troops' in commitment_lower:
            return 'Troops'
        else:
            return commitment_str
    
    # Applica la normalizzazione
    df['commitment'] = df['commitment'].apply(normalize_commitment)
    
    print("\nCommitment dopo normalizzazione:")
    print(df['commitment'].value_counts())
    
    # Salva il dataset aggiornato
    df.to_csv('data/processed/missioni_complete.csv', index=False)
    
    print(f"\n✅ Dataset aggiornato salvato in: data/processed/missioni_complete.csv")
    print(f"📊 Totale missioni: {len(df)}")

if __name__ == "__main__":
    normalize_commitments() 