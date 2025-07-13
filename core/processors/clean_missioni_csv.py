import pandas as pd

# Percorso del file CSV
csv_path = 'data/processed/missioni_complete.csv'

def clean_csv(path):
    df = pd.read_csv(path)
    # Rimuovi righe con nome missione mancante o vuoto
    df_clean = df[df['nome'].notna() & (df['nome'].astype(str).str.strip() != '')]
    # Rimuovi righe completamente vuote
    df_clean = df_clean.dropna(how='all')
    # Salva il file pulito
    df_clean.to_csv(path, index=False)
    print(f'File pulito salvato: {path} ({len(df_clean)} missioni)')

if __name__ == '__main__':
    clean_csv(csv_path) 