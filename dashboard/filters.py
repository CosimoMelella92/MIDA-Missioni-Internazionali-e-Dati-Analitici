"""
Modulo filtri sidebar per la dashboard MIDA.
Gestisce tutti i widget di filtraggio e la logica di applicazione filtri.
"""

import pandas as pd
import streamlit as st


def render_sidebar_filters(df: pd.DataFrame) -> dict:
    """
    Renderizza i filtri nella sidebar e restituisce un dizionario
    con i valori selezionati dall'utente.
    """
    st.sidebar.header("🔍 Filtri")

    filters = {}

    # Filtro per anno di inizio
    anni_disponibili = sorted(df["data_inizio"].dt.year.dropna().unique().astype(int).tolist())
    periodi = ["Tutti i periodi"] + anni_disponibili
    filters["anno"] = st.sidebar.selectbox("Anno di inizio", periodi)

    # Filtro per tipo di partecipazione
    tipi_part = ["Tutti"] + sorted(df["tipo_partecipazione"].dropna().unique().tolist())
    filters["tipo_partecipazione"] = st.sidebar.selectbox("Tipo di partecipazione", tipi_part)

    # Filtro per regione
    regioni = ["Tutte le regioni"] + sorted(df["regione"].dropna().unique().tolist())
    filters["regione"] = st.sidebar.selectbox("Regione", regioni)

    # Filtro per organizzazione
    organizzazioni = ["Tutte le organizzazioni"] + sorted(df["tipo_missione"].dropna().unique().tolist())
    filters["organizzazione"] = st.sidebar.selectbox("Organizzazione", organizzazioni)

    # Filtro per commitment
    if "commitment" in df.columns:
        commitments = ["Tutti i commitment"] + sorted(df["commitment"].dropna().unique().tolist())
        filters["commitment"] = st.sidebar.selectbox("Tipo di Commitment", commitments)
    else:
        filters["commitment"] = "Tutti i commitment"

    return filters


def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """
    Applica i filtri selezionati al DataFrame in modo vettorizzato.
    Restituisce il DataFrame filtrato.
    """
    mask = pd.Series(True, index=df.index)

    if filters.get("anno") and filters["anno"] != "Tutti i periodi":
        mask &= df["data_inizio"].dt.year == filters["anno"]

    if filters.get("tipo_partecipazione") and filters["tipo_partecipazione"] != "Tutti":
        mask &= df["tipo_partecipazione"] == filters["tipo_partecipazione"]

    if filters.get("regione") and filters["regione"] != "Tutte le regioni":
        mask &= df["regione"] == filters["regione"]

    if filters.get("organizzazione") and filters["organizzazione"] != "Tutte le organizzazioni":
        mask &= df["tipo_missione"] == filters["organizzazione"]

    if filters.get("commitment") and filters["commitment"] != "Tutti i commitment":
        if "commitment" in df.columns:
            mask &= df["commitment"] == filters["commitment"]

    return df[mask]


def render_debug_sidebar(df: pd.DataFrame, df_filtered: pd.DataFrame) -> None:
    """Renderizza informazioni di debug nella sidebar."""
    st.sidebar.markdown("---")
    st.sidebar.header("🛠️ Debug Dati")

    if st.sidebar.button("🔄 Ricarica Dati"):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.write(f"Missioni caricate: {len(df)}")
    st.sidebar.write(f"Missioni dopo filtri: {len(df_filtered)}")
    st.sidebar.write(f"Missioni attive: {df['is_active'].sum()}")
    st.sidebar.write(f"Date non valide: {df['data_inizio'].isna().sum()}")

    # Missioni con campi chiave mancanti
    missing = df[df[["nome", "paese", "tipo_missione"]].isna().any(axis=1)]
    if not missing.empty:
        with st.sidebar.expander(f"⚠️ {len(missing)} missioni con campi mancanti"):
            for _, row in missing.head(10).iterrows():
                st.sidebar.write(f"- {row.get('nome', 'N/A')}")
