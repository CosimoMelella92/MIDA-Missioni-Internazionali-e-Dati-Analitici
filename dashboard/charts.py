"""
Modulo grafici Plotly per la dashboard MIDA.
Tema coerente, hover ricchi, layout professionale.
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# PALETTE COLORI
# ---------------------------------------------------------------------------
# Palette militare italiana
# Verde oliva: #3D4F1E #4A5D23 #6B8C2A   Blu marina: #1B3A5C #2C5F8A #4A90C4
# Sabbia: #C4A35A #D4B96E   Rosso esercito: #8B1A1A #A52A2A
# Grigio acciaio: #5A5F63 #8B9298   Kaki: #7D6B3A
ORG_COLORS = {
    "ONU": "#2C5F8A",       # blu marina medio
    "UE": "#4A5D23",        # verde oliva
    "NATO": "#1B3A5C",      # blu marina scuro
    "ITA": "#8B1A1A",       # rosso esercito
    "Multinational": "#5C4E2A",  # kaki scuro
    "Bilateral": "#8B7332",      # sabbia scura
    "Coalizione": "#6B8C2A",     # verde oliva chiaro
    "Altro": "#5A5F63",          # grigio acciaio
}

REGION_COLORS = {
    "Africa": "#A52A2A",          # rosso mattone
    "Europa": "#1B3A5C",          # blu marina
    "Medio Oriente": "#8B7332",   # sabbia scura
    "Asia": "#4A5D23",            # verde oliva
    "America": "#2C5F8A",         # blu marina medio
    "Non specificata": "#8B9298", # grigio acciaio chiaro
}

PARTICIPATION_COLORS = {
    "mil": "#3D4F1E",    # verde oliva scuro
    "civ": "#2C5F8A",    # blu marina medio
    "civmil": "#5C4E2A", # kaki scuro
}

COMMITMENT_COLORS = {
    "Head of Mission": "#1B3A5C",  # blu marina
    "Troops": "#8B1A1A",           # rosso esercito
}

PERIOD_COLORS = ["#3D4F1E", "#1B3A5C", "#8B7332", "#6B8C2A", "#8B1A1A"]

# ---------------------------------------------------------------------------
# TEMA GLOBALE
# ---------------------------------------------------------------------------
MIDA_LAYOUT = dict(
    font=dict(family="Inter, Segoe UI, sans-serif", size=12, color="#2C2C2C"),
    title_font=dict(size=15, color="#3D4F1E"),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=40, r=20, t=50, b=30),
    hovermode="x unified",
    legend=dict(
        orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
        bgcolor="rgba(245,243,238,0.9)", bordercolor="#D4CFC3", borderwidth=1,
        font=dict(size=11),
    ),
    xaxis=dict(gridcolor="#EAE6DC", linecolor="#D4CFC3", zeroline=False),
    yaxis=dict(gridcolor="#EAE6DC", linecolor="#D4CFC3", zeroline=False),
)


MIDA_LAYOUT_DARK = dict(
    font=dict(family="Inter, Segoe UI, sans-serif", size=12, color="#D4CFC3"),
    title_font=dict(size=15, color="#6B8C2A"),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=40, r=20, t=50, b=30),
    hovermode="x unified",
    legend=dict(
        orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
        bgcolor="rgba(34,39,46,0.9)", bordercolor="#3A3F45", borderwidth=1,
        font=dict(size=11, color="#D4CFC3"),
    ),
    xaxis=dict(gridcolor="#3A3F45", linecolor="#3A3F45", zeroline=False),
    yaxis=dict(gridcolor="#3A3F45", linecolor="#3A3F45", zeroline=False),
)


def _is_dark_mode() -> bool:
    """Check if dark mode is active via Streamlit session state."""
    try:
        import streamlit as st
        return st.session_state.get("dark_mode", False)
    except Exception:
        return False


def _apply_theme(fig: go.Figure) -> go.Figure:
    """Applica il tema MIDA a qualsiasi figura."""
    layout = MIDA_LAYOUT_DARK if _is_dark_mode() else MIDA_LAYOUT
    fig.update_layout(**layout)
    return fig


def format_currency(value: float) -> str:
    """Formatta i valori monetari."""
    if value >= 1e9:
        return f"€{value / 1e9:.1f}B"
    elif value >= 1e6:
        return f"€{value / 1e6:.1f}M"
    elif value >= 1e3:
        return f"€{value / 1e3:.1f}K"
    return f"€{value:,.0f}"


# =============================================================================
# GRAFICI PER PERIODO
# =============================================================================

def chart_missions_by_period(period_stats: pd.DataFrame) -> go.Figure:
    """Grafico a barre: numero missioni per periodo."""
    fig = px.bar(
        period_stats, x="Periodo", y="Numero Missioni",
        title="Missioni per Periodo",
        color="Periodo", color_discrete_sequence=PERIOD_COLORS,
        text="Numero Missioni",
    )
    fig.update_traces(textposition="outside", marker_line_width=0,
                      marker_cornerradius=5)
    return _apply_theme(fig).update_layout(showlegend=False, height=380)


def chart_budget_by_period(period_stats: pd.DataFrame) -> go.Figure:
    """Grafico a ciambella: distribuzione budget per periodo."""
    fig = px.pie(
        period_stats, values="Costo Totale", names="Periodo",
        title="Distribuzione Budget per Periodo",
        color_discrete_sequence=PERIOD_COLORS, hole=0.45,
    )
    fig.update_traces(
        textinfo="percent+label", textposition="outside",
        pull=[0.03] * len(period_stats),
        hovertemplate="<b>%{label}</b><br>Costo: €%{value:,.0f}<br>Quota: %{percent}<extra></extra>",
    )
    return _apply_theme(fig).update_layout(height=380)


def chart_personnel_by_period(period_stats: pd.DataFrame) -> go.Figure:
    """Grafico a barre impilate: personale militare vs civile per periodo."""
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Militare", x=period_stats["Periodo"],
        y=period_stats["Personale Militare"], marker_color="#3D4F1E",
        hovertemplate="<b>Militare</b><br>%{x}: %{y:,.0f}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="Civile", x=period_stats["Periodo"],
        y=period_stats["Personale Civile"], marker_color="#2C5F8A",
        hovertemplate="<b>Civile</b><br>%{x}: %{y:,.0f}<extra></extra>",
    ))
    fig.update_layout(barmode="stack", title="Personale per Periodo")
    return _apply_theme(fig)


# =============================================================================
# GRAFICI PER PARTECIPAZIONE
# =============================================================================

def chart_missions_by_participation(stats: pd.DataFrame) -> go.Figure:
    """Grafico a barre: missioni per tipo di partecipazione."""
    fig = px.bar(
        stats, x="Tipo Partecipazione", y="Numero Missioni",
        title="Missioni per Tipo di Partecipazione",
        color="Tipo Partecipazione", color_discrete_map=PARTICIPATION_COLORS,
        text="Numero Missioni",
    )
    fig.update_traces(textposition="outside")
    return _apply_theme(fig).update_layout(showlegend=False)


def chart_personnel_distribution(stats: pd.DataFrame) -> go.Figure:
    """Grafico a ciambella: distribuzione personale per tipo partecipazione."""
    fig = px.pie(
        stats, values="Personale Totale", names="Tipo Partecipazione",
        title="Distribuzione Personale per Partecipazione",
        color_discrete_map=PARTICIPATION_COLORS, hole=0.4,
    )
    fig.update_traces(
        textinfo="percent+label",
        hovertemplate="<b>%{label}</b><br>Personale: %{value:,.0f}<br>Quota: %{percent}<extra></extra>",
    )
    return _apply_theme(fig)


# =============================================================================
# GRAFICI PER REGIONE
# =============================================================================

def chart_missions_by_region(region_summary: pd.DataFrame) -> go.Figure:
    """Grafico a barre orizzontali: missioni per regione."""
    rs = region_summary.sort_values("Numero Missioni", ascending=True)
    fig = px.bar(
        rs, y="Regione", x="Numero Missioni", orientation="h",
        title="Missioni per Regione", color="Regione",
        color_discrete_map=REGION_COLORS, text="Numero Missioni",
    )
    fig.update_traces(textposition="outside", marker_cornerradius=5)
    return _apply_theme(fig).update_layout(showlegend=False, height=350)


def chart_cost_by_region(region_cost: pd.DataFrame) -> go.Figure:
    """Grafico a barre orizzontali: costo per regione."""
    rc = region_cost.sort_values("Costo Totale", ascending=True)
    fig = px.bar(
        rc, y="Regione", x="Costo Totale", orientation="h",
        title="Costo Totale per Regione", color="Regione",
        color_discrete_map=REGION_COLORS,
    )
    fig.update_traces(
        hovertemplate="<b>%{y}</b><br>Costo: €%{x:,.0f}<extra></extra>",
    )
    return _apply_theme(fig).update_layout(showlegend=False)


def chart_personnel_by_region(region_personnel: pd.DataFrame) -> go.Figure:
    """Grafico a barre orizzontali: personale per regione."""
    rp = region_personnel.sort_values("Personale Totale", ascending=True)
    fig = px.bar(
        rp, y="Regione", x="Personale Totale", orientation="h",
        title="Personale per Regione", color="Regione",
        color_discrete_map=REGION_COLORS,
    )
    fig.update_traces(
        hovertemplate="<b>%{y}</b><br>Personale: %{x:,.0f}<extra></extra>",
    )
    return _apply_theme(fig).update_layout(showlegend=False)


def chart_region_heatmap(regional_stats: pd.DataFrame) -> go.Figure | None:
    """Mappa di calore: missioni per regione e sub-regione."""
    if regional_stats.empty:
        return None
    pivot = regional_stats.pivot_table(
        values="Numero Missioni", index="Regione", columns="Sub-Regione",
        aggfunc="sum", fill_value=0,
    )
    fig = px.imshow(
        pivot, title="Missioni per Regione e Sub-Regione",
        aspect="auto", color_continuous_scale="YlOrRd",
        labels=dict(color="Missioni"),
    )
    return _apply_theme(fig)


def chart_region_treemap(df: pd.DataFrame) -> go.Figure:
    """Treemap: distribuzione missioni per regione e paese."""
    tree = df.groupby(["regione", "paese"]).agg(
        count=("nome", "count"),
        personale=("personale_totale", "sum"),
    ).reset_index()
    tree.columns = ["Regione", "Paese", "Missioni", "Personale"]
    fig = px.treemap(
        tree, path=["Regione", "Paese"], values="Missioni",
        color="Personale", color_continuous_scale="Blues",
        title="Distribuzione Missioni per Regione e Paese",
        hover_data={"Personale": ":,.0f"},
    )
    fig.update_traces(textinfo="label+value")
    return _apply_theme(fig)


# =============================================================================
# GRAFICI PER ORGANIZZAZIONE
# =============================================================================

def chart_missions_by_org(org_stats: pd.DataFrame) -> go.Figure:
    """Grafico a barre: missioni per organizzazione."""
    os_ = org_stats.sort_values("Numero Missioni", ascending=True)
    fig = px.bar(
        os_, y="Organizzazione", x="Numero Missioni", orientation="h",
        title="Missioni per Organizzazione",
        color="Organizzazione", color_discrete_map=ORG_COLORS,
        text="Numero Missioni",
    )
    fig.update_traces(textposition="outside", marker_cornerradius=5)
    return _apply_theme(fig).update_layout(showlegend=False, height=380)


def chart_personnel_pie_by_org(org_stats: pd.DataFrame) -> go.Figure:
    """Grafico a ciambella: personale per organizzazione."""
    fig = px.pie(
        org_stats, values="Personale Totale", names="Organizzazione",
        title="Distribuzione Personale per Organizzazione",
        color_discrete_map=ORG_COLORS, hole=0.45,
    )
    fig.update_traces(
        textinfo="percent+label",
        pull=[0.02] * len(org_stats),
        hovertemplate="<b>%{label}</b><br>Personale: %{value:,.0f}<br>Quota: %{percent}<extra></extra>",
    )
    return _apply_theme(fig).update_layout(height=380)


def chart_mil_civ_by_org(org_stats: pd.DataFrame) -> go.Figure:
    """Grafico a barre impilate: militare vs civile per organizzazione."""
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Militare", x=org_stats["Organizzazione"],
        y=org_stats["Personale Militare"], marker_color="#3D4F1E",
        hovertemplate="<b>Militare</b><br>%{x}: %{y:,.0f}<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="Civile", x=org_stats["Organizzazione"],
        y=org_stats["Personale Civile"], marker_color="#2C5F8A",
        hovertemplate="<b>Civile</b><br>%{x}: %{y:,.0f}<extra></extra>",
    ))
    fig.update_layout(barmode="stack", title="Personale Militare vs Civile")
    return _apply_theme(fig)


def chart_cost_by_org(org_stats: pd.DataFrame) -> go.Figure:
    """Grafico a barre: costo per organizzazione."""
    os_ = org_stats.sort_values("Costo Totale", ascending=True)
    fig = px.bar(
        os_, y="Organizzazione", x="Costo Totale", orientation="h",
        title="Costo per Organizzazione",
        color="Organizzazione", color_discrete_map=ORG_COLORS,
    )
    fig.update_traces(
        hovertemplate="<b>%{y}</b><br>Costo: €%{x:,.0f}<extra></extra>",
    )
    return _apply_theme(fig).update_layout(showlegend=False)


def chart_org_sunburst(df: pd.DataFrame) -> go.Figure:
    """Sunburst: organizzazione → regione → missione."""
    sun = df.groupby(["tipo_missione", "regione"]).agg(
        count=("nome", "count"),
        personale=("personale_totale", "sum"),
    ).reset_index()
    sun.columns = ["Organizzazione", "Regione", "Missioni", "Personale"]
    fig = px.sunburst(
        sun, path=["Organizzazione", "Regione"], values="Missioni",
        color="Organizzazione", color_discrete_map=ORG_COLORS,
        title="Missioni: Organizzazione → Regione",
        hover_data={"Personale": ":,.0f"},
    )
    return _apply_theme(fig)


# =============================================================================
# GRAFICI PER COMMITMENT
# =============================================================================

def chart_missions_by_commitment(stats: pd.DataFrame) -> go.Figure:
    """Grafico a barre: missioni per tipo commitment."""
    fig = px.bar(
        stats, x="Tipo Commitment", y="Numero Missioni",
        title="Missioni per Tipo di Commitment",
        color="Tipo Commitment", color_discrete_map=COMMITMENT_COLORS,
        text="Numero Missioni",
    )
    fig.update_traces(textposition="outside")
    return _apply_theme(fig).update_layout(showlegend=False)


def chart_commitment_pie(stats: pd.DataFrame) -> go.Figure:
    """Grafico a ciambella: distribuzione commitment."""
    fig = px.pie(
        stats, values="Numero Missioni", names="Tipo Commitment",
        title="Distribuzione Commitment",
        color_discrete_map=COMMITMENT_COLORS, hole=0.4,
    )
    fig.update_traces(textinfo="percent+label")
    return _apply_theme(fig)


def chart_personnel_by_commitment(stats: pd.DataFrame) -> go.Figure:
    """Grafico a barre: personale per commitment."""
    fig = px.bar(
        stats, x="Tipo Commitment", y="Personale Totale",
        title="Personale per Commitment",
        color="Tipo Commitment", color_discrete_map=COMMITMENT_COLORS,
        text="Personale Totale",
    )
    fig.update_traces(textposition="outside", texttemplate="%{text:,.0f}")
    return _apply_theme(fig).update_layout(showlegend=False)


def chart_cost_by_commitment(stats: pd.DataFrame) -> go.Figure:
    """Grafico a barre: costo per commitment."""
    fig = px.bar(
        stats, x="Tipo Commitment", y="Costo Totale",
        title="Costo per Commitment",
        color="Tipo Commitment", color_discrete_map=COMMITMENT_COLORS,
    )
    fig.update_traces(
        hovertemplate="<b>%{x}</b><br>Costo: €%{y:,.0f}<extra></extra>",
    )
    return _apply_theme(fig).update_layout(showlegend=False)


def chart_commitment_detailed_bar(df_commitment: pd.DataFrame) -> go.Figure:
    """Grafico a barre: distribuzione commitment dettagliato."""
    grouped = df_commitment.groupby("Commitment Dettagliato").size().reset_index(name="Numero Missioni")
    grouped = grouped.sort_values("Numero Missioni", ascending=True)
    fig = px.bar(
        grouped, y="Commitment Dettagliato", x="Numero Missioni",
        orientation="h", color="Commitment Dettagliato",
        title="Missioni per Commitment Dettagliato",
        color_discrete_sequence=px.colors.qualitative.Set2,
        text="Numero Missioni",
    )
    fig.update_traces(textposition="outside")
    return _apply_theme(fig).update_layout(showlegend=False)


# =============================================================================
# TIMELINE
# =============================================================================

def chart_timeline_by_organization(df: pd.DataFrame) -> go.Figure:
    """Timeline raggruppata per organizzazione con subplot."""
    df_t = df.dropna(subset=["data_inizio"]).copy()
    if df_t.empty:
        return go.Figure()
    df_t["anno"] = df_t["data_inizio"].dt.year

    org_year = df_t.groupby(["tipo_missione", "anno"]).agg(
        nome=("nome", "count"),
        personale_totale=("personale_totale", "sum"),
    ).reset_index()

    organizations = df_t["tipo_missione"].unique()
    if len(organizations) == 0:
        return go.Figure()

    fig = make_subplots(
        rows=len(organizations), cols=1,
        subplot_titles=[f"🏛️ {org}" for org in organizations],
        vertical_spacing=0.05,
        specs=[[{"secondary_y": True}] for _ in organizations],
    )

    for i, org in enumerate(organizations, 1):
        org_data = org_year[org_year["tipo_missione"] == org]
        color = ORG_COLORS.get(org, "#7f7f7f")

        fig.add_trace(go.Bar(
            x=org_data["anno"], y=org_data["nome"], name=f"{org} - Missioni",
            marker_color=color, opacity=0.8,
            hovertemplate=f"<b>{org}</b><br>Anno: %{{x}}<br>Missioni: %{{y}}<extra></extra>",
        ), row=i, col=1)

        fig.add_trace(go.Scatter(
            x=org_data["anno"], y=org_data["personale_totale"],
            name=f"{org} - Personale", mode="lines+markers",
            line=dict(color=color, width=3), marker=dict(size=8),
            hovertemplate=f"<b>{org}</b><br>Anno: %{{x}}<br>Personale: %{{y:,.0f}}<extra></extra>",
        ), row=i, col=1, secondary_y=True)

    _apply_theme(fig)
    fig.update_layout(
        title="Timeline per Organizzazione",
        height=280 * len(organizations), showlegend=False, hovermode="closest",
    )
    for i in range(len(organizations)):
        fig.update_xaxes(title_text="Anno", row=i + 1, col=1, gridcolor="#EAE6DC")
        fig.update_yaxes(title_text="Missioni", row=i + 1, col=1, gridcolor="#EAE6DC")
        fig.update_yaxes(title_text="Personale", row=i + 1, col=1, secondary_y=True, gridcolor="#EAE6DC")

    return fig


def chart_timeline_by_region(df: pd.DataFrame) -> go.Figure:
    """Timeline raggruppata per regione con subplot."""
    df_t = df.dropna(subset=["data_inizio"]).copy()
    if df_t.empty:
        return go.Figure()
    df_t["anno"] = df_t["data_inizio"].dt.year

    region_year = df_t.groupby(["regione", "anno"]).agg(
        nome=("nome", "count"),
        personale_totale=("personale_totale", "sum"),
    ).reset_index()

    regions = df_t["regione"].unique()
    if len(regions) == 0:
        return go.Figure()

    fig = make_subplots(
        rows=len(regions), cols=1,
        subplot_titles=[f"🌍 {r}" for r in regions],
        vertical_spacing=0.05,
        specs=[[{"secondary_y": True}] for _ in regions],
    )

    for i, region in enumerate(regions, 1):
        rdata = region_year[region_year["regione"] == region]
        color = REGION_COLORS.get(region, "#95a5a6")

        fig.add_trace(go.Bar(
            x=rdata["anno"], y=rdata["nome"], name=f"{region} - Missioni",
            marker_color=color, opacity=0.8,
            hovertemplate=f"<b>{region}</b><br>Anno: %{{x}}<br>Missioni: %{{y}}<extra></extra>",
        ), row=i, col=1)

        fig.add_trace(go.Scatter(
            x=rdata["anno"], y=rdata["personale_totale"],
            name=f"{region} - Personale", mode="lines+markers",
            line=dict(color=color, width=3), marker=dict(size=8),
            hovertemplate=f"<b>{region}</b><br>Anno: %{{x}}<br>Personale: %{{y:,.0f}}<extra></extra>",
        ), row=i, col=1, secondary_y=True)

    _apply_theme(fig)
    fig.update_layout(
        title="Timeline per Regione",
        height=280 * len(regions), showlegend=False, hovermode="closest",
    )
    for i in range(len(regions)):
        fig.update_xaxes(title_text="Anno", row=i + 1, col=1, gridcolor="#EAE6DC")
        fig.update_yaxes(title_text="Missioni", row=i + 1, col=1, gridcolor="#EAE6DC")
        fig.update_yaxes(title_text="Personale", row=i + 1, col=1, secondary_y=True, gridcolor="#EAE6DC")

    return fig


def chart_timeline_with_duration(df: pd.DataFrame) -> go.Figure:
    """Timeline con durata delle missioni (top 15 per organizzazione)."""
    df_d = df.dropna(subset=["data_inizio", "data_fine"]).copy()
    if df_d.empty:
        return go.Figure()
    df_d["durata_giorni"] = (df_d["data_fine"] - df_d["data_inizio"]).dt.days
    df_d["durata_anni"] = df_d["durata_giorni"] / 365.25
    df_d = df_d[df_d["durata_giorni"] > 30]

    top = df_d.nlargest(15, "durata_giorni")
    organizations = top["tipo_missione"].unique()
    if len(organizations) == 0:
        return go.Figure()

    fig = make_subplots(
        rows=len(organizations), cols=1,
        subplot_titles=[f"🏛️ {org} - Missioni più longeve" for org in organizations],
        vertical_spacing=0.08,
    )

    for i, org in enumerate(organizations, 1):
        org_m = top[top["tipo_missione"] == org].sort_values("durata_anni", ascending=True)
        color = ORG_COLORS.get(org, "#7f7f7f")

        for _, row in org_m.iterrows():
            label = f"{row['nome'][:20]}... ({row['durata_anni']:.1f}a)"
            fig.add_trace(go.Bar(
                x=[row["durata_anni"]], y=[label], orientation="h",
                name=org, marker_color=color, opacity=0.8,
                hovertemplate=(
                    f"<b>{row['nome']}</b><br>"
                    f"Paese: {row['paese']}<br>"
                    f"Durata: {row['durata_anni']:.1f} anni<br>"
                    f"Personale: {row['personale_totale']:,.0f}<br>"
                    "<extra></extra>"
                ),
            ), row=i, col=1)

    _apply_theme(fig)
    fig.update_layout(
        title="Top 15 Missioni più Longeve",
        height=200 * len(organizations), showlegend=False, hovermode="closest",
    )
    for i in range(len(organizations)):
        fig.update_xaxes(title_text="Durata (anni)", row=i + 1, col=1, gridcolor="#EAE6DC")

    return fig


def chart_interactive_timeline(df: pd.DataFrame, selected_years: tuple) -> go.Figure:
    """Timeline interattiva per il periodo selezionato."""
    df_t = df.dropna(subset=["data_inizio"]).copy()
    if df_t.empty:
        return go.Figure()
    df_t["anno_mese"] = df_t["data_inizio"].dt.to_period("M")

    monthly = df_t.groupby(["anno_mese", "tipo_missione"]).agg(
        nome=("nome", "count"),
        personale_totale=("personale_totale", "sum"),
    ).reset_index()
    monthly["data"] = monthly["anno_mese"].dt.to_timestamp()

    fig = go.Figure()

    for org in monthly["tipo_missione"].unique():
        org_data = monthly[monthly["tipo_missione"] == org]
        color = ORG_COLORS.get(org, "#7f7f7f")
        fig.add_trace(go.Scatter(
            x=org_data["data"], y=org_data["nome"],
            mode="lines+markers", name=f"🏛️ {org}",
            line=dict(color=color, width=3), marker=dict(size=8),
            hovertemplate=f"<b>{org}</b><br>Data: %{{x|%B %Y}}<br>Nuove missioni: %{{y}}<extra></extra>",
        ))

    total_pers = monthly.groupby("data")["personale_totale"].sum().reset_index()
    fig.add_trace(go.Scatter(
        x=total_pers["data"], y=total_pers["personale_totale"],
        mode="lines", name="👥 Personale Totale",
        line=dict(color="#6B8C2A", width=4, dash="dash"), yaxis="y2",
        hovertemplate="<b>Personale Totale</b><br>Data: %{x|%B %Y}<br>Personale: %{y:,.0f}<extra></extra>",
    ))

    _apply_theme(fig)
    fig.update_layout(
        title=f"Timeline Interattiva ({selected_years[0]}-{selected_years[1]})",
        xaxis_title="Data", yaxis_title="Nuove Missioni", height=500,
        hovermode="closest",
        yaxis2=dict(title="Personale Totale", overlaying="y", side="right", gridcolor="#EAE6DC"),
    )
    return fig


def chart_scatter_timeline(df: pd.DataFrame) -> go.Figure:
    """Scatter plot interattivo delle missioni nel tempo."""
    df = df.dropna(subset=["data_inizio"]).copy()
    fig = go.Figure()
    for mtype in df["tipo_missione"].unique():
        missions = df[df["tipo_missione"] == mtype]
        color = ORG_COLORS.get(mtype, "#6C757D")
        sizes = np.clip(np.sqrt(missions["personale_totale"].fillna(0)) * 1.5 + 4, 4, 40)
        fig.add_trace(go.Scatter(
            x=missions["data_inizio"], y=missions["personale_totale"],
            mode="markers", name=mtype, text=missions["nome"],
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Data: %{x|%Y-%m-%d}<br>"
                "Personale: %{y:,.0f}<extra></extra>"
            ),
            marker=dict(size=sizes, color=color, opacity=0.75,
                        line=dict(width=1, color="white")),
        ))
    _apply_theme(fig)
    fig.update_layout(
        title="Missioni nel Tempo (dimensione = personale)",
        xaxis_title="Anno", yaxis_title="Personale Totale",
        hovermode="closest", height=500,
    )
    return fig


def chart_gantt(df: pd.DataFrame) -> go.Figure:
    """Gantt chart delle missioni con durata (px.timeline)."""
    df = df.dropna(subset=["data_inizio"]).copy()
    if df.empty:
        return go.Figure()
    df["data_fine_safe"] = df["data_fine"].fillna(pd.Timestamp.now())
    df["label"] = df["nome"].str[:35]
    df_sorted = df.sort_values("data_inizio")
    # Limit to top 40 by personnel to keep chart readable
    if len(df_sorted) > 40:
        df_sorted = df_sorted.nlargest(40, "personale_totale")
    fig = px.timeline(
        df_sorted, x_start="data_inizio", x_end="data_fine_safe",
        y="label", color="tipo_missione",
        color_discrete_map=ORG_COLORS,
        title="Gantt delle Missioni",
        hover_data={"nome": True, "paese": True, "personale_totale": ":,.0f",
                    "label": False, "data_fine_safe": False},
        labels={"tipo_missione": "Organizzazione", "label": "Missione"},
    )
    _apply_theme(fig)
    fig.update_layout(
        height=max(400, 28 * len(df_sorted)),
        yaxis=dict(autorange="reversed", dtick=1),
    )
    fig.update_yaxes(tickfont=dict(size=10))
    return fig


def chart_period_bar(df: pd.DataFrame, period_col: str = "Periodo Storico") -> go.Figure:
    """Grafico a barre: missioni per periodo storico."""
    counts = df[period_col].value_counts().sort_index()
    fig = px.bar(
        x=counts.index, y=counts.values,
        title="Missioni per Periodo Storico",
        labels={"x": "Periodo", "y": "Numero Missioni"},
        color=counts.index, color_discrete_sequence=PERIOD_COLORS,
        text=counts.values,
    )
    fig.update_traces(textposition="outside")
    return _apply_theme(fig).update_layout(showlegend=False)
