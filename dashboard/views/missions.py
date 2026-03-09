"""
Pagina Missioni della dashboard MIDA.
Mostra analisi per organizzazione, commitment e tabella dati completa.
"""

import io
from datetime import datetime

import pandas as pd
import streamlit as st

from dashboard.analysis import (
    create_commitment_analysis,
    create_commitment_detailed,
    create_organization_analysis,
    create_regional_analysis,
)
from dashboard.charts import (
    chart_commitment_detailed_bar,
    chart_commitment_pie,
    chart_cost_by_commitment,
    chart_cost_by_org,
    chart_cost_by_region,
    chart_mil_civ_by_org,
    chart_missions_by_commitment,
    chart_missions_by_org,
    chart_missions_by_region,
    chart_org_sunburst,
    chart_personnel_by_commitment,
    chart_personnel_by_region,
    chart_personnel_pie_by_org,
    chart_region_heatmap,
    chart_region_treemap,
    format_currency,
)


def render(df: pd.DataFrame) -> None:
    """Renderizza la pagina Missioni."""

    # === ANALISI PER REGIONE ===
    st.markdown("## Analisi per Regione e Sub-Regione")
    regional_stats = create_regional_analysis(df)

    # Treemap full-width
    st.plotly_chart(chart_region_treemap(df), use_container_width=True, key="ms_treemap")

    col1, col2 = st.columns(2)
    with col1:
        region_summary = regional_stats.groupby("Regione")["Numero Missioni"].sum().reset_index()
        st.plotly_chart(chart_missions_by_region(region_summary), use_container_width=True, key="ms_region_missions")

        heatmap = chart_region_heatmap(regional_stats)
        if heatmap:
            st.plotly_chart(heatmap, use_container_width=True, key="ms_heatmap_region")

    with col2:
        region_cost = regional_stats.groupby("Regione")["Costo Totale"].sum().reset_index()
        st.plotly_chart(chart_cost_by_region(region_cost), use_container_width=True, key="ms_region_cost")

        region_pers = regional_stats.groupby("Regione")["Personale Totale"].sum().reset_index()
        st.plotly_chart(chart_personnel_by_region(region_pers), use_container_width=True, key="ms_region_pers")

    reg_display = regional_stats.copy()
    reg_display["Costo Totale"] = reg_display["Costo Totale"].apply(format_currency)
    reg_display["Personale Totale"] = reg_display["Personale Totale"].apply(lambda x: f"{x:,.0f}")
    st.dataframe(reg_display, use_container_width=True, hide_index=True)

    st.markdown("---")

    # === ANALISI PER ORGANIZZAZIONE ===
    st.markdown("## Analisi per Organizzazione")
    org_stats = create_organization_analysis(df)

    # Sunburst full-width
    st.plotly_chart(chart_org_sunburst(df), use_container_width=True, key="ms_sunburst")

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(chart_missions_by_org(org_stats), use_container_width=True, key="ms_org_missions")
        st.plotly_chart(chart_personnel_pie_by_org(org_stats), use_container_width=True, key="ms_org_personnel")
    with col2:
        st.plotly_chart(chart_mil_civ_by_org(org_stats), use_container_width=True, key="ms_org_mil_civ")
        st.plotly_chart(chart_cost_by_org(org_stats), use_container_width=True, key="ms_org_cost")

    org_display = org_stats.copy()
    org_display["Costo Totale"] = org_display["Costo Totale"].apply(format_currency)
    for c in ["Personale Totale", "Personale Militare", "Personale Civile"]:
        org_display[c] = org_display[c].apply(lambda x: f"{x:,.0f}")
    st.dataframe(org_display, use_container_width=True, hide_index=True)

    st.markdown("---")

    # === ANALISI PER COMMITMENT ===
    st.markdown("## Analisi per Tipo di Commitment")
    commit_stats = create_commitment_analysis(df)

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(chart_missions_by_commitment(commit_stats), use_container_width=True, key="ms_commit_missions")
        st.plotly_chart(chart_commitment_pie(commit_stats), use_container_width=True, key="ms_commit_pie")
    with col2:
        st.plotly_chart(chart_personnel_by_commitment(commit_stats), use_container_width=True, key="ms_commit_pers")
        st.plotly_chart(chart_cost_by_commitment(commit_stats), use_container_width=True, key="ms_commit_cost")

    commit_display = commit_stats.copy()
    commit_display["Costo Totale"] = commit_display["Costo Totale"].apply(format_currency)
    commit_display["Personale Totale"] = commit_display["Personale Totale"].apply(lambda x: f"{x:,.0f}")
    st.dataframe(commit_display, use_container_width=True, hide_index=True)

    st.markdown("""
    <div style="background-color: #e3f2fd; padding: 1rem; border-radius: 0.5rem; border-left: 4px solid #2196f3; margin: 1rem 0;">
        <strong>🎯 Classificazione Commitment:</strong><br>
        • <strong>Head of Mission:</strong> Missioni con personale principalmente civile o di supporto<br>
        • <strong>Troops:</strong> Missioni con significativo dispiegamento di forze militari
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    # === COMMITMENT DETTAGLIATO ===
    st.markdown("## Commitment Dettagliato per Missione")
    df_commit_detail = create_commitment_detailed(df)
    st.dataframe(df_commit_detail, use_container_width=True, hide_index=True)
    st.plotly_chart(chart_commitment_detailed_bar(df_commit_detail), use_container_width=True, key="ms_commit_detail_bar")

    st.markdown("---")

    # === TABELLA COMPLETA ===
    st.markdown("## Dati Completi delle Missioni")
    _render_data_table(df)

    st.markdown("---")

    # === ESPORTAZIONE ===
    st.markdown("## Esportazione Dati")
    _render_export(df, org_stats, commit_stats, regional_stats)


def _render_data_table(df: pd.DataFrame) -> None:
    """Renderizza la tabella dati completa."""
    df_display = df.copy()
    df_display["costo_fmt"] = df_display["costo_totale"].apply(format_currency)
    df_display["inizio_fmt"] = df_display["data_inizio"].dt.strftime("%Y-%m-%d")
    df_display["fine_fmt"] = df_display["data_fine"].dt.strftime("%Y-%m-%d")
    df_display["personale_fmt"] = df_display["personale_totale"].apply(lambda x: f"{x:,.0f}")

    cols = ["nome", "paese", "regione", "sub_regione", "tipo_partecipazione",
            "inizio_fmt", "fine_fmt", "personale_fmt", "costo_fmt", "tipo_missione"]
    display_names = ["Missione", "Paese", "Regione", "Sub-Regione", "Tipo Partecipazione",
                     "Data Inizio", "Data Fine", "Personale Totale", "Costo Totale", "Tipo Missione"]

    existing_cols = [c for c in cols if c in df_display.columns]
    existing_names = [display_names[i] for i, c in enumerate(cols) if c in df_display.columns]

    df_show = df_display[existing_cols].copy()
    df_show.columns = existing_names
    st.dataframe(df_show, use_container_width=True)


def _render_export(df: pd.DataFrame, org_stats: pd.DataFrame,
                   commit_stats: pd.DataFrame, regional_stats: pd.DataFrame) -> None:
    """Renderizza i pulsanti di esportazione."""
    col1, col2, col3 = st.columns(3)

    with col1:
        csv_data = df.to_csv(index=False, encoding="utf-8")
        st.download_button(
            label="📄 Scarica CSV",
            data=csv_data,
            file_name=f"missioni_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )

    with col2:
        try:
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name="Missioni", index=False)
                org_stats.to_excel(writer, sheet_name="Organizzazioni", index=False)
                commit_stats.to_excel(writer, sheet_name="Commitment", index=False)
                regional_stats.to_excel(writer, sheet_name="Regioni", index=False)
            buffer.seek(0)
            st.download_button(
                label="📊 Scarica Excel",
                data=buffer.getvalue(),
                file_name=f"missioni_analisi_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except ImportError:
            st.warning("Per l'esportazione Excel, installa: pip install openpyxl")

    with col3:
        try:
            from dashboard.pdf_report import generate_report
            if st.button("📕 Genera PDF", key="gen_pdf"):
                with st.spinner("Generazione report PDF..."):
                    pdf_bytes = bytes(generate_report(df))
                    st.download_button(
                        label="⬇️ Scarica PDF",
                        data=pdf_bytes,
                        file_name=f"MIDA_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                        mime="application/pdf",
                    )
        except ImportError:
            st.warning("Per l'esportazione PDF, installa: pip install fpdf2")
