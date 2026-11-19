
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from deltalake import DeltaTable

ROOT = Path(__file__).resolve().parents[1]
DELTA_DIR = ROOT / "data" / "delta"

st.set_page_config(
    page_title="Production Monitoring",
    page_icon="🏭",
    layout="wide",
)

st.title("🏭 Production Monitoring")
st.caption(
    "Supervision quasi temps réel | Kafka · Spark · Delta Lake"
)

# Actualisation automatique de l'interface
@st.fragment(run_every="10s")
def dashboard():
    try:
        gold = DeltaTable(
            str(DELTA_DIR / "gold")
        ).to_pandas()

        silver = DeltaTable(
            str(DELTA_DIR / "silver")
        ).to_pandas()

    except Exception as exc:
        st.error(f"Impossible de lire les tables Delta : {exc}")
        st.info(
            "Vérifiez que Bronze, Silver et Gold "
            "ont été générées."
        )
        return

    if gold.empty or silver.empty:
        st.warning("Aucune donnée disponible.")
        return

    # KPI globaux
    total_events = int(gold["total_events"].sum())
    total_anomalies = int(gold["total_anomalies"].sum())
    anomaly_rate = (
        100 * total_anomalies / total_events
        if total_events else 0
    )

    c1, c2, c3, c4 = st.columns(4)

    c1.metric("Événements", f"{total_events:,}")
    c2.metric("Anomalies", total_anomalies)
    c3.metric("Taux d'anomalies", f"{anomaly_rate:.2f} %")
    c4.metric("Machines", gold["machine_id"].nunique())

    st.divider()

    left, right = st.columns(2)

    with left:
        st.subheader("Anomalies par machine")
        fig = px.bar(
            gold.sort_values("machine_id"),
            x="machine_id",
            y="total_anomalies",
            color="machine_id",
            labels={
                "machine_id": "Machine",
                "total_anomalies": "Nombre d'anomalies",
            },
        )
        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.subheader("Température moyenne")
        fig = px.bar(
            gold.sort_values("machine_id"),
            x="machine_id",
            y="avg_temperature",
            color="machine_id",
            labels={
                "machine_id": "Machine",
                "avg_temperature": "Température moyenne (°C)",
            },
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Indicateurs par machine")
    columns = [
        "machine_id",
        "machine_type",
        "total_events",
        "avg_temperature",
        "avg_vibration",
        "total_anomalies",
        "anomaly_rate",
        "total_defects",
        "avg_production_rate",
    ]
    st.dataframe(
        gold[columns].sort_values("machine_id"),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    st.subheader("Alertes récentes")

    anomalies = silver[
        silver["is_anomaly"] == True
    ].copy()

    if anomalies.empty:
        st.success("Aucune anomalie détectée.")
    else:
        anomalies = anomalies.sort_values(
            "event_timestamp",
            ascending=False,
        )

        st.dataframe(
            anomalies[
                [
                    "event_timestamp",
                    "machine_id",
                    "temperature",
                    "vibration",
                    "anomaly_type",
                ]
            ].head(20),
            use_container_width=True,
            hide_index=True,
        )

    st.caption(
        "Actualisation toutes les 10 secondes · "
        "Données simulées"
    )


dashboard()
