"""Streamlit dashboard for the dependency-free Gold CSV marts."""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="StreamPulse", page_icon="▶", layout="wide")

gold_dir = Path(os.getenv("GOLD_DIR", "output/gold"))
content_path = gold_dir / "content_daily.csv"
platform_path = gold_dir / "platform_hourly.csv"

st.title("StreamPulse playback analytics")
st.caption("Engagement, reliability, and quality-of-experience signals from playback telemetry")

if not content_path.exists() or not platform_path.exists():
    st.info("Run `make demo` from the project root to build the Gold marts.")
    st.stop()

content = pd.read_csv(content_path)
platform = pd.read_csv(platform_path)

country_options = sorted(content["country"].dropna().unique())
device_options = sorted(content["device_type"].dropna().unique())
selected_countries = st.sidebar.multiselect("Country", country_options, default=country_options)
selected_devices = st.sidebar.multiselect("Device", device_options, default=device_options)

filtered = content[
    content["country"].isin(selected_countries) & content["device_type"].isin(selected_devices)
]

plays = int(filtered["plays"].sum())
watch_hours = float(filtered["watch_hours"].sum())
weighted_qoe = (
    (filtered["qoe_score"] * filtered["event_count"]).sum() / filtered["event_count"].sum()
    if filtered["event_count"].sum()
    else 0.0
)
errors = int(filtered["playback_errors"].sum())

metric_columns = st.columns(4)
metric_columns[0].metric("Playback starts", f"{plays:,}")
metric_columns[1].metric("Watch hours", f"{watch_hours:,.1f}")
metric_columns[2].metric("Weighted QoE", f"{weighted_qoe:,.1f} / 100")
metric_columns[3].metric("Playback errors", f"{errors:,}")

left, right = st.columns(2)
with left:
    st.subheader("Top content by watch hours")
    top_content = (
        filtered.groupby("content_title", as_index=False)["watch_hours"]
        .sum()
        .sort_values("watch_hours", ascending=False)
        .head(10)
        .set_index("content_title")
    )
    st.bar_chart(top_content)

with right:
    st.subheader("QoE by device")
    device_qoe = (
        filtered.groupby("device_type", as_index=False)
        .apply(
            lambda group: pd.Series(
                {
                    "qoe_score": (
                        (group["qoe_score"] * group["event_count"]).sum()
                        / group["event_count"].sum()
                    )
                }
            ),
            include_groups=False,
        )
        .set_index("device_type")
    )
    st.bar_chart(device_qoe)

st.subheader("Hourly platform health")
platform["window_start"] = pd.to_datetime(platform["window_start"])
health = (
    platform[platform["country"].isin(selected_countries)]
    .groupby("window_start", as_index=False)
    .agg(qoe_score=("qoe_score", "mean"), error_rate=("error_rate", "mean"))
    .set_index("window_start")
)
st.line_chart(health)

with st.expander("Gold content mart"):
    st.dataframe(filtered.sort_values("watch_hours", ascending=False), use_container_width=True)

