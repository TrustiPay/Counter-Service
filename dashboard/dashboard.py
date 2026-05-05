import time
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

st.set_page_config(
    page_title="TrustiPay Counter Service",
    page_icon="💳",
    layout="wide",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("⚙️ Settings")
    counter_url = st.text_input("Counter Service URL", value="http://localhost:8000")
    refresh_interval = st.slider("Refresh interval (s)", min_value=2, max_value=30, value=5)
    st.divider()
    if st.button("🔄 Refresh now"):
        st.rerun()

# ── Data fetching ─────────────────────────────────────────────────────────────

STATUS_COLORS = {
    "APPROVED":      "#d4edda",
    "REJECTED":      "#f8d7da",
    "FRAUD_PENDING": "#fff3cd",
    "OTP_PENDING":   "#cce5ff",
    "PENDING":       "#e2e3e5",
    "Processing":    "#e2d9f3",
}


def fetch(path: str):
    try:
        r = requests.get(f"{counter_url}{path}", timeout=4)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


# ── Page title + last-updated stamp ──────────────────────────────────────────

st.title("💳 TrustiPay — Counter Service Dashboard")
last_updated = st.empty()

placeholder = st.empty()

# ── Auto-refresh loop ─────────────────────────────────────────────────────────

while True:
    health = fetch("/health")
    status = fetch("/counter/status")
    ledger = fetch("/ledger-data")

    last_updated.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    with placeholder.container():

        # ── Metrics row ───────────────────────────────────────────────────────
        st.subheader("Service Health")
        m1, m2, m3, m4 = st.columns(4)

        service_state = "🟢 Online" if health else "🔴 Offline"
        m1.metric("Counter Service", service_state)

        tx_q = status["tx_queue_size"] if status else "—"
        m2.metric("Offline TX Queue", tx_q, help="Transactions fetched from Firebase awaiting validation")

        fraud_q = status["fraud_queue_size"] if status else "—"
        m3.metric("Fraud Check Queue", fraud_q, help="Validated transactions waiting for fraud model")

        total_ledger = len(ledger) if isinstance(ledger, list) else "—"
        m4.metric("Total Ledger Entries", total_ledger)

        st.divider()

        # ── Active fraud check ────────────────────────────────────────────────
        st.subheader("🔍 Active Fraud Check")

        current = status.get("current_fraud_task") if status else None
        if current:
            fc1, fc2, fc3, fc4 = st.columns(4)
            fc1.metric("Transaction ID", current.get("tx_id", "—"))
            fc2.metric("Sender ID", current.get("sender_id", "—"))
            fc3.metric("Amount", f"${current.get('amount', 0):,.2f}")
            elapsed = current.get("elapsed_seconds", 0)
            fc4.metric("Elapsed", f"{elapsed}s")

            st.progress(min(elapsed / 30.0, 1.0), text="Fraud model processing…")
        else:
            st.info("No fraud check in progress — worker is idle.")

        st.divider()

        # ── Ledger transactions table ─────────────────────────────────────────
        st.subheader("📊 Ledger Transactions")

        if isinstance(ledger, list) and ledger:
            df = pd.DataFrame(ledger)

            col_filter, col_search = st.columns([2, 3])

            with col_filter:
                if "status" in df.columns:
                    options = ["All"] + sorted(df["status"].dropna().unique().tolist())
                    selected_status = st.selectbox("Filter by status", options, key="status_filter")
                    if selected_status != "All":
                        df = df[df["status"] == selected_status]

            with col_search:
                search = st.text_input("Search by TX ID or sender/receiver", key="search")
                if search:
                    mask = pd.Series(False, index=df.index)
                    for col in ("tx_id", "sender_id", "receiver_id"):
                        if col in df.columns:
                            mask |= df[col].astype(str).str.contains(search, case=False, na=False)
                    df = df[mask]

            # Status badge column
            if "status" in df.columns:
                def badge(val):
                    color = STATUS_COLORS.get(val, "#f0f0f0")
                    return f"background-color: {color}"

                styled = df.style.map(badge, subset=["status"])
            else:
                styled = df.style

            st.dataframe(styled, use_container_width=True, hide_index=True)

            # Status breakdown chart
            if "status" in df.columns and len(df) > 0:
                with st.expander("📈 Status breakdown"):
                    counts = (
                        pd.DataFrame(ledger)["status"]
                        .value_counts()
                        .reset_index()
                        .rename(columns={"index": "status", "count": "count", "status": "Status", 0: "Count"})
                    )
                    st.bar_chart(counts.set_index("Status"))

        elif ledger is not None:
            st.info("Ledger is empty — no transactions recorded yet.")
        else:
            st.error("Could not reach the ledger service. Check that Counter Service is running and reachable.")

    time.sleep(refresh_interval)
