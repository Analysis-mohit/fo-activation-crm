#!/usr/bin/env python
# coding: utf-8
"""
FO Activation CRM — Streamlit Dashboard
Wheelseye · FO Growth Cx POD
────────────────────────────────────────────────────────────
Replaces the HTML CRM with a full Streamlit app.
Data source: /home/ubuntu/fo_activation.db (written by pipeline)
Live hot signals: pulled from Redshift every refresh.

Run: streamlit run streamlit_crm.py --server.port 8501
"""
import sqlite3
import re
import time
import datetime
import warnings
import threading

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import redshift_connector

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# 0. CONFIG
# ─────────────────────────────────────────────
# ── Credentials: local vs Streamlit Cloud ────────────────────────────────────
# On Streamlit Cloud, add secrets at: Settings → Secrets (TOML format):
#   [redshift]
#   host = "redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com"
#   port = 5439
#   database = "datalake"
#   user = "pnkj"
#   password = "10Pnkj29"
# ─────────────────────────────────────────────────────────────────────────────
import os

DB_PATH = "/home/ubuntu/fo_activation.db"   # local server only

def _get_rs_params():
    """Return Redshift connection params — from st.secrets on Cloud, env vars locally."""
    try:
        sec = st.secrets["redshift"]
        return dict(host=sec["host"], port=int(sec["port"]),
                    database=sec["database"], user=sec["user"], password=sec["password"])
    except Exception:
        # Fallback to hardcoded (local dev / server)
        return dict(
            host="redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com",
            port=5439, database="datalake", user="pnkj", password="10Pnkj29"
        )

try:
    REDSHIFT_PARAMS = _get_rs_params()
except Exception:
    REDSHIFT_PARAMS = dict(
        host="redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com",
        port=5439, database="datalake", user="pnkj", password="10Pnkj29"
    )
PAGE_SIZE = 50

# ─────────────────────────────────────────────
# 1. STREAMLIT PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="FO Activation CRM — Wheelseye",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# 2. CSS — PulsePoint-style glassmorphism
# ─────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

/* Main header */
.main-header {
    font-size: 2.2rem; font-weight: 700; text-align: center;
    background: linear-gradient(135deg, #534AB7 0%, #3C3489 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    margin-bottom: 1rem;
}

/* Metric cards */
.metric-card {
    background: linear-gradient(135deg, #534AB7 0%, #3C3489 100%);
    border-radius: 16px; padding: 1.2rem; color: white;
    text-align: center; margin: 0.3rem 0;
    box-shadow: 0 8px 24px rgba(83,74,183,.3);
}
.metric-card h3 { font-size: 0.8rem; opacity: 0.85; margin: 0 0 6px 0; }
.metric-card h2 { font-size: 1.8rem; font-weight: 800; margin: 0; }

.metric-hot   { background: linear-gradient(135deg, #C2500A 0%, #E06020 100%); box-shadow: 0 8px 24px rgba(194,80,10,.3); }
.metric-p0    { background: linear-gradient(135deg, #A32D2D 0%, #C23E3E 100%); box-shadow: 0 8px 24px rgba(163,45,45,.3); }
.metric-green { background: linear-gradient(135deg, #3B6D11 0%, #56A018 100%); box-shadow: 0 8px 24px rgba(59,109,17,.3); }
.metric-gray  { background: linear-gradient(135deg, #5F5E5A 0%, #7A7975 100%); box-shadow: 0 8px 24px rgba(95,94,90,.3); }

/* Section headers */
.section-header {
    font-size: 1.3rem; font-weight: 600; color: #1A1A18;
    margin: 1.5rem 0 0.8rem 0; padding-bottom: 0.4rem;
    border-bottom: 3px solid #534AB7;
}

/* Operator row cards */
.op-card {
    background: white; border-radius: 10px; padding: 10px 14px;
    margin: 4px 0; border-left: 4px solid #E0DFDB;
    box-shadow: 0 2px 8px rgba(0,0,0,.06); cursor: pointer;
    transition: all .15s;
}
.op-card:hover { box-shadow: 0 4px 16px rgba(83,74,183,.15); }
.op-card.hot   { border-left-color: #C2500A; background: #FFF8F2; }
.op-card.p0    { border-left-color: #A32D2D; }
.op-card.p1    { border-left-color: #534AB7; }

/* Badge pills */
.badge {
    display: inline-block; padding: 2px 8px; border-radius: 10px;
    font-size: 10px; font-weight: 700; margin-right: 3px;
}
.badge-p0  { background: #FCEBEB; color: #A32D2D; }
.badge-p1  { background: #EEEDFE; color: #534AB7; }
.badge-p2  { background: #FAEEDA; color: #854F0B; }
.badge-p3  { background: #F1EFE8; color: #5F5E5A; }
.badge-hot { background: #FFF0E6; color: #C2500A; }
.badge-va  { background: #EAF3DE; color: #3B6D11;  }
.badge-rec7 { background: #EAF3DE; color: #3B6D11; }
.badge-score { background: #EEEDFE; color: #3C3489; }

/* Profile sections */
.pcc-card {
    border-radius: 14px; padding: 1rem 1.2rem; border: 1px solid;
    display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem;
}
.pcc-p0 { background: #FCEBEB; border-color: #E8BEBE; }
.pcc-p1 { background: #EEEDFE; border-color: #AFA9EC; }
.pcc-p2 { background: #FAEEDA; border-color: #EFD4A0; }
.pcc-p3 { background: #F1EFE8; border-color: #C0BEB6; }

/* Hot banner */
.hot-banner {
    background: linear-gradient(135deg, #FFF8F2 0%, #FFF0E0 100%);
    border: 1.5px solid #D46B0A; border-radius: 14px;
    padding: 0.875rem 1.125rem; margin-bottom: 1rem;
}
.hot-banner-title { font-size: 11px; font-weight: 800; color: #7A3006;
    letter-spacing: .5px; text-transform: uppercase; }

/* Bid time alert */
.bid-time-alert {
    background: #FFF3E5; border-radius: 8px; padding: 8px 12px;
    margin-bottom: 8px; font-size: 12px; font-weight: 700; color: #C2500A;
}

/* Action banner */
.action-warm { background: #FAEEDA; border: 1px solid #EF9F27; border-radius: 12px; padding: 0.8rem; }
.action-cold { background: #F1EFE8; border: 1px solid #C0BEB6; border-radius: 12px; padding: 0.8rem; }
.action-ok   { background: #EEEDFE; border: 1px solid #AFA9EC; border-radius: 12px; padding: 0.8rem; }

/* Stbutton override */
.stButton>button {
    background: linear-gradient(135deg, #534AB7 0%, #3C3489 100%);
    color: white; border: none; border-radius: 20px;
    padding: 0.4rem 1.2rem; font-weight: 600;
    box-shadow: 0 3px 12px rgba(83,74,183,.3);
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# 3. DATA LOADING
# ─────────────────────────────────────────────
@st.cache_data(ttl=300)  # cache for 5 min
def load_base_data():
    """Load base data — SQLite if available (server), else Redshift (Cloud)."""
    import os
    if os.path.exists(DB_PATH):
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql("SELECT * FROM activation_leads ORDER BY calling_rank", conn)
        conn.close()
        return df
    # Streamlit Cloud / no local DB — read from permanent Redshift table
    conn = redshift_connector.connect(**REDSHIFT_PARAMS)
    df = pd.read_sql(
        "SELECT * FROM analytics.fo_activation_crm_leads ORDER BY calling_rank",
        conn
    )
    conn.close()
    return df

@st.cache_data(ttl=180)  # cache for 3 min — hot signals refresh faster
def load_hot_signals():
    """Pull live bid (last 3h) + VA (last 4h) from Redshift."""
    try:
        conn = redshift_connector.connect(**REDSHIFT_PARAMS)
        bid_df = pd.read_sql("""
            SELECT operator_code,
                   MAX((created + interval '5h30m')) AS last_bid_time
            FROM wfms_operator_demand_token
            WHERE created >= GETDATE() - interval '3 hours'
              AND json_extract_path_text(metadata,'opFreight') <> ''
              AND json_extract_path_text(metadata,'opFreight')::integer > 0
            GROUP BY 1
        """, conn)
        va_df = pd.read_sql("""
            SELECT DISTINCT operator_code
            FROM apollo_vehicle_availability
            WHERE updated_at >= GETDATE() - interval '4 hours'
        """, conn)
        conn.close()
        return (
            set(bid_df["operator_code"]),
            set(va_df["operator_code"]),
            dict(zip(bid_df["operator_code"], bid_df["last_bid_time"].astype(str)))
        )
    except Exception as e:
        st.warning(f"Could not refresh live signals: {e}")
        return set(), set(), {}

def enrich_with_hot(df):
    """Overlay live hot signals on base data."""
    bid_set, va_set, bid_ts = load_hot_signals()
    df = df.copy()
    df["bid_today"]               = df["operator_code"].isin(bid_set)
    df["vehicle_available_today"] = df["operator_code"].isin(va_set)
    df["last_bid_time"]           = df["operator_code"].map(bid_ts).fillna("")

    _EPOCH = pd.Timestamp("2000-01-01")
    def _parse(x):
        s = str(x)[:19] if x else ""
        try: return pd.Timestamp(s) if s not in ("","nan","NaT","None") else _EPOCH
        except: return _EPOCH

    df["_bid_ts"]   = df["operator_code"].map(bid_ts).apply(_parse)
    df["_va_ts"]    = df["vehicle_available_today"].map(lambda x: pd.Timestamp("now") if x else _EPOCH)
    df["_hot_ts"]   = df[["_bid_ts","_va_ts"]].max(axis=1)
    df["_hot_sort"] = df["bid_today"].astype(int)*2 + df["vehicle_available_today"].astype(int)

    df = df.sort_values(["_hot_sort","_hot_ts","activation_score"],
                        ascending=[False, False, False]).reset_index(drop=True)
    df["calling_rank"] = range(1, len(df)+1)
    df.drop(columns=["_bid_ts","_va_ts","_hot_ts","_hot_sort"], inplace=True)
    return df

# ─────────────────────────────────────────────
# 4. HELPERS
# ─────────────────────────────────────────────
def rs(v):
    try: n = float(str(v).replace(",",""))
    except: return None
    return None if (np.isnan(n) or n <= 0) else n

def frs(n):
    if n is None: return "—"
    return f"₹{int(n):,}"

def mins_ago(ts_str):
    if not ts_str or ts_str in ("nan","NaT","None",""): return ""
    try:
        ts = pd.Timestamp(str(ts_str)[:19])
        diff = (pd.Timestamp("now") - ts).total_seconds() / 60
        if diff < 0 or diff > 200: return ""
        if diff < 1: return "just now"
        return f"{int(diff)} min ago"
    except: return ""

def parse_lanes(raw):
    if not raw: return []
    rows = []
    for seg in str(raw).split("||||"):
        seg = seg.strip()
        if not seg: continue
        p = seg.split("####")
        did = p[0].strip() if p else ""
        if not did: continue
        lows_raw = p[4].split(";") if len(p) > 4 else []
        lows = [rs(x) for x in lows_raw if rs(x)]
        rows.append({
            "did": did, "route": p[1].strip() if len(p) > 1 else "",
            "truck": p[2].strip() if len(p) > 2 else "",
            "loading": p[3].strip() if len(p) > 3 else "",
            "lows": lows, "bid": lows[0] if lows else None,
            "offered": rs(p[5]) if len(p) > 5 else None,
            "own": rs(p[6]) if len(p) > 6 else None,
        })
    return rows

def parse_token_lanes(raw):
    if not raw: return []
    rows = []
    for seg in str(raw).split("||||"):
        seg = seg.strip()
        if not seg: continue
        p = seg.split("####")
        did = p[0].strip() if p else ""
        if not did: continue
        rows.append({
            "did": did, "route": p[1].strip() if len(p) > 1 else "",
            "truck": p[2].strip() if len(p) > 2 else "",
            "bid": rs(p[3]) if len(p) > 3 else None,
            "placed": rs(p[4]) if len(p) > 4 else None,
        })
    return rows

def parse_bid_today(raw):
    if not raw: return []
    rows = []
    for seg in str(raw).split("||||"):
        seg = seg.strip()
        if not seg: continue
        p = seg.split("####")
        did = p[0].strip() if p else ""
        if not did: continue
        rows.append({
            "did": did, "route": p[1].strip() if len(p) > 1 else "",
            "truck": p[2].strip() if len(p) > 2 else "",
            "bid": rs(p[3]) if len(p) > 3 else None,
            "token_paid": p[4].strip() == "1" if len(p) > 4 else False,
        })
    return rows

def parse_avail(raw):
    if not raw: return []
    rows = []
    for seg in str(raw).split("||||"):
        seg = seg.strip()
        if not seg: continue
        p = seg.split("####")
        vn = p[0].strip() if p else ""
        if not vn: continue
        rows.append({
            "vn": vn,
            "tyre": p[1].strip() if len(p) > 1 else "",
            "body": p[2].strip() if len(p) > 2 else "",
            "size": p[3].strip() if len(p) > 3 else "",
        })
    return rows

def parse_hist(raw):
    if not raw: return []
    rows = []
    for seg in str(raw).split("||||"):
        seg = seg.strip()
        if not seg: continue
        p = seg.split("####")
        did = p[0].strip() if p else ""
        if not did: continue
        rows.append({
            "did": did, "route": p[1].strip() if len(p) > 1 else "",
            "truck": p[2].strip() if len(p) > 2 else "",
            "bid": rs(p[3]) if len(p) > 3 else None,
            "placed": rs(p[4]) if len(p) > 4 else None,
            "verdict": p[5].strip() if len(p) > 5 else "",
            "date": p[6].strip() if len(p) > 6 else "",
        })
    return rows

def parse_call_hist(raw):
    if not raw: return []
    rows = []
    for seg in str(raw).split("||||"):
        seg = seg.strip()
        if not seg: continue
        p = seg.split("####")
        rows.append({
            "date": p[0].strip() if p else "",
            "disposition": p[1].strip() if len(p) > 1 else "",
            "caller": p[2].strip() if len(p) > 2 else "",
            "code": p[3].strip() if len(p) > 3 else "",
        })
    return rows

def pri_color(p):
    return {"P0":"#A32D2D","P1":"#534AB7","P2":"#854F0B","P3":"#5F5E5A"}.get(p,"#5F5E5A")

def istr(v):
    return str(v).lower() in ("true","1","yes")

# ─────────────────────────────────────────────
# 5. PAGE: LEAD LIST (calling queue)
# ─────────────────────────────────────────────
def page_lead_list(df):
    st.markdown('<div class="main-header">🚛 FO Activation CRM</div>', unsafe_allow_html=True)
    st.caption(f"Wheelseye · FO Growth · {len(df):,} operators · hot refreshed live")

    # ── FILTERS ──────────────────────────────────────────────────────
    with st.sidebar:
        st.header("🎛️ Filters")

        flt_hot = st.toggle("🔴 Hot Today Only", key="flt_hot")
        flt_pri = st.multiselect("Priority", ["P0","P1","P2","P3"],
                                 default=[], key="flt_pri")
        flt_rec = st.selectbox("Recency", ["All","Last 7 days","8-30 days","30+ days"],
                               key="flt_rec")
        flt_dr  = st.selectbox("DR Tier", ["All","High DR","Mid DR","Low DR"],
                               key="flt_dr")
        rms = ["All"] + sorted(df["rm_name"].dropna().unique().tolist())
        flt_rm  = st.selectbox("RM", rms, key="flt_rm")
        flt_called = st.selectbox("Call status", ["All","Called","Not Called"],
                                  key="flt_called")
        flt_search = st.text_input("Search operator code / RM", key="flt_search")

        st.markdown("---")
        if st.button("🔄 Refresh live signals", key="btn_refresh"):
            st.cache_data.clear()
            st.rerun()

    # ── APPLY FILTERS ────────────────────────────────────────────────
    fdf = df.copy()
    if flt_hot:
        fdf = fdf[istr(fdf["bid_today"]) | istr(fdf["vehicle_available_today"])]
    if flt_pri:
        fdf = fdf[fdf["priority"].isin(flt_pri)]
    if flt_rec != "All":
        fdf = fdf[fdf["recency_tier"] == flt_rec]
    if flt_dr != "All":
        fdf = fdf[fdf["dr_tier"] == flt_dr]
    if flt_rm != "All":
        fdf = fdf[fdf["rm_name"].str.upper() == flt_rm.upper()]
    if flt_called == "Called":
        fdf = fdf[fdf["last_call_date"].ne("") & fdf["last_call_date"].notna()]
    elif flt_called == "Not Called":
        fdf = fdf[fdf["last_call_date"].eq("") | fdf["last_call_date"].isna()]
    if flt_search:
        q = flt_search.upper()
        fdf = fdf[fdf["operator_code"].str.upper().str.contains(q) |
                  fdf["rm_name"].str.upper().str.contains(q)]

    # ── METRIC STRIP ─────────────────────────────────────────────────
    hot_n   = int(istr(fdf["bid_today"]).sum() | istr(fdf["vehicle_available_today"]).sum()) if len(fdf) else 0
    hot_n   = int((istr(fdf["bid_today"]) | istr(fdf["vehicle_available_today"])).sum())
    p0_n    = int((fdf["priority"] == "P0").sum())
    avg_sc  = fdf["activation_score"].apply(lambda x: int(x) if str(x).isdigit() else 0).mean()

    c1,c2,c3,c4 = st.columns(4)
    c1.markdown(f'<div class="metric-card"><h3>Total leads</h3><h2>{len(fdf):,}</h2></div>', unsafe_allow_html=True)
    c2.markdown(f'<div class="metric-card metric-hot"><h3>🔥 Hot Today</h3><h2>{hot_n:,}</h2></div>', unsafe_allow_html=True)
    c3.markdown(f'<div class="metric-card metric-p0"><h3>P0 operators</h3><h2>{p0_n:,}</h2></div>', unsafe_allow_html=True)
    c4.markdown(f'<div class="metric-card metric-gray"><h3>Avg Score</h3><h2>{avg_sc:.0f}</h2></div>', unsafe_allow_html=True)

    st.markdown("---")

    # ── LEAD TABLE ───────────────────────────────────────────────────
    st.markdown(f"<div class='section-header'>📋 Call Queue — {len(fdf):,} leads</div>",
                unsafe_allow_html=True)

    # Pagination
    page = st.session_state.get("lead_page", 0)
    total_pages = max(1, (len(fdf) - 1) // PAGE_SIZE + 1)

    pc1, pc2, pc3 = st.columns([1, 3, 1])
    with pc1:
        if st.button("← Prev", disabled=(page == 0), key="btn_prev"):
            st.session_state["lead_page"] = max(0, page - 1)
            st.rerun()
    with pc2:
        st.caption(f"Page {page+1} / {total_pages}  ·  rows {page*PAGE_SIZE+1}–{min((page+1)*PAGE_SIZE, len(fdf))}")
    with pc3:
        if st.button("Next →", disabled=(page >= total_pages-1), key="btn_next"):
            st.session_state["lead_page"] = min(total_pages-1, page+1)
            st.rerun()

    page_df = fdf.iloc[page*PAGE_SIZE:(page+1)*PAGE_SIZE]

    pri_cls = {"P0":"badge-p0","P1":"badge-p1","P2":"badge-p2","P3":"badge-p3"}
    rec_lbl = {"Last 7 days":"7d","8-30 days":"8-30d","30+ days":"30+d"}

    for _, row in page_df.iterrows():
        bid_t = istr(row.get("bid_today",""))
        va_t  = istr(row.get("vehicle_available_today",""))
        is_hot = bid_t or va_t
        code  = str(row.get("operator_code","")).upper()
        pri   = str(row.get("priority","P3"))
        rec   = str(row.get("recency_tier",""))
        dr    = str(row.get("dr_tier",""))
        rm    = str(row.get("rm_name","")).upper()
        rank  = int(row.get("calling_rank",0))
        sc    = str(int(row.get("activation_score",0))) if str(row.get("activation_score","0")).replace(".","").isdigit() else "0"
        last_call = str(row.get("last_call_date",""))
        last_disp = str(row.get("last_call_disposition",""))
        bid_age   = mins_ago(row.get("last_bid_time",""))

        hot_badge = ""
        if bid_t and va_t: hot_badge = '<span class="badge badge-hot">BID+VA</span>'
        elif bid_t:         hot_badge = '<span class="badge badge-hot">BID</span>'
        elif va_t:          hot_badge = '<span class="badge" style="background:#EAF3DE;color:#3B6D11">VA</span>'
        bid_age_tag = f'<span style="font-size:9px;font-weight:700;color:#C2500A;background:#FFF0E6;padding:1px 5px;border-radius:8px">{bid_age}</span>' if bid_age else ""
        rec_badge = f'<span class="badge badge-rec7">{rec_lbl.get(rec,rec)}</span>' if rec else ""
        dr_badge  = f'<span class="badge" style="background:#E6F1FB;color:#185FA5">{dr}</span>' if dr else ""

        card_cls = "op-card hot" if is_hot else f"op-card {pri.lower()}"
        html = f"""
        <div class="{card_cls}">
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
            <span style="font-size:10px;color:#9A9890;min-width:24px">#{rank}</span>
            <span style="font-size:13px;font-weight:700;font-family:monospace;color:{'#C2500A' if is_hot else '#1A1A18'}">{code}</span>
            {bid_age_tag}
            <span class="badge {pri_cls.get(pri,'badge-p3')}">{pri}</span>
            {hot_badge}{rec_badge}{dr_badge}
            <span style="font-size:10px;color:#9A9890;margin-left:auto">{rm}</span>
            <span class="badge badge-score">#{sc}</span>
          </div>
          {f'<div style="font-size:9px;color:#9A9890;margin-top:3px">📞 {last_call} · {last_disp}</div>' if last_call and last_call not in ("","nan") else ""}
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)

        if st.button(f"Open profile ›", key=f"btn_{code}_{rank}"):
            st.session_state["selected_op"] = code
            st.session_state["view"] = "profile"
            st.rerun()

# ─────────────────────────────────────────────
# 6. PAGE: OPERATOR PROFILE
# ─────────────────────────────────────────────
def page_operator_profile(df):
    code = st.session_state.get("selected_op","")
    row_matches = df[df["operator_code"].str.upper() == code.upper()]
    if row_matches.empty:
        st.error(f"Operator {code} not found. Go back and try again.")
        if st.button("← Back to list"): st.session_state["view"]="list"; st.rerun()
        return
    row = row_matches.iloc[0]

    if st.button("← Back to list", key="profile_back"):
        st.session_state["view"] = "list"
        st.rerun()

    # ── PRIORITY CONTEXT ─────────────────────────────────────────────
    pri = str(row.get("priority","P3"))
    pcc_map = {"P0":"pcc-p0","P1":"pcc-p1","P2":"pcc-p2","P3":"pcc-p3"}
    def_map = {"P0":"High LDP + High Token","P1":"High LDP + No Token",
               "P2":"Low LDP + High Token","P3":"Low LDP + No Token"}
    st.markdown(f"""
    <div class="pcc-card {pcc_map.get(pri,'pcc-p3')}">
      <div style="background:{pri_color(pri)};color:white;padding:4px 14px;border-radius:20px;font-size:14px;font-weight:800">{pri}</div>
      <div style="flex:1">
        <div style="font-size:11px;font-weight:700;color:{pri_color(pri)};margin-bottom:6px">{def_map.get(pri,'')}</div>
        <div style="display:flex;gap:16px;flex-wrap:wrap">
          <div style="text-align:center"><div style="font-size:16px;font-weight:700">{int(row.get('lifetime_ldps',0)):,}</div><div style="font-size:9px;color:#9A9890">Lifetime LDPs</div></div>
          <div style="text-align:center"><div style="font-size:16px;font-weight:700">{int(row.get('lifetime_tokens',0)):,}</div><div style="font-size:9px;color:#9A9890">Lifetime Tokens</div></div>
          <div style="text-align:center"><div style="font-size:16px;font-weight:700">{int(row.get('appeared_in_drs',0)):,}</div><div style="font-size:9px;color:#9A9890">DR Appearances</div></div>
          <div style="text-align:center"><div style="font-size:16px;font-weight:700">{int(row.get('lifetime_trips',0))}</div><div style="font-size:9px;color:#9A9890">Lifetime Trips</div></div>
        </div>
      </div>
      <div style="text-align:right">
        <div style="font-size:10px;color:#9A9890;font-family:monospace">Rank #{int(row.get('calling_rank',0))}</div>
        <div style="font-size:10px;font-weight:600;color:#534AB7">Score: {int(row.get('activation_score',0))}</div>
        <div style="font-size:10px;color:#5F5E5A">{str(row.get('recency_tier',''))}</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── HOT TODAY ────────────────────────────────────────────────────
    bid_t = istr(row.get("bid_today",""))
    va_t  = istr(row.get("vehicle_available_today",""))
    if bid_t or va_t:
        parts = []
        if bid_t: parts.append("Bid (last 3h)")
        if va_t:  parts.append("VA (last 4h)")
        bid_age = mins_ago(str(row.get("last_bid_time","")))

        st.markdown(f"""
        <div class="hot-banner">
          <div class="hot-banner-title">🔥 Active Today — {' + '.join(parts)}</div>
        </div>
        """, unsafe_allow_html=True)

        if bid_t and bid_age:
            st.markdown(f'<div class="bid-time-alert">⏱ Last bid: <b>{bid_age}</b> — push for token payment now!</div>',
                        unsafe_allow_html=True)

        hc1, hc2 = st.columns(2)
        if bid_t:
            with hc1:
                st.markdown("**🔴 Bid Today (last 3h)**")
                bt_lanes = parse_bid_today(str(row.get("bid_today_lane_details","")))
                if bt_lanes:
                    for l in bt_lanes[:3]:
                        paid_tag = " ✓ Paid" if l.get("token_paid") else ""
                        st.markdown(f"• **{l['route'] or l['did']}** {f'· {frs(l[\"bid\"])}' if l.get('bid') else ''}{paid_tag}")
                else:
                    st.caption("Rate entered — demand details loading")
        if va_t:
            with hc2:
                st.markdown("**🟡 Vehicle Available (last 4h)**")
                avail = parse_avail(str(row.get("avail_vehicles","")))
                if avail:
                    for v in avail[:4]:
                        spec = " · ".join(filter(None, [f"{v['tyre']}T" if v.get("tyre") else "", v.get("body",""), f"{v['size']}ft" if v.get("size") else ""]))
                        st.markdown(f"• **{v['vn']}** {spec}")
                else:
                    st.caption("Vehicle details loading")

    # ── OPERATOR CARD ────────────────────────────────────────────────
    oc1, oc2 = st.columns([3,1])
    with oc1:
        st.markdown(f"### `{code}`")
        st.caption(f"RM: {str(row.get('rm_name','')).upper()} · Cluster: {str(row.get('Cluster',''))}")
    with oc2:
        sc = int(row.get("activation_score",0))
        sc_color = "#C2500A" if sc >= 100 else "#534AB7" if sc >= 30 else "#5F5E5A"
        st.markdown(f'<div style="text-align:right"><div style="font-size:36px;font-weight:800;color:{sc_color}">{sc}</div><div style="font-size:9px;color:#9A9890;text-transform:uppercase;letter-spacing:.5px">Activity Score · 15d</div></div>', unsafe_allow_html=True)

    # ── ACTION BANNER ────────────────────────────────────────────────
    dr_n  = int(row.get("num_of_drs_in_opsearch",0))
    tok_n = int(row.get("num_of_drs_paid_token",0))
    lanes = parse_lanes(str(row.get("lane_vt_details","")))
    token_set = set(str(row.get("token_demand_ids","")).split(","))

    if dr_n == 0:
        st.markdown('<div class="action-cold">🔍 <b>No live lanes in op-search right now.</b> Ask the operator to mark a vehicle available and set preferred routes, then refresh.</div>', unsafe_allow_html=True)
    elif tok_n == 0:
        open_lanes = [l for l in lanes if l["did"] not in token_set]
        best = sorted([l for l in open_lanes if l.get("bid")], key=lambda x: x["bid"])[0] if any(l.get("bid") for l in open_lanes) else (open_lanes[0] if open_lanes else None)
        pitch = ""
        if best:
            rate = best.get("bid") or best.get("offered")
            pitch = f"Pitch → **{best['route'] or 'Demand #'+best['did']}** · lowest bid {frs(rate)}"
        st.markdown(f'<div class="action-warm">🎯 <b>{dr_n} live lane{"s" if dr_n>1 else ""} · 0 tokens paid today.</b> Push the operator to pay a token now. Lead with the lowest-bid lane.<br>{pitch}</div>', unsafe_allow_html=True)
    else:
        open_n = len([l for l in lanes if l["did"] not in token_set])
        st.markdown(f'<div class="action-ok">✅ <b>{tok_n} token{"s" if tok_n>1 else ""} paid · {open_n} lane{"s" if open_n!=1 else ""} still open.</b> {"Keep pushing — close remaining lanes." if open_n else "All live lanes converted today!"}</div>', unsafe_allow_html=True)

    st.markdown("---")

    # ── ACTIVITY METRICS ─────────────────────────────────────────────
    st.markdown('<div class="section-header">📅 Activity — last 15 days</div>', unsafe_allow_html=True)
    a1,a2,a3,a4 = st.columns(4)
    a1.metric("VA days", int(row.get("va_days",0)), help="Days vehicle was marked available")
    a2.metric("LDP days", int(row.get("ldp_days",0)), help="Days operator browsed loads")
    a3.metric("Total LDPs", int(row.get("total_ldp_count",0)), help="Total load detail page views")
    a4.metric("Tokens paid", int(row.get("token_count",0)), help="Tokens paid in last 15 days")

    # ── DR FUNNEL ────────────────────────────────────────────────────
    st.markdown('<div class="section-header">🔀 DR Funnel</div>', unsafe_allow_html=True)
    fc1,fc2,fc3 = st.columns(3)
    conv = f"{round(tok_n/dr_n*100)}%" if dr_n > 0 else "—"
    fc1.metric("In op-search", dr_n, help="Live loads shown to operator today")
    fc2.metric("Tokens paid", tok_n, help="Tokens converted today")
    fc3.metric("Conversion", conv)

    # Live lanes table
    if lanes:
        with st.expander(f"📋 Live lanes ({len(lanes)})"):
            lane_data = []
            for l in lanes:
                paid = "✓ Paid" if l["did"] in token_set else "Open"
                lane_data.append({
                    "Demand ID": l["did"], "Route": l["route"], "Truck": l["truck"],
                    "Lowest Bid": frs(l["bid"]) if l.get("bid") else (f"~{frs(l['offered'])}" if l.get("offered") else "—"),
                    "Status": paid
                })
            st.dataframe(pd.DataFrame(lane_data), use_container_width=True, hide_index=True)

    # Token paid lanes
    tok_lanes = parse_token_lanes(str(row.get("token_lane_details","")))
    if tok_lanes:
        with st.expander(f"✅ Token paid today ({len(tok_lanes)})"):
            tok_data = [{"Demand ID": l["did"],"Route": l["route"],"Your Bid": frs(l.get("bid")),"Placed Rate": frs(l.get("placed"))} for l in tok_lanes]
            st.dataframe(pd.DataFrame(tok_data), use_container_width=True, hide_index=True)

    # ── VEHICLE ──────────────────────────────────────────────────────
    st.markdown('<div class="section-header">🚛 Vehicle Profile</div>', unsafe_allow_html=True)
    tot_v = int(row.get("total_vehicles",0))
    ver_v = int(row.get("corrected_vehicles",0))
    vc1,vc2,vc3 = st.columns(3)
    vc1.metric("Total trucks", tot_v)
    vc2.metric("Verified", ver_v)
    vc3.metric("Unverified", max(0, tot_v-ver_v))

    avail = parse_avail(str(row.get("avail_vehicles","")))
    if avail:
        st.caption("🟢 Available vehicles:")
        cols = st.columns(min(4, len(avail)))
        for i, v in enumerate(avail[:8]):
            spec = " · ".join(filter(None, [f"{v['tyre']}T" if v.get("tyre") else "", v.get("body",""), f"{v['size']}ft" if v.get("size") else ""]))
            cols[i%4].markdown(f"**{v['vn']}** `{spec}`")

    # ── BID HISTORY ──────────────────────────────────────────────────
    hist = parse_hist(str(row.get("hist_token_details","")))
    if hist:
        with st.expander(f"📜 Bid History ({len(hist)} bids)"):
            hist_data = [{"Date": h["date"],"Demand ID": h["did"],"Route": h["route"],"Truck": h["truck"],"Bid": frs(h.get("bid")),"Placed": frs(h.get("placed")),"Verdict": h["verdict"]} for h in hist[:20]]
            st.dataframe(pd.DataFrame(hist_data), use_container_width=True, hide_index=True)

    # ── CALL HISTORY ─────────────────────────────────────────────────
    calls = parse_call_hist(str(row.get("call_history","")))
    total_calls = int(row.get("total_calls",0))
    if calls:
        with st.expander(f"📞 Call History ({total_calls} total calls)"):
            call_data = [{"Date": c["date"],"Disposition": c["disposition"],"Code": c["code"],"Caller (RM)": c["caller"]} for c in calls]
            st.dataframe(pd.DataFrame(call_data), use_container_width=True, hide_index=True)
    else:
        st.caption("No calls made in MP Load Support campaign.")

# ─────────────────────────────────────────────
# 7. PAGE: ANALYTICS
# ─────────────────────────────────────────────
def page_analytics(df):
    st.markdown('<div class="main-header">📊 Activation Analytics</div>', unsafe_allow_html=True)

    hot_n = int((istr(df["bid_today"]) | istr(df["vehicle_available_today"])).sum())
    p0_n  = int((df["priority"]=="P0").sum())
    p1_n  = int((df["priority"]=="P1").sum())
    p2_n  = int((df["priority"]=="P2").sum())
    p3_n  = int((df["priority"]=="P3").sum())

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.markdown(f'<div class="metric-card"><h3>Total Operators</h3><h2>{len(df):,}</h2></div>', unsafe_allow_html=True)
    c2.markdown(f'<div class="metric-card metric-hot"><h3>🔥 Hot Today</h3><h2>{hot_n:,}</h2></div>', unsafe_allow_html=True)
    c3.markdown(f'<div class="metric-card metric-p0"><h3>P0</h3><h2>{p0_n:,}</h2></div>', unsafe_allow_html=True)
    c4.markdown(f'<div class="metric-card"><h3>P1</h3><h2>{p1_n:,}</h2></div>', unsafe_allow_html=True)
    c5.markdown(f'<div class="metric-card metric-gray"><h3>P2+P3</h3><h2>{p2_n+p3_n:,}</h2></div>', unsafe_allow_html=True)

    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        pri_counts = df["priority"].value_counts().reset_index()
        pri_counts.columns = ["Priority","Count"]
        fig = px.pie(pri_counts, values="Count", names="Priority",
                     title="Priority Distribution",
                     color="Priority",
                     color_discrete_map={"P0":"#A32D2D","P1":"#534AB7","P2":"#854F0B","P3":"#5F5E5A"})
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(height=380, margin=dict(l=0,r=0,t=40,b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        rec_counts = df["recency_tier"].value_counts().reset_index()
        rec_counts.columns = ["Recency","Count"]
        fig2 = px.bar(rec_counts, x="Recency", y="Count",
                      title="Recency Distribution",
                      color="Recency",
                      color_discrete_map={"Last 7 days":"#3B6D11","8-30 days":"#854F0B","30+ days":"#5F5E5A"})
        fig2.update_layout(height=380, margin=dict(l=0,r=0,t=40,b=0), showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown('<div class="section-header">📈 Activation Score Distribution</div>', unsafe_allow_html=True)
    scores = df["activation_score"].apply(lambda x: int(x) if str(x).replace(".","").isdigit() else 0)
    fig3 = px.histogram(scores, nbins=30, title="Activation Score Distribution",
                        color_discrete_sequence=["#534AB7"])
    fig3.update_layout(height=320, margin=dict(l=0,r=0,t=40,b=0),
                       xaxis_title="Activation Score", yaxis_title="Operators")
    st.plotly_chart(fig3, use_container_width=True)

    st.markdown('<div class="section-header">📞 RM Workload</div>', unsafe_allow_html=True)
    rm_counts = df["rm_name"].value_counts().head(15).reset_index()
    rm_counts.columns = ["RM","Count"]
    fig4 = px.bar(rm_counts, y="RM", x="Count", orientation="h",
                  title="Top 15 RMs by Operator Count",
                  color_discrete_sequence=["#534AB7"])
    fig4.update_layout(height=420, margin=dict(l=0,r=0,t=40,b=0))
    st.plotly_chart(fig4, use_container_width=True)

# ─────────────────────────────────────────────
# 8. MAIN
# ─────────────────────────────────────────────
def main():
    # Session state defaults
    if "view" not in st.session_state:    st.session_state["view"] = "list"
    if "selected_op" not in st.session_state: st.session_state["selected_op"] = ""
    if "lead_page" not in st.session_state:   st.session_state["lead_page"] = 0

    # Navigation
    with st.sidebar:
        st.markdown("### 🚛 FO Activation CRM")
        nav = st.radio("Navigation", ["📋 Call Queue","📊 Analytics"],
                       key="nav_main",
                       index=0 if st.session_state["view"] in ("list","profile") else 1)
        if nav == "📊 Analytics" and st.session_state["view"] != "analytics":
            st.session_state["view"] = "analytics"; st.rerun()
        if nav == "📋 Call Queue" and st.session_state["view"] == "analytics":
            st.session_state["view"] = "list"; st.rerun()

    # Load data
    try:
        with st.spinner("Loading data…"):
            df_base = load_base_data()
            df = enrich_with_hot(df_base)
    except Exception as e:
        st.error(f"Could not load data from `{DB_PATH}`: {e}")
        st.info("Make sure the pipeline has run at least once (`python rm_new_activations.py`) to create the SQLite DB.")
        return

    # Route
    view = st.session_state["view"]
    if view == "analytics":
        page_analytics(df)
    elif view == "profile":
        page_operator_profile(df)
    else:
        page_lead_list(df)

    # Footer
    st.markdown("""
    <div style='text-align:center;margin-top:40px;padding:16px;background:#F1EFE8;border-radius:10px;'>
        <p style='color:#5F5E5A;font-size:12px;margin:0'>🚛 FO Activation CRM · Wheelseye FO Growth Cx POD</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
