import re
import hashlib
import html
import random
import time
import textwrap
import base64
from contextlib import contextmanager
from queue import Empty, Queue
from typing import Optional

import google.generativeai as genai
import pandas as pd
import streamlit as st
from PIL import Image
import snowflake.snowpark.exceptions as snowpark_exceptions
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F

# ============================================================================
# 1. PAGE CONFIG & THEME
# ============================================================================
st.set_page_config(
    page_title="🌱 Aether-Chain: Proof of Green",
    page_icon="🌱",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ============================================================================
# 2. CUSTOM CSS - NATURE AESTHETIC (Dark Green)
# ============================================================================
st.markdown(
    """
<style>
    :root {
        --primary-green: #0d5c2c;
        --secondary-green: #1a8f4f;
        --accent-green: #2db968;
        --light-bg: #0a1f12;
        --card-bg: #0f3018;
        --text-light: #e8f5e9;
        --border-color: #1a8f4f;
    }

    * {
        font-family: 'Inter', 'Segoe UI', sans-serif;
    }

    body {
        background: linear-gradient(135deg, #0a1f12 0%, #0d5c2c 100%);
        color: #e8f5e9;
    }

    .stApp {
        background: linear-gradient(135deg, #0a1f12 0%, #0d5c2c 100%);
        color: #e8f5e9;
    }

    h1 {
        background: linear-gradient(135deg, #2db968 0%, #1a8f4f 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        font-weight: 800;
        font-size: 3rem;
        margin-bottom: 0.5rem;
    }

    h2, h3 {
        color: #2db968;
        font-weight: 700;
    }

    .stButton > button {
        background: linear-gradient(135deg, #2db968 0%, #1a8f4f 100%);
        color: #ffffff;
        border: none;
        border-radius: 12px;
        padding: 12px 24px;
        font-weight: 600;
        font-size: 1rem;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(45, 185, 104, 0.3);
    }

    .stButton > button:hover {
        background: linear-gradient(135deg, #1a8f4f 0%, #0d5c2c 100%);
        box-shadow: 0 6px 20px rgba(45, 185, 104, 0.5);
        transform: translateY(-2px);
    }

    .stButton > button[kind="primary"]:hover {
        box-shadow: 0 0 18px rgba(45, 185, 104, 0.95), 0 0 32px rgba(26, 143, 79, 0.7);
        filter: brightness(1.08);
    }

    .stTextInput > div > div > input,
    .stTextArea > div > div > textarea {
        background-color: #0f3018 !important;
        color: #e8f5e9 !important;
        border: 2px solid #1a8f4f !important;
        border-radius: 8px !important;
        padding: 10px 12px !important;
        font-size: 1rem !important;
    }

    .stTextInput > div > div > input:focus,
    .stTextArea > div > div > textarea:focus {
        border-color: #2db968 !important;
        box-shadow: 0 0 10px rgba(45, 185, 104, 0.3) !important;
    }

    .stFileUploader {
        background-color: #0f3018;
        border: 2px dashed #1a8f4f;
        border-radius: 8px;
        padding: 20px;
    }

    .card-container {
        background-color: #0f3018;
        border: 2px solid #1a8f4f;
        border-radius: 12px;
        padding: 20px;
        margin: 15px 0;
        box-shadow: 0 4px 10px rgba(0, 0, 0, 0.3);
    }

    .stDataFrame {
        background-color: #0f3018 !important;
    }

    .stDataFrame th {
        background-color: #0d5c2c !important;
        color: #2db968 !important;
        font-weight: 700 !important;
    }

    .stDataFrame td {
        color: #e8f5e9 !important;
        border-bottom: 1px solid #1a8f4f !important;
    }

    .stSuccess, .stInfo {
        background-color: #0f3018 !important;
        border: 2px solid #2db968 !important;
        border-radius: 8px !important;
        color: #e8f5e9 !important;
    }

    .stError {
        background-color: #2c1a1a !important;
        border: 2px solid #f44336 !important;
        border-radius: 8px !important;
        color: #ffcdd2 !important;
    }

    .ticker-container {
        overflow: hidden;
        white-space: nowrap;
        background: rgba(45, 185, 104, 0.1);
        border-left: 4px solid #2db968;
        border-radius: 8px;
        padding: 10px 0;
        margin: 20px 0;
        width: 100%;
    }

    .deed-ticker {
        overflow: hidden;
        white-space: nowrap;
        width: 100%;
        margin: 16px 0 22px;
        border: 1px solid rgba(45, 185, 104, 0.65);
        border-radius: 12px;
        background: rgba(8, 28, 15, 0.68);
        box-shadow: 0 0 12px rgba(45, 185, 104, 0.25);
        padding: 10px 0;
    }

    .deed-ticker.deed-alert {
        border: 1px solid rgba(115, 255, 170, 0.95);
        background: linear-gradient(90deg, rgba(9, 36, 19, 0.92), rgba(20, 72, 39, 0.82));
        box-shadow: 0 0 20px rgba(45, 185, 104, 0.65), inset 0 0 18px rgba(45, 185, 104, 0.28);
    }

    .deed-ticker-track {
        display: inline-block;
        padding-left: 100%;
        animation: marquee 24s linear infinite;
        color: #b5ffcb;
        font-weight: 700;
        text-shadow: 0 0 10px #2db968, 0 0 20px #1a8f4f;
    }

    .marquee-text {
        display: inline-block;
        padding-left: 100%;
        animation: marquee 20s linear infinite;
    }

    .step-card {
        border: 1px solid rgba(45, 185, 104, 0.8) !important;
        box-shadow: 0 0 12px rgba(45, 185, 104, 0.18);
        padding: 18px !important;
    }

    .botanical-step {
        position: relative;
        overflow: hidden;
        background:
            linear-gradient(140deg, rgba(10, 34, 18, 0.85) 0%, rgba(18, 62, 34, 0.66) 100%),
            url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='280' height='180' viewBox='0 0 280 180'%3E%3Cg fill='none' stroke='%2376f1a6' stroke-opacity='0.25' stroke-width='1.5'%3E%3Cpath d='M20 164c45-34 72-88 78-146'/%3E%3Cpath d='M72 154c39-24 66-66 78-118'/%3E%3Cpath d='M132 165c26-20 48-52 62-96'/%3E%3Cpath d='M190 160c18-16 36-42 48-76'/%3E%3C/g%3E%3C/svg%3E");
        background-size: cover, 320px 220px;
        backdrop-filter: blur(3px);
    }

    .botanical-step h2,
    .botanical-step h3,
    .botanical-step p,
    .botanical-step strong {
        color: #d9ffe8;
        text-shadow: 0 0 10px rgba(45, 185, 104, 0.55), 0 0 18px rgba(26, 143, 79, 0.35);
    }

    .login-header-card {
        margin-bottom: 12px;
    }

    .quote-banner {
        margin: 10px 0 20px;
        padding: 12px 16px;
        border-left: 3px solid rgba(126, 240, 172, 0.75);
        background: rgba(8, 28, 15, 0.58);
        border-radius: 10px;
        font-style: italic;
        color: #d9ffe8;
        letter-spacing: 0.2px;
    }

    .wallet-card {
        text-align: center;
        background: rgba(14, 44, 24, 0.48);
        border: 1px solid rgba(125, 247, 173, 0.35);
        box-shadow: 0 0 18px rgba(45, 185, 104, 0.24), inset 0 0 20px rgba(8, 23, 14, 0.45);
        backdrop-filter: blur(10px);
        padding: 28px 22px !important;
    }

    .status-card {
        border-radius: 14px;
        background: linear-gradient(145deg, rgba(11, 34, 18, 0.9), rgba(19, 70, 38, 0.55));
        border: 1px solid rgba(115, 255, 170, 0.4);
        padding: 22px;
        box-shadow: 0 8px 24px rgba(0, 0, 0, 0.25);
    }

    .title-pill {
        display: inline-block;
        padding: 6px 12px;
        border-radius: 999px;
        background: rgba(34, 139, 79, 0.35);
        border: 1px solid rgba(126, 240, 172, 0.45);
        font-weight: 700;
        color: #bfffd7;
        margin-bottom: 12px;
    }

    .title-pill.legendary {
        background: linear-gradient(120deg, rgba(245, 211, 117, 0.24), rgba(111, 255, 185, 0.28));
        border-color: rgba(245, 211, 117, 0.8);
        color: #ffe7a8;
        box-shadow: 0 0 14px rgba(245, 211, 117, 0.65), 0 0 24px rgba(111, 255, 185, 0.45);
        animation: legendary-glow 2.1s ease-in-out infinite;
    }

    .xp-value.bump {
        animation: xp-bump 0.75s ease;
    }

    .xp-float {
        position: absolute;
        right: 14px;
        top: 10px;
        font-size: 1.05rem;
        font-weight: 800;
        color: #9affbe;
        text-shadow: 0 0 10px rgba(80, 255, 155, 0.8);
        animation: floatUp 1.8s ease forwards;
        pointer-events: none;
    }

    .progress-wrap {
        width: 100%;
        height: 12px;
        border-radius: 999px;
        background: rgba(9, 26, 14, 0.85);
        border: 1px solid rgba(95, 226, 146, 0.28);
        overflow: hidden;
        margin: 8px 0 4px;
    }

    .progress-bar {
        height: 100%;
        background: linear-gradient(90deg, #2db968, #7cf4ad);
        box-shadow: 0 0 10px rgba(45, 185, 104, 0.45);
    }

    .botanical-filler {
        position: relative;
        min-height: 120px;
        border-radius: 14px;
        border: 1px solid rgba(115, 255, 170, 0.35);
        padding: 18px;
        color: #d8ffe7;
        overflow: hidden;
        background-color: rgba(15, 48, 24, 0.8);
        background-image:
            linear-gradient(130deg, rgba(12, 42, 23, 0.92) 0%, rgba(21, 69, 38, 0.72) 100%),
            url('https://www.transparenttextures.com/patterns/leaves.png');
        background-size: cover, auto;
        backdrop-filter: blur(2px);
        box-shadow: inset 0 0 28px rgba(8, 22, 13, 0.55), 0 0 18px rgba(45, 185, 104, 0.2);
    }

    .botanical-filler::after {
        content: "";
        position: absolute;
        right: -22px;
        bottom: -16px;
        width: 140px;
        height: 80px;
        opacity: 0.22;
        background-repeat: no-repeat;
        background-size: contain;
        background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 280 160'%3E%3Cpath fill='%2376f1a6' d='M21 142c51-5 79-37 95-74 6-16 16-37 34-45-7 17-7 38-2 55 6 24 24 45 47 55-23 11-52 7-74-6-20-11-32-32-46-49-15 38-31 55-54 64z'/%3E%3Cpath fill='%234ee08a' d='M160 121c24-18 41-44 48-73 15 19 24 43 23 68-1 10-2 20-6 29-7-12-19-21-32-24-11-3-23-2-33 0z'/%3E%3C/svg%3E");
        pointer-events: none;
    }

    .verify-button button {
        animation: pulse-glow 1.9s ease-in-out infinite;
    }

    .nature-panel {
        position: relative;
        overflow: hidden;
        border: 1px solid rgba(45, 185, 104, 0.45);
        border-radius: 14px;
        padding: 20px;
        margin-top: 18px;
        background:
            linear-gradient(135deg, rgba(9, 27, 16, 0.85), rgba(16, 62, 32, 0.7)),
            url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='220' height='220' viewBox='0 0 220 220'%3E%3Cg fill='none' stroke='%233ecf78' stroke-opacity='0.2' stroke-width='1.2'%3E%3Cpath d='M30 192c38-42 68-100 72-164'/%3E%3Cpath d='M108 174c20-24 40-58 46-102'/%3E%3Cpath d='M24 138c16-8 28-22 38-38'/%3E%3Cpath d='M130 126c16-12 22-26 28-46'/%3E%3C/g%3E%3C/svg%3E");
        background-size: cover, 220px 220px;
        backdrop-filter: blur(3px);
    }

    .leaderboard-shell {
        border-radius: 16px;
        padding: 10px;
        background: rgba(14, 44, 24, 0.4);
        border: 1px solid rgba(115, 255, 170, 0.25);
        box-shadow: inset 0 0 0 1px rgba(16, 82, 40, 0.5), 0 8px 28px rgba(0, 0, 0, 0.25);
        backdrop-filter: blur(10px);
    }

    .leaderboard-table {
        width: 100%;
        border-collapse: separate;
        border-spacing: 0 10px;
    }

    .leaderboard-table thead th {
        color: #7ef0ac;
        text-align: left;
        font-size: 0.9rem;
        padding: 6px 12px 10px;
    }

    .leaderboard-table tbody tr {
        background: rgba(7, 26, 14, 0.65);
        border-radius: 12px;
        overflow: hidden;
    }

    .leaderboard-table tbody td {
        padding: 12px;
        border-top: 1px solid rgba(80, 209, 130, 0.2);
        border-bottom: 1px solid rgba(80, 209, 130, 0.2);
    }

    .leaderboard-table tbody td:first-child {
        border-left: 4px solid rgba(80, 209, 130, 0.35);
        border-top-left-radius: 12px;
        border-bottom-left-radius: 12px;
        width: 70px;
        font-weight: 700;
    }

    .leaderboard-table tbody td:last-child {
        border-right: 1px solid rgba(80, 209, 130, 0.2);
        border-top-right-radius: 12px;
        border-bottom-right-radius: 12px;
        color: #46ff99;
        font-weight: 800;
        text-shadow: 0 0 8px rgba(45, 185, 104, 0.45);
    }

    .leaderboard-table tbody tr.rank-1 td:first-child { border-left-color: #d4af37; }
    .leaderboard-table tbody tr.rank-2 td:first-child { border-left-color: #c0c0c0; }
    .leaderboard-table tbody tr.rank-3 td:first-child { border-left-color: #cd7f32; }

    .profile-icon-wrap {
        position: fixed;
        top: 20px;
        right: 22px;
        z-index: 999;
    }

    .profile-icon-wrap button {
        width: 62px !important;
        height: 62px !important;
        border-radius: 50% !important;
        border: 2px solid var(--accent-green) !important;
        box-shadow: 0 0 10px var(--accent-green) !important;
        padding: 0 !important;
        overflow: hidden !important;
        background: var(--card-bg) !important;
    }

    .profile-dashboard-shell {
        background: var(--card-bg);
        border: 1px solid var(--accent-green);
        border-radius: 16px;
        padding: 16px;
        box-shadow: 0 0 16px rgba(45, 185, 104, 0.45);
        margin-bottom: 16px;
    }

    .dashboard-avatar {
        width: 120px;
        height: 120px;
        border-radius: 50%;
        border: 2px solid var(--accent-green);
        box-shadow: 0 0 10px var(--accent-green);
        object-fit: cover;
    }

    .profile-corner-avatar {
        width: 62px;
        height: 62px;
        border-radius: 50%;
        border: 2px solid var(--accent-green);
        box-shadow: 0 0 10px var(--accent-green);
        object-fit: cover;
        background: var(--card-bg);
    }

    .success-modal {
        animation: modal-pop 0.6s cubic-bezier(0.34, 1.56, 0.64, 1);
        border: 1px solid rgba(115, 255, 170, 0.95);
        box-shadow:
            0 0 20px rgba(45, 185, 104, 0.8),
            0 0 40px rgba(45, 185, 104, 0.5),
            inset 0 0 22px rgba(45, 185, 104, 0.28);
    }

    hr {
        border-color: #1a8f4f !important;
        margin: 30px 0 !important;
    }

    @keyframes marquee {
        from { transform: translateX(0%); }
        to { transform: translateX(-100%); }
    }

    @keyframes modal-pop {
        0% {
            opacity: 0;
            transform: scale(0.8);
        }
        100% {
            opacity: 1;
            transform: scale(1);
        }
    }

    @keyframes pulse-glow {
        0%, 100% {
            box-shadow: 0 4px 15px rgba(45, 185, 104, 0.35);
            transform: translateY(0);
        }
        50% {
            box-shadow: 0 0 18px rgba(45, 185, 104, 0.75), 0 0 30px rgba(26, 143, 79, 0.45);
            transform: translateY(-1px);
        }
    }

    @keyframes floatUp {
        0% { opacity: 0; transform: translateY(12px) scale(0.95); }
        15% { opacity: 1; }
        100% { opacity: 0; transform: translateY(-30px) scale(1.05); }
    }

    @keyframes xp-bump {
        0% { transform: scale(1); }
        45% { transform: scale(1.14); }
        100% { transform: scale(1); }
    }

    @keyframes legendary-glow {
        0%, 100% { filter: brightness(1); }
        50% { filter: brightness(1.22); }
    }
</style>
""",
    unsafe_allow_html=True,
)

# ============================================================================
# 3. CONSTANTS & VALIDATION
# ============================================================================
LEADERBOARD_TABLE = "CLIMATE_LEADERBOARD"
SOLANA_WALLET_PATTERN = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
GEMINI_CANDIDATE_MODELS = [
    "gemini-2.5-flash",
    "gemini-2.0-flash",
    "gemini-1.5-flash",
]
LEADERBOARD_SCHEMA = [
    "USERNAME",
    "WALLET_ADDRESS",
    "POINTS",
    "DEED_TYPE",
    "ACTION_CONTEXT",
    "IMAGE_HASH",
    "CREATED_AT",
]
LEADERBOARD_INSERT_SCHEMA = [
    "USERNAME",
    "WALLET_ADDRESS",
    "POINTS",
    "DEED_TYPE",
    "ACTION_CONTEXT",
    "IMAGE_HASH",
]
DEFAULT_PROFILE_AVATAR = "https://api.dicebear.com/9.x/bottts-neutral/svg?seed=NatureGuardian"
COMMON_DEED_TYPO_CORRECTIONS = {
    "panted": "planted",
    "planed": "planted",
    "plaanted": "planted",
    "tre": "tree",
    "tress": "trees",
    "enviroment": "environment",
    "enviornment": "environment",
    "environement": "environment",
    "recylced": "recycled",
    "recycleing": "recycling",
    "cleened": "cleaned",
    "cleand": "cleaned",
    "riveer": "river",
    "beech": "beach",
}




def shorten_wallet_address(wallet_address: str) -> str:
    """Return compact wallet display for username synthesis."""
    cleaned = (wallet_address or "").strip()
    if len(cleaned) <= 10:
        return cleaned
    return f"{cleaned[:4]}...{cleaned[-3:]}"


def normalize_action_context(action_context: str) -> str:
    """Fix common spelling mistakes in deed descriptions."""
    normalized = action_context
    for typo, correction in COMMON_DEED_TYPO_CORRECTIONS.items():
        normalized = re.sub(rf"\b{re.escape(typo)}\b", correction, normalized, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", normalized).strip()


# ============================================================================
# 4. DATABASE UTILITIES
# ============================================================================
SNOWPARK_RETRYABLE_EXCEPTIONS = tuple(
    exc
    for exc in (
        getattr(snowpark_exceptions, "SnowparkClientException", None),
        getattr(snowpark_exceptions, "SnowparkSessionException", None),
        getattr(snowpark_exceptions, "SnowparkServerException", None),
        getattr(snowpark_exceptions, "SnowparkSQLException", None),
    )
    if exc is not None
)


REQUIRED_SNOWFLAKE_SECRETS = (
    "account",
    "user",
    "password",
    "warehouse",
    "database",
    "schema",
    "role",
)


def _snowflake_missing_secret_message() -> str:
    """Return an actionable error message for missing Snowflake secrets."""
    snowflake_secrets = st.secrets.get("snowflake", {})
    for key in REQUIRED_SNOWFLAKE_SECRETS:
        value = str(snowflake_secrets.get(key, "")).strip()
        if not value:
            return f"Snowflake connection failed. Check if {key} is defined in secrets."
    return (
        "Snowflake connection failed after retry. "
        "Verify credentials and network reachability for your Snowflake account."
    )


class SnowflakeSessionPool:
    """Small in-app pool for Snowflake sessions."""

    def __init__(self, configs: dict, max_size: int = 2):
        self.configs = configs
        self.max_size = max_size
        self._queue: Queue[Session] = Queue(maxsize=max_size)

    def _new_session(self) -> Session:
        return Session.builder.configs(self.configs).create()

    def _heartbeat(self, session: Session) -> None:
        session.sql("SELECT 1").collect()

    def acquire(self) -> Session:
        try:
            session = self._queue.get_nowait()
        except Empty:
            session = self._new_session()
        self._heartbeat(session)
        return session

    def release(self, session: Session) -> None:
        if session is None:
            return
        try:
            self._queue.put_nowait(session)
        except Exception:
            try:
                session.close()
            except Exception:
                pass

    def clear(self) -> None:
        while not self._queue.empty():
            try:
                session = self._queue.get_nowait()
            except Empty:
                break
            try:
                session.close()
            except Exception:
                pass


@st.cache_resource(show_spinner=False)
def get_snowflake_session_pool() -> SnowflakeSessionPool:
    """Create a cached Snowflake pool resource."""
    return SnowflakeSessionPool(dict(st.secrets.get("snowflake", {})))


def _validate_session_or_retry(pool: SnowflakeSessionPool, *, allow_retry: bool) -> Session:
    """Acquire pooled session, run heartbeat, and self-heal once on failure."""
    try:
        return pool.acquire()
    except SNOWPARK_RETRYABLE_EXCEPTIONS:
        if not allow_retry:
            raise
        st.cache_resource.clear()
        refreshed_pool = get_snowflake_session_pool()
        return _validate_session_or_retry(refreshed_pool, allow_retry=False)


def reset_snowflake_session() -> None:
    """Clear cached Snowflake pool and release active session handle."""
    active_session = st.session_state.get("snowflake_session")
    pool = st.session_state.get("snowflake_pool")
    if pool is not None and active_session is not None:
        pool.release(active_session)
    st.session_state.pop("snowflake_session", None)
    st.session_state.pop("snowflake_pool", None)
    st.cache_resource.clear()


def create_snowflake_session() -> Session:
    """Create/reuse a pooled Snowflake session with one self-healing reconnect."""
    existing_session = st.session_state.get("snowflake_session")
    pool = st.session_state.get("snowflake_pool")
    if existing_session is not None and pool is not None:
        try:
            existing_session.sql("SELECT 1").collect()
            return existing_session
        except SNOWPARK_RETRYABLE_EXCEPTIONS:
            pool.release(existing_session)
            st.session_state.pop("snowflake_session", None)
    try:
        pool = get_snowflake_session_pool()
        session = _validate_session_or_retry(pool, allow_retry=True)
        st.session_state.snowflake_pool = pool
        st.session_state.snowflake_session = session
        return session
    except (SNOWPARK_RETRYABLE_EXCEPTIONS, KeyError):
        st.error(_snowflake_missing_secret_message())
        raise RuntimeError("Snowflake session unavailable after retry.")


@contextmanager
def pooled_session() -> Session:
    """Yield a session from the pool and return it when done."""
    session = create_snowflake_session()
    try:
        yield session
    finally:
        pool = st.session_state.get("snowflake_pool")
        if pool is not None:
            pool.release(session)
        if st.session_state.get("snowflake_session") is session:
            st.session_state.pop("snowflake_session", None)


def get_leaderboard_df(session: Session):
    return session.table(LEADERBOARD_TABLE)


def get_leaderboard_columns(session: Session) -> set[str]:
    """Return uppercase column names currently available on the leaderboard table."""
    return {name.upper() for name in get_leaderboard_df(session).columns}


def _sql_literal(value: Optional[object]) -> str:
    """Return a safe SQL literal for Snowflake INSERT statements."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def insert_leaderboard_row(session: Session, row_payload: dict[str, Optional[object]]) -> None:
    """Insert exactly the 6 expected non-generated columns into the leaderboard table."""
    required_columns = LEADERBOARD_INSERT_SCHEMA
    missing_from_payload = [col for col in required_columns if col not in row_payload]
    if missing_from_payload:
        raise RuntimeError(
            f"Insert payload missing required columns: {', '.join(missing_from_payload)}"
        )

    values_sql = ", ".join(_sql_literal(row_payload[col]) for col in required_columns)
    columns_sql = ", ".join(required_columns)
    session.sql(
        f"INSERT INTO {LEADERBOARD_TABLE} ({columns_sql}) VALUES ({values_sql})"
    ).collect()


def user_exists(session: Session, username: str) -> bool:
    """Check if a user exists in the leaderboard."""
    count = (
        get_leaderboard_df(session)
        .filter(F.upper(F.col("USERNAME")) == F.lit(username.upper()))
        .count()
    )
    return count > 0


def get_existing_usernames(session: Session) -> set[str]:
    """Load existing usernames from Snowflake for collision checks."""
    usernames = (
        get_leaderboard_df(session)
        .select("USERNAME")
        .to_pandas()["USERNAME"]
        .dropna()
        .astype(str)
        .tolist()
    )
    return {name.strip().upper() for name in usernames if str(name).strip()}


def get_username_by_wallet_address(session: Session, wallet_address: str) -> Optional[str]:
    """Return the existing username for a wallet, if present."""
    rows = (
        get_leaderboard_df(session)
        .filter(F.upper(F.col("WALLET_ADDRESS")) == F.lit(wallet_address.upper()))
        .select("USERNAME")
        .limit(1)
        .collect()
    )
    if not rows:
        return None
    return str(rows[0]["USERNAME"]).strip() if rows[0]["USERNAME"] else None


def get_guardian_profile_by_wallet(session: Session, wallet_address: str) -> Optional[dict[str, Optional[str]]]:
    """Return persisted Guardian profile fields for a wallet, if present."""
    rows = (
        get_leaderboard_df(session)
        .filter(F.upper(F.col("WALLET_ADDRESS")) == F.lit(wallet_address.upper()))
        .sort(F.when(F.upper(F.col("DEED_TYPE")) == F.lit("USER_CREATED"), F.lit(0)).otherwise(F.lit(1)))
        .select("USERNAME", "IMAGE_HASH")
        .limit(1)
        .collect()
    )
    if not rows:
        return None
    return {
        "username": str(rows[0]["USERNAME"]).strip() if rows[0]["USERNAME"] else None,
        "image_hash": str(rows[0]["IMAGE_HASH"]).strip() if rows[0]["IMAGE_HASH"] else None,
    }


def wallet_has_profile_row(session: Session, wallet_address: str) -> bool:
    """Return whether this wallet already has a USER_CREATED profile row."""
    count = (
        get_leaderboard_df(session)
        .filter(F.upper(F.col("WALLET_ADDRESS")) == F.lit(wallet_address.upper()))
        .filter(F.upper(F.col("DEED_TYPE")) == F.lit("USER_CREATED"))
        .count()
    )
    return count > 0


def ensure_profile_row_exists(session: Session, username: str, wallet_address: str) -> None:
    """Ensure a dedicated USER_CREATED row exists for profile-level updates."""
    if wallet_has_profile_row(session, wallet_address):
        return
    insert_leaderboard_row(
        session,
        {
            "USERNAME": username,
            "WALLET_ADDRESS": wallet_address,
            "POINTS": 0,
            "DEED_TYPE": "USER_CREATED",
            "ACTION_CONTEXT": "Guardian joined Aether-Chain",
            "IMAGE_HASH": None,
        },
    )


def get_wallet_profile_image_hash(session: Session, wallet_address: str) -> Optional[str]:
    """Get the profile avatar hash/url token from the wallet's profile row."""
    rows = (
        get_leaderboard_df(session)
        .filter(F.upper(F.col("WALLET_ADDRESS")) == F.lit(wallet_address.upper()))
        .filter(F.upper(F.col("DEED_TYPE")) == F.lit("USER_CREATED"))
        .select("IMAGE_HASH")
        .limit(1)
        .collect()
    )
    if not rows:
        return None
    return str(rows[0]["IMAGE_HASH"]).strip() if rows[0]["IMAGE_HASH"] else None


def update_wallet_profile(
    session: Session,
    wallet_address: str,
    username: Optional[str] = None,
    profile_image_ref: Optional[str] = None,
) -> None:
    """Update wallet profile fields in Snowflake on the USER_CREATED row."""
    current_username = get_username_by_wallet_address(session, wallet_address) or "Guardian"
    ensure_profile_row_exists(session, current_username, wallet_address)

    safe_wallet = wallet_address.replace("'", "''")
    if username is not None:
        safe_username = username.replace("'", "''")
        session.sql(
            f"""
            UPDATE {LEADERBOARD_TABLE}
            SET USERNAME = '{safe_username}'
            WHERE UPPER(WALLET_ADDRESS) = UPPER('{safe_wallet}')
            """
        ).collect()
    if profile_image_ref is not None:
        safe_ref = profile_image_ref.replace("'", "''")
        session.sql(
            f"""
            UPDATE {LEADERBOARD_TABLE}
            SET IMAGE_HASH = '{safe_ref}'
            WHERE UPPER(WALLET_ADDRESS) = UPPER('{safe_wallet}')
              AND UPPER(DEED_TYPE) = 'USER_CREATED'
            """
        ).collect()


def generate_username_suggestions(
    chosen_name: str,
    wallet_address: str,
    existing_usernames: set[str],
    total: int = 3,
) -> list[str]:
    """Generate unique username suggestions that are not already taken."""
    suggestions: list[str] = []
    base = chosen_name.strip()
    wallet_suffix = (wallet_address or "").strip()[-4:]
    candidate_pool = [f"{base}_{wallet_suffix}"] if wallet_suffix else []

    while len(candidate_pool) < 50:
        candidate_pool.append(f"{base}_{random.randint(100, 999)}")

    seen = set()
    for candidate in candidate_pool:
        normalized = candidate.upper()
        if normalized in existing_usernames or normalized in seen:
            continue
        suggestions.append(candidate)
        seen.add(normalized)
        if len(suggestions) == total:
            break
    return suggestions


def create_user_entry(
    session: Session,
    username: str,
    wallet_address: str,
    image_hash: Optional[str] = None,
) -> None:
    """Create a new user entry in the leaderboard if they don't exist."""
    try:
        ensure_profile_row_exists(session, username, wallet_address)
        if image_hash:
            update_wallet_profile(
                session,
                wallet_address,
                profile_image_ref=image_hash,
            )
    except Exception as exc:
        raise RuntimeError("Unable to sync your profile to Snowflake right now.") from exc


def uploaded_image_to_base64(uploaded_file) -> Optional[str]:
    """Convert an uploaded image file to a base64 payload."""
    if uploaded_file is None:
        return None
    file_bytes = uploaded_file.getvalue()
    if not file_bytes:
        return None
    return base64.b64encode(file_bytes).decode("utf-8")


def get_user_total_points(session: Session, username: str) -> int:
    """Get total points for a user."""
    result = (
        get_leaderboard_df(session)
        .filter(F.upper(F.col("USERNAME")) == F.lit(username.upper()))
        .agg(F.sum(F.col("POINTS")).alias("TOTAL"))
        .collect()
    )
    total = result[0]["TOTAL"] if result else 0
    return int(total or 0)




def get_guardian_title(points: int) -> str:
    """Return guardian rank title for a total XP score."""
    if points <= 50:
        return "Rookie"
    if points <= 200:
        return "Earther"
    if points <= 500:
        return "Verdant Scout"
    if points <= 999:
        return "Nature Guardian"
    return "Earth Legend"


def get_next_rank_target(points: int) -> tuple[int, Optional[int]]:
    """Return (current floor, next target) for rank progression; next target None when max rank."""
    if points <= 50:
        return 0, 51
    if points <= 200:
        return 51, 201
    if points <= 500:
        return 201, 501
    if points <= 999:
        return 501, 1000
    return 1000, None

def get_recent_deed_feed(session: Session, limit: int = 12) -> list[str]:
    """Return a short ticker feed of recent deed actions."""
    rows = (
        get_leaderboard_df(session)
        .select("USERNAME", "ACTION_CONTEXT", "CREATED_AT")
        .filter(F.col("ACTION_CONTEXT").is_not_null())
        .sort(F.col("CREATED_AT").desc())
        .limit(limit)
        .collect()
    )

    feed = []
    for row in rows:
        username = str(row["USERNAME"] or "A Guardian").strip()
        action = str(row["ACTION_CONTEXT"] or "made a green impact").strip()
        feed.append(f"{username} has just {action}!")
    return feed


def record_deed(
    session: Session,
    username: str,
    wallet_address: str,
    action_context: str,
    points: int,
    deed_type: str,
    image_hash: Optional[str],
) -> None:
    """Record a deed verification result to Snowflake safely."""
    try:
        insert_leaderboard_row(
            session,
            {
                "USERNAME": username,
                "WALLET_ADDRESS": wallet_address,
                "POINTS": points,
                "DEED_TYPE": deed_type,
                "ACTION_CONTEXT": action_context,
                "IMAGE_HASH": image_hash,
            },
        )
    except Exception as exc:
        raise RuntimeError("Unable to record this deed in Snowflake.") from exc


def deed_image_already_submitted(session: Session, username: str, image_hash: str) -> bool:
    """Check whether the user has already submitted the same image hash."""
    columns = get_leaderboard_columns(session)
    if "IMAGE_HASH" not in columns:
        # Backward-compatible path for older table schemas that predate IMAGE_HASH.
        return False

    count = (
        get_leaderboard_df(session)
        .filter(F.upper(F.col("USERNAME")) == F.lit(username.upper()))
        .filter(F.col("IMAGE_HASH") == F.lit(image_hash))
        .count()
    )
    return count > 0


def compute_image_hash(file_bytes: bytes) -> str:
    """Return deterministic SHA-256 hash for uploaded image bytes."""
    return hashlib.sha256(file_bytes).hexdigest()


# ============================================================================
# 5. AI UTILITIES
# ============================================================================
def configure_gemini() -> None:
    """Configure Gemini API."""
    api_key = st.secrets.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is missing from Streamlit secrets.")
    genai.configure(api_key=api_key)


def _normalize_model_name(name: str) -> str:
    """Normalize a Gemini model id by stripping optional `models/` prefix."""
    return name.removeprefix("models/")


def _get_supported_model() -> Optional[str]:
    """Return the best available Gemini model id for generate_content."""
    try:
        configure_gemini()
        available = [
            m.name
            for m in genai.list_models()
            if hasattr(m, "supported_generation_methods")
            and "generateContent" in m.supported_generation_methods
        ]

        if not available:
            return None

        normalized_map = {_normalize_model_name(name): name for name in available}

        for candidate in GEMINI_CANDIDATE_MODELS:
            if candidate in normalized_map:
                return normalized_map[candidate]

        # Fallback: prefer any flash model if configured candidates are unavailable.
        for normalized_name, original_name in normalized_map.items():
            if "flash" in normalized_name:
                return original_name

        # Last resort: use first available model with generateContent support.
        return available[0]
    except Exception:
        return None


def generate_daily_wisdom() -> str:
    """Generate a daily climate wisdom quote using Gemini."""
    try:
        model_name = _get_supported_model()
        if not model_name:
            return "🌍 Every action counts. Plant hope, harvest change. 🌱"
        model = genai.GenerativeModel(model_name)
        response = model.generate_content(
            "Generate a powerful, inspirational quote about saving Earth, nature, or greenery. "
            "Keep it to exactly 15 words maximum. Reply with ONLY the quote, no extra text."
        )
        return (response.text or "").strip() or "🌍 Every action counts. Plant hope, harvest change. 🌱"
    except Exception:
        return "🌍 Every action counts. Plant hope, harvest change. 🌱"


def verify_deed_with_gemini(image: Image.Image, action_context: str) -> tuple[bool, int, str]:
    """
    Verify deed using Gemini Vision API.
    Returns: (is_verified: bool, points: int, analysis: str)
    """
    try:
        model_name = _get_supported_model()
        if not model_name:
            return (
                False,
                0,
                "Error during verification: No Gemini model with generateContent support is available.",
            )
        model = genai.GenerativeModel(model_name)
        prompt = f"""
        Analyze this image in the context of the described environmental action: "{action_context}".

        Determine if this image is a generic stock photo or a personal, real-world photograph.
        If it looks like a high-quality studio stock image or an image sourced from the internet,
        set verified: false and analysis: "Please upload a real, personal photo of your deed."
        Determine if this image is a real, original photograph taken by a user or a professional
        stock photo/internet-sourced image. If the image is a stock photo, reject it even if it
        shows the correct environmental action.

        Return strict JSON with keys:
        verified: boolean
        impact_magnitude: string ("small" or "large")
        points: integer (10-20 for small deeds, 50-100 for large deeds when verified=true, else 0)
        analysis: short 1-2 sentence explanation
        """
        response = model.generate_content([prompt, image])
        payload = (response.text or "").strip()

        verified_match = re.search(r'"?verified"?\s*:\s*(true|false)', payload, flags=re.IGNORECASE)
        points_match = re.search(r'"?points"?\s*:\s*(\d+)', payload, flags=re.IGNORECASE)
        impact_match = re.search(r'"?impact_magnitude"?\s*:\s*"?(small|large)' , payload, flags=re.IGNORECASE)
        analysis_match = re.search(r'"?analysis"?\s*:\s*"?(.+?)"?\s*(?:\}|$)', payload, flags=re.IGNORECASE | re.DOTALL)

        verified = bool(verified_match and verified_match.group(1).lower() == "true")
        parsed_points = int(points_match.group(1)) if points_match else 0
        impact = impact_match.group(1).lower() if impact_match else "small"

        if verified:
            if impact == "large":
                points = max(50, min(parsed_points, 100)) if parsed_points else 60
            else:
                points = max(10, min(parsed_points, 20)) if parsed_points else 15
        else:
            points = 0
        analysis = (
            analysis_match.group(1).strip().strip('"')
            if analysis_match
            else "Analysis complete."
        )

        return verified, points, analysis
    except Exception as e:
        return False, 0, f"Error during verification: {str(e)}"


# ============================================================================
# 6. SESSION STATE INITIALIZATION
# ============================================================================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "username" not in st.session_state:
    st.session_state.username = None
if "wallet_address" not in st.session_state:
    st.session_state.wallet_address = None
if "daily_wisdom" not in st.session_state:
    st.session_state.daily_wisdom = None
if "last_processed_submission_key" not in st.session_state:
    st.session_state.last_processed_submission_key = None
if "deed_alert_text" not in st.session_state:
    st.session_state.deed_alert_text = ""
if "deed_alert_time" not in st.session_state:
    st.session_state.deed_alert_time = 0.0
if "last_awarded_points" not in st.session_state:
    st.session_state.last_awarded_points = 0
if "last_award_time" not in st.session_state:
    st.session_state.last_award_time = 0.0
if "submitted_upload_keys" not in st.session_state:
    st.session_state.submitted_upload_keys = set()
if "user_xp" not in st.session_state:
    st.session_state.user_xp = 0
if "needs_username_registration" not in st.session_state:
    st.session_state.needs_username_registration = False
if "wallet_lookup_complete" not in st.session_state:
    st.session_state.wallet_lookup_complete = False
if "profile_edit_mode" not in st.session_state:
    st.session_state.profile_edit_mode = False
if "profile_image_ref" not in st.session_state:
    st.session_state.profile_image_ref = None
if "profile_image_preview" not in st.session_state:
    st.session_state.profile_image_preview = None
if "last_wallet_lookup" not in st.session_state:
    st.session_state.last_wallet_lookup = None


# ============================================================================
# 7. LOGIN PAGE
# ============================================================================
def complete_manual_login(wallet_address: str, username: str) -> None:
    """Complete login/registration flow after wallet + username validation."""
    wallet_address = wallet_address.strip()
    username = username.strip()

    if not SOLANA_WALLET_PATTERN.fullmatch(wallet_address):
        st.error("Invalid Solana wallet address. Please verify and try again.")
        return

    try:
        session = create_snowflake_session()
        existing_usernames = get_existing_usernames(session)
        normalized_username = username.upper()
        if normalized_username in existing_usernames:
            st.warning("Username already taken.")
            suggestions = generate_username_suggestions(username, wallet_address, existing_usernames)
            if suggestions:
                st.info("Try one of these available usernames: " + ", ".join(suggestions))
            return

        create_user_entry(session, username, wallet_address)
        st.session_state.wallet_address = wallet_address
        st.session_state.username = username
        st.session_state.profile_image_ref = get_wallet_profile_image_hash(session, wallet_address)
        st.session_state.profile_image_preview = None
        st.session_state.user_xp = get_user_total_points(session, username)
        st.session_state.logged_in = True
        st.session_state.authenticated = True
        st.session_state.needs_username_registration = False
        st.session_state.wallet_lookup_complete = False
        st.session_state.daily_wisdom = generate_daily_wisdom()
        st.rerun()
    except Exception:
        st.error(
            "We couldn't sync your wallet with Snowflake right now. "
            "Please try again in a moment."
        )


def login_page() -> None:
    """Render smart check-in authentication page."""
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown(
            """
        <h1 style="text-align: center; font-size: 2.5rem;">
            🌱 Aether-Chain
        </h1>
        <p style="text-align: center; color: #2db968; font-size: 1.2rem; margin-bottom: 2rem;">
            Proof of Green • On-Chain Environmental Impact
        </p>
        """,
            unsafe_allow_html=True,
        )

        st.markdown(
            """
            <div class="card-container step-card botanical-step login-header-card">
                <h3>Welcome, Guardian!</h3>
                <p>Enter your Wallet ID for smart check-in. Returning guardians skip registration.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown('<div class="card-container">', unsafe_allow_html=True)
        st.markdown("#### 🔐 Smart Check-In")

        manual_wallet_input = st.text_input(
            "Wallet ID",
            key="manual_wallet_input",
            placeholder="e.g. 9xQeWvG816bUx9EPfPy...",
        )
        submitted_wallet = manual_wallet_input.strip()
        if submitted_wallet and submitted_wallet != st.session_state.last_wallet_lookup:
            st.session_state.last_wallet_lookup = submitted_wallet
            if not SOLANA_WALLET_PATTERN.fullmatch(submitted_wallet):
                st.error("Invalid Solana Wallet ID format.")
            else:
                try:
                    session = create_snowflake_session()
                    guardian_profile = get_guardian_profile_by_wallet(session, submitted_wallet)
                    if guardian_profile and guardian_profile.get("username"):
                        existing_wallet_username = guardian_profile["username"] or "Guardian"
                        st.session_state.wallet_address = submitted_wallet
                        st.session_state.username = existing_wallet_username
                        st.session_state.profile_image_ref = guardian_profile.get("image_hash")
                        st.session_state.profile_image_preview = None
                        st.session_state.user_xp = get_user_total_points(session, existing_wallet_username)
                        st.session_state.logged_in = True
                        st.session_state.authenticated = True
                        st.session_state.needs_username_registration = False
                        st.session_state.wallet_lookup_complete = False
                        st.session_state.daily_wisdom = generate_daily_wisdom()
                        st.success("Existing guardian found. Redirecting now...")
                        st.rerun()
                    else:
                        st.session_state.wallet_address = submitted_wallet
                        st.session_state.wallet_lookup_complete = True
                        st.session_state.needs_username_registration = True
                except Exception:
                    pass

        if st.session_state.wallet_lookup_complete and st.session_state.needs_username_registration:
            st.info("🟢 Green Initiation: New wallet detected. Complete your guardian profile.")
            with st.form("green_initiation_form"):
                registration_username = st.text_input(
                    "Choose Username",
                    key="register_guardian_name",
                    placeholder="Choose a unique guardian name",
                )
                initiation_image = st.file_uploader(
                    "Optional Profile Picture",
                    type=["jpg", "jpeg", "png"],
                    key="green_initiation_image",
                )
                register_submit = st.form_submit_button("Begin Journey", use_container_width=True)

            if register_submit:
                candidate_name = registration_username.strip()
                if not candidate_name:
                    st.error("Please enter a Guardian Name before continuing.")
                else:
                    try:
                        session = create_snowflake_session()
                        existing_usernames = get_existing_usernames(session)
                        if candidate_name.upper() in existing_usernames:
                            st.warning("Username already taken.")
                            suggestions = generate_username_suggestions(
                                candidate_name,
                                st.session_state.wallet_address or "",
                                existing_usernames,
                            )
                            if suggestions:
                                st.info("Try one of these available usernames: " + ", ".join(suggestions))
                        else:
                            image_payload = uploaded_image_to_base64(initiation_image)
                            create_user_entry(
                                session,
                                candidate_name,
                                st.session_state.wallet_address or "",
                                image_hash=image_payload,
                            )
                            st.session_state.username = candidate_name
                            st.session_state.profile_image_ref = image_payload
                            st.session_state.profile_image_preview = None
                            st.session_state.user_xp = get_user_total_points(session, candidate_name)
                            st.session_state.logged_in = True
                            st.session_state.authenticated = True
                            st.session_state.needs_username_registration = False
                            st.session_state.wallet_lookup_complete = False
                            st.session_state.daily_wisdom = generate_daily_wisdom()
                            st.rerun()
                    except Exception:
                        st.error("Unable to complete Green Initiation right now.")

        wallet_address = (st.session_state.wallet_address or "").strip()

        if wallet_address:
            st.success(f"Connected Wallet: {shorten_wallet_address(wallet_address)}")
            st.caption(f"Username: {st.session_state.username}")
            st.info("Finalizing login…")
        else:
            st.info("Enter your wallet ID to begin.")

        st.markdown("</div>", unsafe_allow_html=True)


# ============================================================================
# 8. DASHBOARD PAGE
# ============================================================================
def _profile_avatar_data_uri() -> str:
    """Return data URI for circular icon/avatar image."""
    if st.session_state.profile_image_preview:
        return st.session_state.profile_image_preview
    profile_ref = (st.session_state.profile_image_ref or "").strip()
    if profile_ref.startswith("profile:sha256:"):
        return DEFAULT_PROFILE_AVATAR
    if profile_ref.startswith("data:image/"):
        return profile_ref
    if profile_ref:
        return f"data:image/png;base64,{profile_ref}"
    return profile_ref or DEFAULT_PROFILE_AVATAR


def render_profile_dashboard(session: Session) -> None:
    """Top-right circular profile icon and editable dashboard."""
    icon_src = _profile_avatar_data_uri()
    st.markdown(
        f"""
        <div class="profile-icon-wrap">
            <img src="{html.escape(icon_src)}" class="profile-corner-avatar" alt="guardian profile icon">
        </div>
        """,
        unsafe_allow_html=True,
    )
    with st.expander("🧑‍🌿 Guardian Profile", expanded=False):
        st.markdown(
            '<div class="profile-dashboard-shell"><h3>🌿 Guardian Profile</h3></div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<img src="{html.escape(icon_src)}" class="dashboard-avatar" alt="guardian avatar">',
            unsafe_allow_html=True,
        )
        st.caption("Nature Guardian Avatar")
        st.text_input("Wallet ID", value=st.session_state.wallet_address or "", disabled=True)

        username_col, action_col = st.columns([3, 1])
        with username_col:
            current_name = st.text_input(
                "Guardian Name",
                value=st.session_state.username or "",
                key="profile_username_display",
                disabled=not st.session_state.profile_edit_mode,
            )
        with action_col:
            if st.button("Edit", key="profile_edit_toggle", use_container_width=True):
                st.session_state.profile_edit_mode = not st.session_state.profile_edit_mode
                st.rerun()

        if st.session_state.profile_edit_mode and st.button("Check & Save Name", key="save_profile_name"):
            candidate = current_name.strip()
            if not candidate:
                st.error("Guardian Name cannot be empty.")
            elif not re.fullmatch(r"^[A-Za-z0-9_]{3,32}$", candidate):
                st.error("Use 3-32 letters, numbers, or underscore only.")
            else:
                existing_usernames = get_existing_usernames(session)
                if (
                    candidate.upper() in existing_usernames
                    and candidate.upper() != (st.session_state.username or "").upper()
                ):
                    st.warning("That Guardian Name is already taken.")
                    suggestions = generate_username_suggestions(
                        candidate,
                        st.session_state.wallet_address or "",
                        existing_usernames,
                    )
                    if suggestions:
                        st.info("Available alternatives: " + ", ".join(suggestions))
                else:
                    update_wallet_profile(
                        session,
                        st.session_state.wallet_address or "",
                        username=candidate,
                    )
                    st.session_state.username = candidate
                    st.session_state.profile_edit_mode = False
                    st.success("Guardian Name updated in Snowflake.")
                    st.rerun()

        st.markdown("##### Update Profile Picture")
        profile_upload = st.file_uploader(
            "Upload profile image",
            type=["jpg", "jpeg", "png"],
            key="profile_image_upload",
        )
        if profile_upload is not None and st.button("Save Profile Picture", key="save_profile_picture"):
            profile_image_base64 = uploaded_image_to_base64(profile_upload)
            if not profile_image_base64:
                st.error("Couldn't read uploaded image. Please try another file.")
            else:
                update_wallet_profile(
                    session,
                    st.session_state.wallet_address or "",
                    profile_image_ref=profile_image_base64,
                )
                st.session_state.profile_image_ref = profile_image_base64
                st.session_state.profile_image_preview = f"data:image/png;base64,{profile_image_base64}"
                st.success("Profile picture synced to Snowflake.")
                st.rerun()

        if st.button("🚪 Logout", use_container_width=True, key="profile_logout"):
            st.session_state.logged_in = False
            st.session_state.username = None
            st.session_state.wallet_address = None
            st.session_state.profile_image_ref = None
            st.session_state.profile_image_preview = None
            st.session_state.profile_edit_mode = False
            st.session_state.authenticated = False
            st.rerun()
def dashboard_page(session: Session) -> None:
    """Render the main dashboard after login."""
    render_profile_dashboard(session)
    now_ts = time.time()
    if (
        st.session_state.deed_alert_text
        and st.session_state.deed_alert_time
        and (now_ts - st.session_state.deed_alert_time > 10)
    ):
        st.session_state.deed_alert_text = ""
        st.session_state.deed_alert_time = 0.0

    st.markdown(
        """
    <h1 style="margin-bottom: 0.5rem;">
        🌱 Aether-Chain: Proof of Green
    </h1>
    """,
        unsafe_allow_html=True,
    )
    mission_text = "Mission: Reach Earth Legend status by verifying more deeds!"
    try:
        preview_points = get_user_total_points(session, st.session_state.username)
        if get_guardian_title(preview_points) == "Earth Legend":
            mission_text = "Mission: Maintain Earth Legend status by mentoring and inspiring fellow Guardians!"
    except Exception:
        pass

    welcome_col, tip_col = st.columns(2)
    with welcome_col:
        st.markdown(
            f"""
            <div class="card-container step-card botanical-step">
                <h3>👋 Welcome, <strong>{html.escape(str(st.session_state.username))}</strong>!</h3>
                <p><strong>Current Mission:</strong> {mission_text}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with tip_col:
        st.markdown(
            """
            <div class="card-container step-card botanical-step">
                <h3>🌿 Pro-Tip</h3>
                <p>Did you know? Trees planted in urban areas can reduce local temperatures by up to 8°C!</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    deed_alert_placeholder = st.empty()
    if st.session_state.deed_alert_text:
        deed_alert_placeholder.markdown(
            f"""
            <div class="deed-ticker deed-alert">
                <div class="deed-ticker-track">✨ {st.session_state.deed_alert_text}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        deed_alert_placeholder.empty()

    wisdom_text = (
        st.session_state.daily_wisdom
        or "🌱 Welcome, Guardian! Upload your first green deed to start your journey."
    )
    st.markdown(
        f"""
        <div class="quote-banner">
            {html.escape(wisdom_text)}
        </div>
    """,
        unsafe_allow_html=True,
    )
    try:
        deed_updates = get_recent_deed_feed(session, limit=12)
    except Exception:
        deed_updates = []

    deed_ticker_text = (
        " ✦ ".join(html.escape(item) for item in deed_updates)
        if deed_updates
        else "A Guardian has just planted native trees! ✦ A Guardian has just cleaned up a riverbank!"
    )
    st.markdown(
        f"""
        <div class="deed-ticker">
            <div class="deed-ticker-track">🌿 {deed_ticker_text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("### 🌍 Submit Your Environmental Deed")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="card-container step-card botanical-step">', unsafe_allow_html=True)
        st.markdown("**Step 1: Describe Your Action**")
        raw_action_context = st.text_area(
            "What environmental action did you take?",
            placeholder="E.g., 'I planted 3 Neem trees in my garden' or 'Cleaned up plastic waste from the beach'",
            height=100,
            label_visibility="collapsed",
        ).strip()
        action_context = normalize_action_context(raw_action_context)
        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="card-container step-card botanical-step">', unsafe_allow_html=True)
        st.markdown("**Step 2: Upload Proof**")
        uploaded_file = st.file_uploader(
            "📸 Upload image or video proof",
            type=["jpg", "jpeg", "png", "mp4", "mov"],
            label_visibility="collapsed",
        )
        st.markdown("</div>", unsafe_allow_html=True)

    filler_col1, filler_col2 = st.columns(2)
    with filler_col1:
        st.markdown(
            """
            <div class="botanical-filler">
                <strong>🌿 Native Habitat Spotlight</strong><br><br>
                Add details like species planted, cleanup location, or community impact for richer proof.
            </div>
            """,
            unsafe_allow_html=True,
        )
    with filler_col2:
        st.markdown(
            """
            <div class="botanical-filler">
                <strong>🍃 Deed Quality Tip</strong><br><br>
                Use clear daylight photos with your action visible so verification stays smooth and fair.
            </div>
            """,
            unsafe_allow_html=True,
        )

    if action_context and uploaded_file:
        st.markdown('<div class="card-container">', unsafe_allow_html=True)

        is_image = uploaded_file.type and uploaded_file.type.startswith("image")
        if is_image:
            st.image(uploaded_file, caption="📸 Evidence Preview", width=300)
        else:
            st.info("🎬 Video uploaded. Current verifier supports images only; please upload a representative image.")

        file_bytes = uploaded_file.getvalue()
        image_hash = compute_image_hash(file_bytes)
        submission_key = (
            f"{st.session_state.username}:{uploaded_file.name}:{len(file_bytes)}:{image_hash}"
        )

        if (
            st.session_state.last_processed_submission_key == submission_key
            or submission_key in st.session_state.submitted_upload_keys
        ):
            st.warning("⚠️ You have already submitted this specific upload in this session.")

        st.markdown('<div class="verify-button">', unsafe_allow_html=True)
        verify_clicked = st.button(
            "✅ Verify & Claim Rewards",
            type="primary",
            use_container_width=True,
            disabled=(
                st.session_state.last_processed_submission_key == submission_key
                or submission_key in st.session_state.submitted_upload_keys
            ),
        )
        st.markdown("</div>", unsafe_allow_html=True)
        if verify_clicked:
            if not is_image:
                st.warning("⚠️ Please upload a JPG or PNG image for AI verification.")
            else:
                with st.spinner("🔍 Analyzing your deed with Gemini AI..."):
                    try:
                        if deed_image_already_submitted(session, st.session_state.username, image_hash):
                            st.warning("⚠️ This image was already used for rewards. Upload a new deed photo.")
                            st.session_state.last_processed_submission_key = submission_key
                            st.session_state.submitted_upload_keys.add(submission_key)
                            st.markdown("</div>", unsafe_allow_html=True)
                            return

                        st.session_state.last_processed_submission_key = submission_key
                        st.session_state.submitted_upload_keys.add(submission_key)
                        img = Image.open(uploaded_file)
                        verified, points, analysis = verify_deed_with_gemini(img, action_context)

                        deed_type = "Verified Deed" if verified else "Rejected Deed"
                        record_deed(
                            session,
                            st.session_state.username,
                            st.session_state.wallet_address,
                            action_context,
                            points,
                            deed_type,
                            image_hash,
                        )

                        if verified:
                            st.session_state.deed_alert_text = (
                                f"{html.escape(st.session_state.username)} has just {html.escape(action_context)}!"
                            )
                            st.session_state.deed_alert_time = time.time()
                            st.session_state.last_awarded_points = points
                            st.session_state.last_award_time = time.time()
                            st.markdown(
                                f"""
                                <div class="success-modal card-container" style="border: 2px solid #2db968; background: linear-gradient(135deg, #0f3018 0%, #1a8f4f20 100%);">
                                    <h2 style="text-align: center; color: #2db968;">✨ Deed Verified!</h2>
                                    <p style="text-align: center; font-size: 1.1rem; color: #e8f5e9;">
                                        Thank you for your deed!<br>
                                        <strong>Your contribution to Earth has been recorded on-chain.</strong>
                                    </p>
                                    <p style="text-align: center; font-size: 2rem; color: #2db968; font-weight: bold;">
                                        +{points} XP Awarded!
                                    </p>
                                </div>
                                """,
                                unsafe_allow_html=True,
                            )
                            st.balloons()
                        else:
                            st.warning("⚠️ Deed could not be verified. Please check your image and try again.")

                        st.info(f"🤖 **AI Analysis:** {analysis}")

                    except Exception as e:
                        st.error(f"❌ Verification failed: {str(e)}")

        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("---")

    try:
        user_points = get_user_total_points(session, st.session_state.username)
        title = get_guardian_title(user_points)
        floor, next_target = get_next_rank_target(user_points)
        if next_target is None:
            progress_pct = 100.0
            progress_caption = "Max rank achieved"
        else:
            span = max(1, next_target - floor)
            progress_pct = max(0.0, min(100.0, ((user_points - floor) / span) * 100))
            progress_caption = f"{max(next_target - user_points, 0)} XP to {get_guardian_title(next_target)}"

        mask_wallet = f"{st.session_state.wallet_address[:4]}...{st.session_state.wallet_address[-4:]}"
        show_float = (
            st.session_state.last_awarded_points > 0
            and (time.time() - st.session_state.last_award_time) < 2.5
        )

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(
                f"""
            <div class="card-container" style="text-align: center; position: relative;">
                <h3 style="color: #2db968; margin: 0;">Your XP</h3>
                <p class="xp-value {'bump' if show_float else ''}" style="font-size: 2.5rem; color: #2db968; font-weight: bold; margin: 10px 0;">
                    {user_points}
                </p>
                {f'<div class="xp-float">+{st.session_state.last_awarded_points} XP</div>' if show_float else ''}
            </div>
            """,
                unsafe_allow_html=True,
            )

        with col2:
            st.markdown(
                f"""
            <div class="card-container wallet-card">
                <h3 style="color: #7ef0ac; margin: 0;">Private Wallet</h3>
                <p style="font-size: 1.05rem; color: #e8f5e9; margin: 14px 0 0; letter-spacing: 1px;">
                    {mask_wallet}
                </p>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with col3:
            status_class = "title-pill legendary" if title == "Earth Legend" else "title-pill"
            st.markdown(
                f"""
            <div class="status-card">
                <h3 style="color: #8ff7b9; margin: 0;">Status</h3>
                <div class="{status_class}">{title}</div>
                <div style="font-size: 0.84rem; color: #b5ffcb;">Progress to next rank</div>
                <div class="progress-wrap"><div class="progress-bar" style="width: {progress_pct:.1f}%;"></div></div>
                <div style="font-size: 0.82rem; color: #9de7b8;">{progress_caption}</div>
            </div>
            """,
                unsafe_allow_html=True,
            )
    except Exception:
        st.warning("Could not load user stats.")

    st.markdown("---")

    st.markdown("### 🏆 Global Leaderboard")
    try:
        leaderboard_df = get_leaderboard_df(session).to_pandas()

        if not leaderboard_df.empty:
            leaderboard_df["USERNAME"] = leaderboard_df["USERNAME"].astype(str).str.upper()
            leaderboard_df["POINTS"] = pd.to_numeric(leaderboard_df["POINTS"], errors="coerce").fillna(0).astype(int)
            leaderboard_df = leaderboard_df.groupby("USERNAME", as_index=False)["POINTS"].sum()
            leaderboard_df = leaderboard_df.sort_values(by="POINTS", ascending=False, kind="stable").head(20).reset_index(drop=True)
            leaderboard_df.insert(0, "RANK", range(1, len(leaderboard_df) + 1))
            leaderboard_df.insert(1, "LEVEL", leaderboard_df["POINTS"].apply(get_guardian_title))

            table_html = "<table class='leaderboard-table'><thead><tr><th>Rank</th><th>Level</th><th>Guardian</th><th>Total XP</th></tr></thead><tbody>"
            for _, row in leaderboard_df.iterrows():
                rank = int(row["RANK"])
                row_class = f"rank-{rank}" if rank <= 3 else ""
                guardian = html.escape(str(row["USERNAME"]))
                level = html.escape(str(row["LEVEL"]))
                xp = html.escape(str(int(row["POINTS"])))
                table_html += (
                    f"<tr class='{row_class}'>"
                    f"<td>{rank}</td>"
                    f"<td>{level}</td>"
                    f"<td>{guardian}</td>"
                    f"<td>{xp} XP</td>"
                    f"</tr>"
                )
            table_html += "</tbody></table>"

            leaderboard_html = textwrap.dedent(
                f"""
                <div class="nature-panel">
                    <div class="leaderboard-shell">
                        {table_html}
                    </div>
                </div>
                """
            ).strip()
            st.markdown(leaderboard_html, unsafe_allow_html=True)
        else:
            st.markdown(
                """
                <div class="nature-panel">
                    <div class="card-container" style="margin: 0;">
                        🌱 Be the first to verify a deed and top the leaderboard!
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    except Exception:
        st.markdown(
            """
            <div class="nature-panel">
                <div class="card-container" style="margin: 0;">
                    📊 Leaderboard is being initialized. Check back soon!
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ============================================================================
# 9. MAIN EXECUTION
# ============================================================================

def main():
    # Initialize session state
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        st.markdown('<div class="nature-panel">', unsafe_allow_html=True)
        st.title("🌱 Aether-Chain Login")
        wallet = st.text_input("Enter Solana Wallet Address", placeholder="e.g. 7xKX...j9")
        
        if st.button("🌿 Enter the Aether", kind="primary"):
            if SOLANA_WALLET_PATTERN.match(wallet):
                # Here you would call your Snowflake logic to verify
                st.session_state.authenticated = True
                st.session_state.wallet_address = wallet
                st.rerun()
            else:
                st.error("Invalid Solana Wallet format.")
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.success(f"Welcome back, {st.session_state.wallet_address}!")
        if st.button("Logout"):
            st.session_state.authenticated = False
            st.rerun()

if __name__ == "__main__":
    main()

