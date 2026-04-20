import re
import hashlib
import html
import time
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

    .stButton > button,
    .stFormSubmitButton > button {
        background: linear-gradient(135deg, #2db968 0%, #1a8f4f 100%);
        color: #ffffff;
        border: 1px solid rgba(151, 255, 196, 0.45);
        border-radius: 12px;
        padding: 12px 24px;
        font-weight: 700;
        font-size: 1rem;
        letter-spacing: 0.2px;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(45, 185, 104, 0.3);
    }

    .stButton > button:hover,
    .stFormSubmitButton > button:hover {
        background: linear-gradient(135deg, #1a8f4f 0%, #0d5c2c 100%);
        box-shadow: 0 6px 20px rgba(45, 185, 104, 0.5);
        transform: translateY(-2px);
    }

    .stButton > button[kind="primary"]:hover,
    .stFormSubmitButton > button[kind="primary"]:hover {
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
        border-radius: 15px;
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

    .login-shell {
        background: linear-gradient(145deg, rgba(10, 33, 19, 0.86), rgba(23, 77, 42, 0.45));
        border: 1px solid rgba(126, 240, 172, 0.38);
        border-radius: 15px;
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        box-shadow: 0 10px 28px rgba(0, 0, 0, 0.3), inset 0 0 0 1px rgba(45, 185, 104, 0.15);
        max-width: 720px;
        margin: 0 auto;
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

    .leaderboard-shell {
        width: 100%;
        background: linear-gradient(150deg, rgba(12, 42, 23, 0.88), rgba(17, 63, 34, 0.68));
        border: 1px solid rgba(126, 240, 172, 0.35);
        border-radius: 15px;
        padding: 18px;
        box-shadow: 0 12px 30px rgba(0, 0, 0, 0.26);
        backdrop-filter: blur(8px);
        -webkit-backdrop-filter: blur(8px);
    }

    .leaderboard-table {
        width: 100%;
        border-radius: 15px;
        overflow: hidden;
        border: 1px solid rgba(115, 255, 170, 0.22);
    }

    .leaderboard-header,
    .leaderboard-row {
        display: grid;
        grid-template-columns: 0.9fr 1.8fr 2.8fr 2.5fr;
        gap: 12px;
        align-items: center;
    }

    .leaderboard-header {
        background: linear-gradient(90deg, #2db968, #1a8f4f);
        color: #f4fff8;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-size: 0.86rem;
        padding: 12px 14px;
    }

    .leaderboard-row {
        margin-top: 8px;
        padding: 12px 14px;
        background: rgba(8, 27, 14, 0.74);
        border: 1px solid rgba(115, 255, 170, 0.15);
        border-radius: 12px;
        transition: transform 0.18s ease, border-color 0.18s ease, box-shadow 0.18s ease;
    }

    .leaderboard-row:hover {
        transform: scale(1.02);
        border-color: rgba(115, 255, 170, 0.5);
        box-shadow: 0 10px 20px rgba(45, 185, 104, 0.2);
    }

    .leaderboard-row.rank-1 {
        border-color: rgba(245, 214, 119, 0.95);
        box-shadow: 0 0 16px rgba(245, 214, 119, 0.45);
    }

    .leaderboard-row.rank-2 {
        border-color: rgba(209, 224, 237, 0.92);
        box-shadow: 0 0 14px rgba(209, 224, 237, 0.35);
    }

    .leaderboard-row.rank-3 {
        border-color: rgba(204, 143, 91, 0.95);
        box-shadow: 0 0 14px rgba(204, 143, 91, 0.35);
    }

    .leaderboard-rank {
        font-weight: 800;
        font-size: 1rem;
    }

    .leaderboard-level {
        color: #c7ffdd;
        font-weight: 600;
    }

    .leaderboard-user {
        font-family: 'Inter', 'Segoe UI', sans-serif;
        font-weight: 800;
        letter-spacing: 0.35px;
    }

    .points-wrap {
        display: flex;
        flex-direction: column;
        gap: 6px;
    }

    .leaderboard-points {
        font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
        font-weight: 700;
        color: #eafff2;
    }

    .mini-progress-track {
        width: 100%;
        height: 7px;
        border-radius: 999px;
        background: rgba(126, 240, 172, 0.2);
        overflow: hidden;
    }

    .mini-progress-fill {
        height: 100%;
        border-radius: 999px;
        background: linear-gradient(90deg, #56df92 0%, #2db968 100%);
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


def verify_wallet(wallet_input: str) -> tuple[bool, str]:
    """Validate and normalize a Solana wallet address from text input."""
    normalized_wallet = (wallet_input or "").strip()
    is_verified = bool(SOLANA_WALLET_PATTERN.fullmatch(normalized_wallet))
    return is_verified, normalized_wallet


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


def get_snowflake_session() -> Session:
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


def create_snowflake_session() -> Session:
    """Backward-compatible alias for Snowflake session access."""
    return get_snowflake_session()


@contextmanager
def pooled_session() -> Session:
    """Yield a session from the pool and return it when done."""
    session = get_snowflake_session()
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
    """Insert row payload into available leaderboard columns."""
    required_columns = LEADERBOARD_INSERT_SCHEMA
    existing_columns = get_leaderboard_columns(session)
    optional_columns = {"IMAGE_HASH"}
    mandatory_columns = [col for col in required_columns if col not in optional_columns]

    missing_from_table = [col for col in mandatory_columns if col not in existing_columns]
    if missing_from_table:
        raise RuntimeError(
            "CLIMATE_LEADERBOARD schema mismatch. Missing table columns: "
            + ", ".join(missing_from_table)
        )

    insert_columns = [col for col in required_columns if col in existing_columns]
    missing_from_payload = [col for col in insert_columns if col not in row_payload]
    if missing_from_payload:
        raise RuntimeError(
            f"Insert payload missing required columns: {', '.join(missing_from_payload)}"
        )

    values_sql = ", ".join(_sql_literal(row_payload[col]) for col in insert_columns)
    columns_sql = ", ".join(insert_columns)
    try:
        session.sql(
            f"INSERT INTO {LEADERBOARD_TABLE} ({columns_sql}) VALUES ({values_sql})"
        ).collect()
    except Exception as exc:
        snowflake_error_code = getattr(exc, "error_code", None) or getattr(exc, "errno", None) or "N/A"
        snowflake_sql_state = getattr(exc, "sqlstate", None) or "N/A"
        print(
            f"Snowflake insert failed [code={snowflake_error_code}, sqlstate={snowflake_sql_state}]: {exc}"
        )
        raise RuntimeError(
            f"Unable to record this deed in Snowflake. code={snowflake_error_code}, sqlstate={snowflake_sql_state}"
        ) from exc


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


def create_user_entry(
    session: Session,
    username: str,
    wallet_address: str,
) -> None:
    """Create a new user entry in the leaderboard if they don't exist."""
    try:
        if get_username_by_wallet_address(session, wallet_address):
            return
        insert_leaderboard_row(
            session,
            {
                "USERNAME": username,
                "WALLET_ADDRESS": wallet_address,
                "POINTS": 0,
                "DEED_TYPE": "USER_CREATED",
                "ACTION_CONTEXT": "User joined Aether-Chain",
                "IMAGE_HASH": None,
            },
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
    numeric_points: float | int
    if isinstance(points, bool):
        numeric_points = int(points)
    elif isinstance(points, (int, float)):
        numeric_points = points
    else:
        numeric_points = int(float(points))
    try:
        insert_leaderboard_row(
            session,
            {
                "USERNAME": username,
                "WALLET_ADDRESS": wallet_address,
                "POINTS": numeric_points,
                "DEED_TYPE": deed_type,
                "ACTION_CONTEXT": action_context,
                "IMAGE_HASH": image_hash,
            },
        )
    except Exception as exc:
        raise RuntimeError(f"Unable to record this deed in Snowflake. Details: {exc}") from exc


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


def verify_deed_with_gemini(image: Image.Image, action_context: str) -> tuple[bool, int, str, str]:
    """
    Verify deed using Gemini Vision API.
    Returns: (is_verified: bool, points: int, impact_magnitude: str, analysis: str)
    """
    try:
        model_name = _get_supported_model()
        if not model_name:
            return (
                False,
                0,
                "small",
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

        return verified, points, impact, analysis
    except Exception as e:
        return False, 0, "small", f"Error during verification: {str(e)}"


# ============================================================================
# 6. SESSION STATE INITIALIZATION
# ============================================================================
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
if "wallet_verified" not in st.session_state:
    st.session_state.wallet_verified = False
if "verified_wallet_input" not in st.session_state:
    st.session_state.verified_wallet_input = ""


# ============================================================================
# 7. LOGIN PAGE
# ============================================================================
def login_page() -> None:
    """Render a minimal username + wallet login page."""
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
            <div class="card-container step-card botanical-step login-header-card login-shell">
                <h3>Welcome, Guardian!</h3>
                <p>Create your manual session with a unique Username and your Solana Wallet ID.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown('<div class="card-container login-shell">', unsafe_allow_html=True)
        st.markdown("#### 🔐 Login")

        with st.form("manual_login_form"):
            manual_username_input = st.text_input(
                "Username",
                key="manual_username_input",
                placeholder="e.g. username_oak",
            )
            manual_wallet_input = st.text_input(
                "Solana Wallet ID",
                key="manual_wallet_input",
                placeholder="e.g. 9xQeWvG816bUx9EPfPy...",
            )
            check_in_submit = st.form_submit_button("Enter Aether", type="primary", use_container_width=True)

        username_input = (st.session_state.get("manual_username_input") or "").strip()
        wallet_input = (st.session_state.get("manual_wallet_input") or "").strip()
        existing_usernames: set[str] = set()
        try:
            existing_usernames = get_existing_usernames(create_snowflake_session())
        except Exception:
            st.warning("Unable to fetch username availability right now.")

        if username_input and username_input.upper() in existing_usernames:
            st.error("❌ Username already exists in CLIMATE_LEADERBOARD.")

        if check_in_submit:
            if not username_input:
                st.error("Please enter a Username.")
            elif not re.fullmatch(r"^[A-Za-z0-9_]{3,32}$", username_input):
                st.error("Username must be 3-32 characters with letters, numbers, or underscore.")
            elif not wallet_input:
                st.error("Please enter your Solana Wallet ID.")
            elif username_input.upper() in existing_usernames:
                st.error("This username is already taken. Please choose a different username.")
            else:
                try:
                    session = create_snowflake_session()
                    create_user_entry(session, username_input, wallet_input)
                    st.session_state.wallet_address = wallet_input
                    st.session_state.username = username_input
                    st.session_state.user_xp = get_user_total_points(session, username_input)
                except Exception:
                    # Keep login session-based even when Snowflake profile sync is temporarily unavailable.
                    st.session_state.wallet_address = wallet_input
                    st.session_state.username = username_input
                    st.session_state.user_xp = 0
                st.session_state.authenticated = True
                st.session_state.wallet_verified = True
                st.session_state.verified_wallet_input = wallet_input
                st.session_state.daily_wisdom = generate_daily_wisdom()
                st.rerun()

        st.markdown("</div>", unsafe_allow_html=True)


def dashboard_page(session: Session) -> None:
    """Render minimalist AI-first page with upload and automatic verification."""

    st.markdown(
        """
    <h1 style="margin-bottom: 0.5rem;">
        🌱 Aether-Chain: Proof of Green
    </h1>
    """,
        unsafe_allow_html=True,
    )
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
    st.markdown("### 🌍 Submit Your Environmental Deed")
    st.markdown('<div class="card-container">', unsafe_allow_html=True)
    raw_action_context = st.text_area(
        "What environmental action did you take?",
        placeholder="E.g., I planted 3 trees in my neighborhood park",
        height=100,
    ).strip()
    action_context = normalize_action_context(raw_action_context)
    uploaded_file = st.file_uploader(
        "📸 Upload image evidence",
        type=["jpg", "jpeg", "png"],
    )
    st.markdown("</div>", unsafe_allow_html=True)

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

        analyze_clicked = st.button(
            "✅ Analyze with AI & Record Deed",
            type="primary",
            use_container_width=True,
            disabled=(
                st.session_state.last_processed_submission_key == submission_key
                or submission_key in st.session_state.submitted_upload_keys
            ),
        )

        if analyze_clicked:
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
                        verified, points, impact_magnitude, analysis = verify_deed_with_gemini(img, action_context)

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

                        eligibility = "Eligible for deed points ✅" if verified else "Not eligible for deed points ❌"
                        deed_size = (
                            "Large deed impact 🌳" if verified and impact_magnitude == "large"
                            else "Small deed impact 🌱" if verified
                            else "No impact points awarded"
                        )

                        if verified:
                            st.success(f"✅ Deed verified and recorded. +{points} XP awarded.")
                            st.balloons()
                        else:
                            st.warning("⚠️ Deed analyzed but not verified for points.")

                        st.info(
                            f"🤖 **AI Analysis:** {analysis}\n\n"
                            f"**Impact category:** {deed_size}\n\n"
                            f"**Eligibility:** {eligibility}"
                        )
                    except Exception as e:
                        st.error(f"❌ Verification failed: {str(e)}")

        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 🌍 Global Impact Rankings")
    try:
        leaderboard_df = get_leaderboard_df(session).to_pandas()

        if not leaderboard_df.empty:
            leaderboard_df["USERNAME"] = leaderboard_df["USERNAME"].astype(str).str.upper()
            leaderboard_df["POINTS"] = pd.to_numeric(leaderboard_df["POINTS"], errors="coerce").fillna(0).astype(int)
            leaderboard_df = leaderboard_df.groupby("USERNAME", as_index=False)["POINTS"].sum()
            leaderboard_df = leaderboard_df.sort_values(by="POINTS", ascending=False, kind="stable").head(20).reset_index(drop=True)
            leaderboard_df.insert(0, "RANK", range(1, len(leaderboard_df) + 1))
            leaderboard_df.insert(1, "LEVEL", leaderboard_df["POINTS"].apply(get_guardian_title))
            max_points = max(int(leaderboard_df["POINTS"].max()), 1)
            leaderboard_rows: list[str] = []
            for row in leaderboard_df.itertuples(index=False):
                rank = int(row.RANK)
                rank_class = f"rank-{rank}" if rank <= 3 else "rank-default"
                progress_percent = int((int(row.POINTS) / max_points) * 100)
                badge = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else "🌿"
                leaderboard_rows.append(
                    f"""
                    <div class="leaderboard-row {rank_class}">
                        <div class="leaderboard-rank">{badge} #{rank}</div>
                        <div class="leaderboard-level">{html.escape(str(row.LEVEL))}</div>
                        <div class="leaderboard-user">{html.escape(str(row.USERNAME))}</div>
                        <div class="points-wrap">
                            <div class="leaderboard-points">{int(row.POINTS):,} pts</div>
                            <div class="mini-progress-track">
                                <div class="mini-progress-fill" style="width: {progress_percent}%"></div>
                            </div>
                        </div>
                    </div>
                    """
                )

            st.markdown(
                f"""
                <div class="leaderboard-shell">
                    <div class="leaderboard-table">
                        <div class="leaderboard-header">
                            <div>Rank</div>
                            <div>Level</div>
                            <div>Guardian</div>
                            <div>Points</div>
                        </div>
                        {''.join(leaderboard_rows)}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.info("🌱 Be the first to verify a deed and top the leaderboard!")
    except Exception:
        st.info("📊 Leaderboard is being initialized. Check back soon!")


# ============================================================================
# 9. MAIN EXECUTION
# ============================================================================
def main() -> None:
    configure_gemini()
    try:
        session = create_snowflake_session()
    except SNOWPARK_RETRYABLE_EXCEPTIONS:
        st.error("Snowflake connection timed out before the app finished loading.")
        if st.button("Reconnect", key="snowflake_reconnect_button"):
            reset_snowflake_session()
            st.rerun()
        return
    except RuntimeError:
        st.error("Snowflake connection failed. Please verify credentials and reconnect.")
        if st.button("Reconnect", key="snowflake_reconnect_runtime_button"):
            reset_snowflake_session()
            st.rerun()
        return

    if not st.session_state.authenticated:
        login_page()
    else:
        dashboard_page(session)


if __name__ == "__main__":
    main()
