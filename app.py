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

    .verify-button {
        animation: pulse-glow 2s ease-in-out infinite;
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
    """
