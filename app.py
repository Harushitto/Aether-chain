import re
import hashlib
import html
import time
import textwrap
from typing import Optional

import google.generativeai as genai
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from PIL import Image
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


def derive_guardian_name(wallet_address: str) -> str:
    """Build deterministic Guardian username directly from wallet."""
    short_wallet = shorten_wallet_address(wallet_address)
    return f"Guardian_{short_wallet}"


def phantom_wallet_component() -> dict:
    """Render wallet bridge with retry detection + connect state sync."""
    return components.html(
        """
        <div id="phantom-wallet-root">
            <button id="phantom-connect" style="
                width: 100%;
                background: linear-gradient(135deg, #2db968 0%, #1a8f4f 100%);
                color: #ffffff;
                border: none;
                border-radius: 12px;
                padding: 12px 24px;
                font-weight: 600;
                font-size: 1rem;
                box-shadow: 0 4px 15px rgba(45, 185, 104, 0.3);
                cursor: pointer;
            ">🔌 Connect Wallet</button>
            <div id="phantom-status" style="margin-top: 10px; color: #bfffd7; font-size: 0.9rem;">Checking wallet extension…</div>
        </div>

        <script src="https://unpkg.com/streamlit-component-lib@1.4.0/dist/index.js"></script>
        <script>
            const statusEl = document.getElementById('phantom-status');
            const connectButton = document.getElementById('phantom-connect');
            let pollIntervalId = null;
            let pollAttempts = 0;
            const maxAttempts = 10; // 10 * 500ms = 5 seconds

            function sendPayload(payload) {
                window.parent.postMessage({ type: "AETHER_PHANTOM_STATE", payload }, "*");
                if (window.Streamlit && window.Streamlit.setComponentValue) {
                    window.Streamlit.setComponentValue(payload);
                }
            }

            function getProvider() {
                return (
                    window.solana ||
                    (window.parent && window.parent.solana) ||
                    (window.top && window.top.solana) ||
                    null
                );
            }

            function walletAvailable() {
                const provider = getProvider();
                return !!(provider && provider.isPhantom);
            }

            function setConnectLoading(isLoading) {
                connectButton.disabled = isLoading;
                connectButton.textContent = isLoading ? '⏳ Loading...' : '🔌 Connect Wallet';
                connectButton.style.opacity = isLoading ? '0.75' : '1.0';
            }

            function refreshStatus() {
                if (walletAvailable()) {
                    statusEl.textContent = '✅ Wallet Ready.';
                    sendPayload({
                        connected: false,
                        wallet_ready: true,
                        wallet_found: true,
                        wallet_address: null,
                        error: null,
                        connect_in_progress: false
                    });
                } else {
                    statusEl.textContent = '⏳ Waiting for Phantom extension...';
                    sendPayload({
                        connected: false,
                        wallet_ready: false,
                        wallet_found: false,
                        wallet_address: null,
                        error: null,
                        connect_in_progress: false
                    });
                }
            }

            function startWalletPolling() {
                refreshStatus();
                if (walletAvailable()) {
                    return;
                }
                pollIntervalId = window.setInterval(() => {
                    pollAttempts += 1;
                    refreshStatus();
                    if (walletAvailable() || pollAttempts >= maxAttempts) {
                        window.clearInterval(pollIntervalId);
                        pollIntervalId = null;
                        if (!walletAvailable()) {
                            statusEl.textContent = '❌ Phantom Wallet not detected. Please install/enable the extension.';
                            sendPayload({
                                connected: false,
                                wallet_ready: false,
                                wallet_found: false,
                                wallet_address: null,
                                error: 'Phantom Wallet not detected. Please install/enable the extension.',
                                connect_in_progress: false
                            });
                        }
                    }
                }, 500);
            }

            connectButton.addEventListener('click', async () => {
                if (!walletAvailable()) {
                    refreshStatus();
                    return;
                }

                try {
                    const provider = getProvider();
                    setConnectLoading(true);
                    statusEl.textContent = 'Waiting for Phantom approval...';
                    sendPayload({
                        connected: false,
                        wallet_ready: true,
                        wallet_found: true,
                        wallet_address: null,
                        error: null,
                        connect_in_progress: true
                    });
                    const response = await provider.connect();
                    const publicKey = response.publicKey.toString();
                    statusEl.textContent = `Connected: ${publicKey.slice(0, 4)}...${publicKey.slice(-3)}`;
                    sendPayload({
                        connected: true,
                        wallet_ready: true,
                        wallet_found: true,
                        wallet_address: publicKey,
                        error: null,
                        connect_in_progress: false
                    });
                } catch (err) {
                    sendPayload({
                        connected: false,
                        wallet_ready: walletAvailable(),
                        wallet_found: true,
                        wallet_address: null,
                        error: err && err.message ? err.message : 'Wallet connection failed.',
                        connect_in_progress: false
                    });
                    statusEl.textContent = 'Wallet connection cancelled or failed.';
                } finally {
                    setConnectLoading(false);
                }
            });

            startWalletPolling();
            if (window.Streamlit && window.Streamlit.setFrameHeight) {
                window.Streamlit.setFrameHeight(110);
            }
        </script>
        """,
        height=130,
    )


def reset_environment_for_production(session: Session) -> None:
    """One-time utility: rebuild primary tables and clear test-era records."""
    session.sql(f"DROP TABLE IF EXISTS {LEADERBOARD_TABLE}").collect()
    session.sql(
        f"""
        CREATE TABLE {LEADERBOARD_TABLE} (
            USERNAME STRING NOT NULL,
            WALLET_ADDRESS STRING NOT NULL,
            POINTS NUMBER(38,0) DEFAULT 0,
            DEED_TYPE STRING,
            ACTION_CONTEXT STRING,
            IMAGE_HASH STRING,
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    ).collect()

    # Snowflake does not implement traditional secondary indexes; search optimization
    # provides index-like acceleration for equality lookups on these identity columns.
    session.sql(
        f"ALTER TABLE {LEADERBOARD_TABLE} ADD SEARCH OPTIMIZATION ON EQUALITY(USERNAME, WALLET_ADDRESS)"
    ).collect()

    # Clear announcement/test channels if they exist from prior staging runs.
    for table_name in ["ANNOUNCEMENTS", "CLIMATE_ANNOUNCEMENTS", "TEST_ANNOUNCEMENTS"]:
        try:
            session.sql(f"DELETE FROM {table_name}").collect()
        except Exception:
            continue


def normalize_action_context(action_context: str) -> str:
    """Fix common spelling mistakes in deed descriptions."""
    normalized = action_context
    for typo, correction in COMMON_DEED_TYPO_CORRECTIONS.items():
        normalized = re.sub(rf"\b{re.escape(typo)}\b", correction, normalized, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", normalized).strip()


# ============================================================================
# 4. DATABASE UTILITIES
# ============================================================================
@st.cache_resource
def create_snowflake_session() -> Session:
    """Create and cache a Snowflake session for the active Streamlit worker."""
    try:
        return Session.builder.configs(st.secrets["snowflake"]).create()
    except Exception as e:
        raise RuntimeError(
            "Failed to create Snowflake session. Verify Streamlit secrets for "
            "account, user, password/authenticator, role, warehouse, database, and schema."
        ) from e


def get_leaderboard_df(session: Session):
    return session.table(LEADERBOARD_TABLE)


def get_leaderboard_columns(session: Session) -> set[str]:
    """Return uppercase column names currently available on the leaderboard table."""
    return {name.upper() for name in get_leaderboard_df(session).columns}


def user_exists(session: Session, username: str) -> bool:
    """Check if a user exists in the leaderboard."""
    count = (
        get_leaderboard_df(session)
        .filter(F.upper(F.col("USERNAME")) == F.lit(username.upper()))
        .count()
    )
    return count > 0


def create_user_entry(session: Session, username: str, wallet_address: str) -> None:
    """Create a new user entry in the leaderboard if they don't exist."""
    if user_exists(session, username):
        return

    row_df = session.create_dataframe(
        [
            [
                username,
                wallet_address,
                0,
                "USER_CREATED",
                "Guardian joined Aether-Chain",
                None,
                None,
            ]
        ],
        schema=LEADERBOARD_SCHEMA,
    )
    row_df = row_df.with_column("CREATED_AT", F.current_timestamp())
    row_df.write.mode("append").save_as_table(LEADERBOARD_TABLE)


def wallet_address_exists(session: Session, wallet_address: str) -> bool:
    """Check if wallet address has already been recorded."""
    count = (
        get_leaderboard_df(session)
        .filter(F.upper(F.col("WALLET_ADDRESS")) == F.lit(wallet_address.upper()))
        .count()
    )
    return count > 0


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
    columns = get_leaderboard_columns(session)
    if "IMAGE_HASH" in columns:
        values = [username, wallet_address, points, deed_type, action_context, image_hash, None]
        schema = LEADERBOARD_SCHEMA
    else:
        values = [username, wallet_address, points, deed_type, action_context, None]
        schema = [col for col in LEADERBOARD_SCHEMA if col != "IMAGE_HASH"]

    row_df = session.create_dataframe([values], schema=schema)
    row_df = row_df.with_column("CREATED_AT", F.current_timestamp())
    row_df.write.mode("append").save_as_table(LEADERBOARD_TABLE)


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
if "production_reset_done" not in st.session_state:
    st.session_state.production_reset_done = False
if "production_reset_attempted" not in st.session_state:
    st.session_state.production_reset_attempted = False
if "wallet_ready" not in st.session_state:
    st.session_state.wallet_ready = False
if "wallet_connect_in_progress" not in st.session_state:
    st.session_state.wallet_connect_in_progress = False
if "user_xp" not in st.session_state:
    st.session_state.user_xp = 0


# ============================================================================
# 7. LOGIN PAGE
# ============================================================================
def login_page() -> None:
    """Render the wallet-first authentication page."""
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
                <p>Connect Phantom to begin. Your Solana wallet is your single source of identity.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown('<div class="card-container">', unsafe_allow_html=True)
        st.markdown("#### 🔐 Connect Phantom Wallet")

        wallet_payload = phantom_wallet_component()

        if isinstance(wallet_payload, dict):
            payload_wallet = (wallet_payload.get("wallet_address") or "").strip()
            payload_error = wallet_payload.get("error")
            wallet_found = wallet_payload.get("wallet_found")
            wallet_ready = bool(wallet_payload.get("wallet_ready"))
            connect_in_progress = bool(wallet_payload.get("connect_in_progress"))
            st.session_state.wallet_ready = wallet_ready
            st.session_state.wallet_connect_in_progress = connect_in_progress

            if payload_wallet and SOLANA_WALLET_PATTERN.fullmatch(payload_wallet):
                guardian_name = derive_guardian_name(payload_wallet)
                st.session_state.wallet_address = payload_wallet
                st.session_state.username = guardian_name
                try:
                    session = create_snowflake_session()
                    if not wallet_address_exists(session, payload_wallet):
                        create_user_entry(session, guardian_name, payload_wallet)
                    st.session_state.user_xp = get_user_total_points(session, guardian_name)
                    st.session_state.logged_in = True
                    st.session_state.daily_wisdom = generate_daily_wisdom()
                    st.rerun()
                except Exception as e:
                    st.error(
                        "Wallet connected, but login failed while syncing Snowflake. "
                        f"Details: {e}"
                    )
            elif payload_error:
                if str(payload_error).strip() == "Phantom Wallet not detected. Please install the extension.":
                    st.warning("Phantom Wallet not detected. Please install the extension.")
                else:
                    st.info(f"Wallet status: {payload_error}")
            elif wallet_found is False:
                st.warning("Phantom Wallet not detected. Please install the extension.")

        wallet_address = (st.session_state.wallet_address or "").strip()

        if wallet_address:
            guardian_name = derive_guardian_name(wallet_address)
            st.session_state.username = guardian_name
            st.success(f"Connected Wallet: {shorten_wallet_address(wallet_address)}")
            st.caption(f"Guardian Name is auto-derived: {guardian_name}")
            st.info("Finalizing login…")
        else:
            if st.session_state.wallet_connect_in_progress:
                st.info("Loading... approve the Phantom popup to continue.")
            elif st.session_state.wallet_ready:
                st.success("Wallet Ready. Click Connect Wallet to proceed.")
            else:
                st.info("Connect your Phantom wallet to continue.")

        st.markdown("</div>", unsafe_allow_html=True)


# ============================================================================
# 8. DASHBOARD PAGE
# ============================================================================
def dashboard_page() -> None:
    """Render the main dashboard after login."""
    session = create_snowflake_session()
    now_ts = time.time()
    if (
        st.session_state.deed_alert_text
        and st.session_state.deed_alert_time
        and (now_ts - st.session_state.deed_alert_time > 10)
    ):
        st.session_state.deed_alert_text = ""
        st.session_state.deed_alert_time = 0.0

    col1, col2, col3 = st.columns([3, 1, 1])
    with col3:
        if st.button("🚪 Logout", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.username = None
            st.session_state.wallet_address = None
            st.rerun()

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
def main() -> None:
    configure_gemini()

    if (
        st.secrets.get("RESET_ENVIRONMENT_ON_BOOT")
        and not st.session_state.get("production_reset_attempted")
    ):
        st.session_state.production_reset_attempted = True
        try:
            reset_environment_for_production(create_snowflake_session())
            st.session_state.production_reset_done = True
            st.success("Production reset completed.")
        except Exception as reset_error:
            st.error(f"Production reset failed: {reset_error}")

    if not st.session_state.logged_in:
        login_page()
    else:
        dashboard_page()


if __name__ == "__main__":
    main()
