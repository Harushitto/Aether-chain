import re
import hashlib
from typing import Optional

import google.generativeai as genai
import pandas as pd
import streamlit as st
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

    .marquee-text {
        display: inline-block;
        padding-left: 100%;
        animation: marquee 20s linear infinite;
    }

    .success-modal {
        animation: modal-pop 0.6s cubic-bezier(0.34, 1.56, 0.64, 1);
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
</style>
""",
    unsafe_allow_html=True,
)

# ============================================================================
# 3. CONSTANTS & VALIDATION
# ============================================================================
LEADERBOARD_TABLE = "CLIMATE_LEADERBOARD"
USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_.\- ]{3,30}$")
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
    row_df = session.create_dataframe(
        [[username, wallet_address, points, deed_type, action_context, image_hash, None]],
        schema=LEADERBOARD_SCHEMA,
    )
    row_df = row_df.with_column("CREATED_AT", F.current_timestamp())
    row_df.write.mode("append").save_as_table(LEADERBOARD_TABLE)


def deed_image_already_submitted(session: Session, username: str, image_hash: str) -> bool:
    """Check whether the user has already submitted the same image hash."""
    count = (
        get_leaderboard_df(session)
        .filter(F.upper(F.col("USERNAME")) == F.lit(username.upper()))
        .filter(F.col("IMAGE_HASH") == F.lit(image_hash))
        .count()
    )
    return count > 0


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
        points: integer (10 only when verified=true, else 0)
        analysis: short 1-2 sentence explanation
        """
        response = model.generate_content([prompt, image])
        payload = (response.text or "").strip()

        verified_match = re.search(r'"?verified"?\s*:\s*(true|false)', payload, flags=re.IGNORECASE)
        points_match = re.search(r'"?points"?\s*:\s*(\d+)', payload, flags=re.IGNORECASE)
        analysis_match = re.search(r'"?analysis"?\s*:\s*"?(.+?)"?\s*(?:\}|$)', payload, flags=re.IGNORECASE | re.DOTALL)

        verified = bool(verified_match and verified_match.group(1).lower() == "true")
        parsed_points = int(points_match.group(1)) if points_match else 0
        points = 10 if verified and parsed_points >= 10 else 0
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


# ============================================================================
# 7. LOGIN PAGE
# ============================================================================
def login_page() -> None:
    """Render the login/authentication page."""
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

        st.markdown('<div class="card-container">', unsafe_allow_html=True)
        st.markdown("### Welcome, Guardian!")
        st.markdown("Join the global movement to verify and celebrate environmental deeds.")

        username = st.text_input(
            "👤 Guardian Name",
            placeholder="Enter your username",
            key="login_username",
        ).strip()

        wallet_address = st.text_input(
            "💳 Phantom Wallet Address (Solana)",
            placeholder="Enter your Solana wallet address",
            key="login_wallet",
        ).strip()

        if st.button("Enter Aether-Chain", type="primary", use_container_width=True):
            if not username or not wallet_address:
                st.warning("⚠️ Please enter both your guardian name and wallet address.")
            elif not USERNAME_PATTERN.fullmatch(username):
                st.warning("⚠️ Username must be 3-30 chars and use letters, numbers, spaces, _ . -")
            elif not SOLANA_WALLET_PATTERN.fullmatch(wallet_address):
                st.warning("⚠️ Please enter a valid Solana wallet address format.")
            else:
                try:
                    session = create_snowflake_session()
                    create_user_entry(session, username, wallet_address)
                    st.session_state.logged_in = True
                    st.session_state.username = username
                    st.session_state.wallet_address = wallet_address
                    st.session_state.daily_wisdom = generate_daily_wisdom()
                    st.rerun()
                except Exception as e:
                    st.error(
                        "Login failed while connecting to Snowflake. "
                        f"Details: {e}"
                    )

        st.markdown("</div>", unsafe_allow_html=True)


# ============================================================================
# 8. DASHBOARD PAGE
# ============================================================================
def dashboard_page() -> None:
    """Render the main dashboard after login."""
    session = create_snowflake_session()

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
    st.markdown(f"### 👋 Welcome, **{st.session_state.username}**!")

    wisdom_text = (
        st.session_state.daily_wisdom
        or "🌱 Welcome, Guardian! Upload your first green deed to start your journey."
    )
    st.markdown(
        f"""
        <div class="ticker-container">
            <div class="marquee-text">
                💡 <strong>Aether-Chain Update:</strong> {wisdom_text}
            </div>
        </div>
    """,
        unsafe_allow_html=True,
    )

    st.markdown("### 🌍 Submit Your Environmental Deed")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="card-container">', unsafe_allow_html=True)
        st.markdown("**Step 1: Describe Your Action**")
        action_context = st.text_area(
            "What environmental action did you take?",
            placeholder="E.g., 'I planted 3 Neem trees in my garden' or 'Cleaned up plastic waste from the beach'",
            height=100,
            label_visibility="collapsed",
        ).strip()
        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="card-container">', unsafe_allow_html=True)
        st.markdown("**Step 2: Upload Proof**")
        uploaded_file = st.file_uploader(
            "📸 Upload image or video proof",
            type=["jpg", "jpeg", "png", "mp4", "mov"],
            label_visibility="collapsed",
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
        image_hash = hashlib.sha256(file_bytes).hexdigest()
        submission_key = (
            f"{st.session_state.username}:{uploaded_file.name}:{len(file_bytes)}:{image_hash}"
        )

        if st.session_state.last_processed_submission_key == submission_key:
            st.warning("⚠️ You have already submitted this specific upload in this session.")

        verify_clicked = st.button(
            "✅ Verify & Claim Rewards",
            type="primary",
            use_container_width=True,
            disabled=st.session_state.last_processed_submission_key == submission_key,
        )
        if verify_clicked:
            if not is_image:
                st.warning("⚠️ Please upload a JPG or PNG image for AI verification.")
            else:
                with st.spinner("🔍 Analyzing your deed with Gemini AI..."):
                    try:
                        if deed_image_already_submitted(session, st.session_state.username, image_hash):
                            st.warning("⚠️ This image was already used for rewards. Upload a new deed photo.")
                            st.session_state.last_processed_submission_key = submission_key
                            st.markdown("</div>", unsafe_allow_html=True)
                            return

                        st.session_state.last_processed_submission_key = submission_key
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

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(
                f"""
            <div class="card-container" style="text-align: center;">
                <h3 style="color: #2db968; margin: 0;">Your XP</h3>
                <p style="font-size: 2.5rem; color: #2db968; font-weight: bold; margin: 10px 0;">
                    {user_points}
                </p>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with col2:
            st.markdown(
                f"""
            <div class="card-container" style="text-align: center;">
                <h3 style="color: #2db968; margin: 0;">Wallet</h3>
                <p style="font-size: 0.9rem; color: #e8f5e9; margin: 10px 0; word-break: break-all;">
                    {st.session_state.wallet_address[:10]}...
                </p>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with col3:
            st.markdown(
                """
            <div class="card-container" style="text-align: center;">
                <h3 style="color: #2db968; margin: 0;">Status</h3>
                <p style="font-size: 1.2rem; color: #2db968; font-weight: bold; margin: 10px 0;">
                    🌱 Active
                </p>
            </div>
            """,
                unsafe_allow_html=True,
            )
    except Exception:
        st.warning("Could not load user stats.")

    st.markdown("---")

    st.markdown("### 🏆 Global Leaderboard")
    try:
        df = (
            get_leaderboard_df(session)
            .group_by("USERNAME")
            .agg(F.sum(F.col("POINTS")).alias("TOTAL_XP"))
            .sort(F.col("TOTAL_XP").desc())
            .limit(20)
            .to_pandas()
        )

        if not df.empty:
            df.columns = ["Guardian", "Total XP"]
            df.insert(0, "Rank", range(1, len(df) + 1))

            st.markdown('<div class="card-container">', unsafe_allow_html=True)
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info("🌱 Be the first to verify a deed and top the leaderboard!")
    except Exception:
        st.info("📊 Leaderboard is being initialized. Check back soon!")


# ============================================================================
# 9. MAIN EXECUTION
# ============================================================================
def main() -> None:
    configure_gemini()

    if not st.session_state.logged_in:
        login_page()
    else:
        dashboard_page()


if __name__ == "__main__":
    main()
