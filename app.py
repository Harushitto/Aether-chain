import streamlit as st
import google.generativeai as genai
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from PIL import Image
import pandas as pd
from datetime import datetime
import io

# ============================================================================
# 1. PAGE CONFIG & THEME
# ============================================================================
st.set_page_config(
    page_title="🌱 Aether-Chain: Proof of Green",
    page_icon="🌱",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ============================================================================
# 2. CUSTOM CSS - NATURE AESTHETIC (Dark Green)
# ============================================================================
st.markdown("""
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
    
    /* Main title styling */
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
    
    /* Button styling */
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
    
    /* Input fields */
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
    
    /* File uploader */
    .stFileUploader {
        background-color: #0f3018;
        border: 2px dashed #1a8f4f;
        border-radius: 8px;
        padding: 20px;
    }
    
    /* Card container */
    .card-container {
        background-color: #0f3018;
        border: 2px solid #1a8f4f;
        border-radius: 12px;
        padding: 20px;
        margin: 15px 0;
        box-shadow: 0 4px 10px rgba(0, 0, 0, 0.3);
    }
    
    /* Leaderboard table */
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
    
    /* Success/Error messages */
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
    
    /* Ticker animation */
    .ticker-container {
        background: linear-gradient(90deg, #0d5c2c, #1a8f4f);
        border-left: 4px solid #2db968;
        border-radius: 8px;
        padding: 15px 20px;
        margin: 20px 0;
        font-weight: 600;
        color: #e8f5e9;
        animation: slide-in 0.5s ease;
        box-shadow: 0 4px 10px rgba(45, 185, 104, 0.2);
    }
    
    @keyframes slide-in {
        from {
            opacity: 0;
            transform: translateX(-20px);
        }
        to {
            opacity: 1;
            transform: translateX(0);
        }
    }
    
    /* Modal/Success overlay */
    .success-modal {
        animation: modal-pop 0.6s cubic-bezier(0.34, 1.56, 0.64, 1);
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
    
    /* Divider */
    hr {
        border-color: #1a8f4f !important;
        margin: 30px 0 !important;
    }
    @keyframes marquee {
        from { transform: translateX(0%); }
        to { transform: translateX(-100%); }
    }
    .ticker-container {
        overflow: hidden;
        white-space: nowrap;
        background: rgba(45, 185, 104, 0.1); 
        padding: 10px 0;
        width: 100%;
    }
    .marquee-text {
        display: inline-block;
        padding-left: 100%; 
        animation: marquee 20s linear infinite;
    }


    </style>
    """, unsafe_allow_html=True)

# ============================================================================
# 3. DATABASE UTILITIES
# ============================================================================
def create_snowflake_session():
    """Create and return a Snowflake session."""
    return Session.builder.configs(st.secrets["snowflake"]).create()

def user_exists(session, username: str) -> bool:
    """Check if a user exists in the leaderboard."""
    result = session.sql(f"""
        SELECT COUNT(*) as cnt FROM CLIMATE_LEADERBOARD 
        WHERE UPPER(USERNAME) = UPPER('{username}')
    """).collect()
    return result[0]["CNT"] > 0

def create_user_entry(session, username: str, wallet_address: str):
    """Create a new user entry in the leaderboard if they don't exist."""
    if not user_exists(session, username):
        session.sql(f"""
            INSERT INTO CLIMATE_LEADERBOARD (USERNAME, WALLET_ADDRESS, POINTS, DEED_TYPE, CREATED_AT) 
            VALUES ('{username}', '{wallet_address}', 0, 'User Created', CURRENT_TIMESTAMP())
        """).collect()

def get_user_total_points(session, username: str) -> int:
    """Get total points for a user."""
    result = session.sql(f"""
        SELECT SUM(POINTS) as total FROM CLIMATE_LEADERBOARD 
        WHERE UPPER(USERNAME) = UPPER('{username}')
    """).collect()
    return result[0]["TOTAL"] if result[0]["TOTAL"] else 0

def record_deed(session, username: str, wallet_address: str, action_context: str, points: int):
    """Record a verified deed to Snowflake."""
    session.sql(f"""
        INSERT INTO CLIMATE_LEADERBOARD (USERNAME, WALLET_ADDRESS, POINTS, DEED_TYPE, ACTION_CONTEXT, CREATED_AT) 
        VALUES ('{username}', '{wallet_address}', {points}, 'Verified Deed', '{action_context}', CURRENT_TIMESTAMP())
    """).collect()

# ============================================================================
# 4. AI UTILITIES
# ============================================================================
def configure_gemini():
    """Configure Gemini API."""
    if "GEMINI_API_KEY" in st.secrets:
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])

def generate_daily_wisdom() -> str:
    """Generate a daily climate wisdom quote using Gemini."""
    try:
        model = genai.GenerativeModel("models/gemini-1.5-flash")
        response = model.generate_content(
            "Generate a powerful, inspirational quote about saving Earth, nature, or greenery. "
            "Keep it to exactly 15 words maximum. Reply with ONLY the quote, no extra text."
        )
        return response.text.strip()
    except Exception as e:
        return "🌍 Every action counts. Plant hope, harvest change. 🌱"

def verify_deed_with_gemini(image: Image.Image, action_context: str) -> tuple[bool, int, str]:
    """
    Verify deed using Gemini Vision API.
    Returns: (is_verified: bool, points: int, analysis: str)
    """
    try:
        model = genai.GenerativeModel("models/gemini-1.5-flash")
        prompt = f"""
        Analyze this image in the context of the described environmental action: "{action_context}"
        
        Determine if the image supports the action described. Respond in this exact format:
        VERIFIED: Yes/No
        POINTS: 10 if verified, 0 if not
        ANALYSIS: Brief 1-2 sentence explanation
        """
        response = model.generate_content([prompt, image])
        
        lines = response.text.strip().split('\n')
        verified = "yes" in lines[0].lower()
        points = 10 if verified else 0
        analysis = lines[2] if len(lines) > 2 else "Analysis complete."
        
        return verified, points, analysis
    except Exception as e:
        return False, 0, f"Error during verification: {str(e)}"

# ============================================================================
# 5. SESSION STATE INITIALIZATION
# ============================================================================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "username" not in st.session_state:
    st.session_state.username = None
if "wallet_address" not in st.session_state:
    st.session_state.wallet_address = None
if "daily_wisdom" not in st.session_state:
    st.session_state.daily_wisdom = None

# ============================================================================
# 6. LOGIN PAGE
# ============================================================================
def login_page():
    """Render the login/authentication page."""
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown("""
        <h1 style="text-align: center; font-size: 2.5rem;">
            🌱 Aether-Chain
        </h1>
        <p style="text-align: center; color: #2db968; font-size: 1.2rem; margin-bottom: 2rem;">
            Proof of Green • On-Chain Environmental Impact
        </p>
        """, unsafe_allow_html=True)
        
        st.markdown('<div class="card-container">', unsafe_allow_html=True)
        st.markdown("### Welcome, Guardian!")
        st.markdown("Join the global movement to verify and celebrate environmental deeds.")
        
        username = st.text_input(
            "👤 Guardian Name",
            placeholder="Enter your username",
            key="login_username"
        )
        
        wallet_address = st.text_input(
            "💳 Phantom Wallet Address (Solana)",
            placeholder="Enter your Solana wallet address",
            key="login_wallet"
        )
        
        if st.button("🚀 Enter Aether-Chain", type="primary", use_container_width=True):
            if username and wallet_address:
                try:
                    session = create_snowflake_session()
                    create_user_entry(session, username, wallet_address)
                    st.session_state.logged_in = True
                    st.session_state.username = username
                    st.session_state.wallet_address = wallet_address
                    st.session_state.daily_wisdom = generate_daily_wisdom()
                    st.rerun()
                except Exception as e:
                    st.error(f"Login failed: {str(e)}")
            else:
                st.warning("⚠️ Please enter both your guardian name and wallet address.")
        
        st.markdown('</div>', unsafe_allow_html=True)

# ============================================================================
# 7. DASHBOARD PAGE
# ============================================================================
def dashboard_page():
    """Render the main dashboard after login."""
    
    # Header with logout
    col1, col2, col3 = st.columns([3, 1, 1])
    with col3:
        if st.button("🚪 Logout", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.username = None
            st.session_state.wallet_address = None
            st.rerun()
    
    # Title
    st.markdown("""
    <h1 style="margin-bottom: 0.5rem;">
        🌱 Aether-Chain: Proof of Green
    </h1>
    """, unsafe_allow_html=True)
    st.markdown(f"### 👋 Welcome, **{st.session_state.username}**!")
    
    # ========================================================================
    # DAILY CLIMATE WISDOM TICKER
    # ========================================================================
        # === DAILY CLIMATE WISDOM TICKER (Updated) ===
    if st.session_state.daily_wisdom:
        wisdom_text = st.session_state.daily_wisdom
    else:
        wisdom_text = "🌱 Welcome, Guardian! Upload your first green deed to start your journey."

        st.markdown(f"""
        <div class="ticker-container">
            <div class="marquee-text">
                💡 <strong>Aether-Chain Update:</strong> {wisdom_text}
            </div>
        </div>
    """, unsafe_allow_html=True)
    # ========================================================================
    # ACTION SUBMISSION SECTION
    # ========================================================================
    st.markdown("### 🌍 Submit Your Environmental Deed")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="card-container">', unsafe_allow_html=True)
        st.markdown("**Step 1: Describe Your Action**")
        action_context = st.text_area(
            "What environmental action did you take?",
            placeholder="E.g., 'I planted 3 Neem trees in my garden' or 'Cleaned up plastic waste from the beach'",
            height=100,
            label_visibility="collapsed"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="card-container">', unsafe_allow_html=True)
        st.markdown("**Step 2: Upload Proof**")
        uploaded_file = st.file_uploader(
            "📸 Upload image or video proof",
            type=["jpg", "jpeg", "png", "mp4", "mov"],
            label_visibility="collapsed"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    
    # ========================================================================
    # VERIFICATION & SUBMISSION
    # ========================================================================
    if action_context and uploaded_file:
        st.markdown('<div class="card-container">', unsafe_allow_html=True)
        st.image(uploaded_file, caption="📸 Evidence Preview", width=300)
        
        if st.button("✅ Verify & Claim Rewards", type="primary", use_container_width=True):
            with st.spinner("🔍 Analyzing your deed with Gemini AI..."):
                try:
                    # Verify with Gemini
                    img = Image.open(uploaded_file) if uploaded_file.type.startswith('image') else Image.new('RGB', (100, 100))
                    verified, points, analysis = verify_deed_with_gemini(img, action_context)
                    
                    # Record to Snowflake
                    session = create_snowflake_session()
                    record_deed(session, st.session_state.username, st.session_state.wallet_address, action_context, points)
                    
                    if verified:
                        # Success modal
                        st.markdown("""
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
                        """, unsafe_allow_html=True)
                        st.balloons()
                    else:
                        st.warning("⚠️ Deed could not be verified. Please check your image and try again.")
                    
                    st.info(f"🤖 **AI Analysis:** {analysis}")
                    
                except Exception as e:
                    st.error(f"❌ Verification failed: {str(e)}")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ========================================================================
    # USER STATS
    # ========================================================================
    try:
        session = create_snowflake_session()
        user_points = get_user_total_points(session, st.session_state.username)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f"""
            <div class="card-container" style="text-align: center;">
                <h3 style="color: #2db968; margin: 0;">Your XP</h3>
                <p style="font-size: 2.5rem; color: #2db968; font-weight: bold; margin: 10px 0;">
                    {user_points}
                </p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="card-container" style="text-align: center;">
                <h3 style="color: #2db968; margin: 0;">Wallet</h3>
                <p style="font-size: 0.9rem; color: #e8f5e9; margin: 10px 0; word-break: break-all;">
                    {st.session_state.wallet_address[:10]}...
                </p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="card-container" style="text-align: center;">
                <h3 style="color: #2db968; margin: 0;">Status</h3>
                <p style="font-size: 1.2rem; color: #2db968; font-weight: bold; margin: 10px 0;">
                    🌱 Active
                </p>
            </div>
            """, unsafe_allow_html=True)
    except Exception as e:
        st.warning("Could not load user stats.")
    
    st.markdown("---")
    
    # ========================================================================
    # GLOBAL LEADERBOARD
    # ========================================================================
    st.markdown("### 🏆 Global Leaderboard")
    try:
        session = create_snowflake_session()
        df = session.sql("""
            SELECT USERNAME, SUM(POINTS) as TOTAL_XP 
            FROM CLIMATE_LEADERBOARD 
            GROUP BY USERNAME 
            ORDER BY TOTAL_XP DESC 
            LIMIT 20
        """).to_pandas()
        
        if not df.empty:
            df.columns = ["Guardian", "Total XP"]
            df.insert(0, "Rank", range(1, len(df) + 1))
            
            st.markdown('<div class="card-container">', unsafe_allow_html=True)
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.info("🌱 Be the first to verify a deed and top the leaderboard!")
    except Exception as e:
        st.info("📊 Leaderboard is being initialized. Check back soon!")

# ============================================================================
# 8. MAIN EXECUTION
# ============================================================================
def main():
    # 1. Start the AI
    configure_gemini()
    
    # 2. Route the user based on login status
    if not st.session_state.logged_in:
        login_page()
    else:
        dashboard_page()

if __name__ == "__main__":
    main()
    
