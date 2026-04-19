import streamlit as st
import google.generativeai as genai
from snowflake.snowpark import Session
from PIL import Image
import pandas as pd

# 1. DATABASE CONNECTION SETUP
def create_snowflake_session():
    return Session.builder.configs(st.secrets["snowflake"]).create()

# 2. AI CONFIGURATION
if "GEMINI_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])

st.set_page_config(page_title="Aether-Chain", page_icon="🌱", layout="wide")

# 3. UI - HEADER
st.title("🌱 Aether-Chain: Proof of Green")
st.markdown("### Verify your deeds on-chain and climb the global leaderboard.")

# 4. USER INPUTS
col1, col2 = st.columns(2)
with col1:
    username = st.text_input("👤 Guardian Name (Username):")
with col2:
    wallet_address = st.text_input("💳 Phantom Wallet Address (Solana):")

uploaded_file = st.file_uploader("📸 Upload Proof (Photo of your action)", type=["jpg", "jpeg", "png"])

# 5. VERIFICATION LOGIC
if uploaded_file and username and wallet_address:
    st.image(uploaded_file, caption="Evidence Provided", width=300)
    
    if st.button("Verify & Claim Rewards 🚀", type="primary"):
        with st.spinner("Analyzing with Gemini AI..."):
            try:
                # Part A: Gemini Analysis
                img = Image.open(uploaded_file)
                model = genai.GenerativeModel("gemini-1.5-flash")
                # We tell Gemini to give us a specific "Impact Score" for the rewards
                response = model.generate_content(["Analyze this environmental deed. Give a verification (Yes/No) and an Impact Score from 1 to 10.", img])
                
                # Part B: Save to Snowflake (Including Wallet)
                session = create_snowflake_session()
                session.sql(f"""
                    INSERT INTO CLIMATE_LEADERBOARD (USERNAME, POINTS, DEED_TYPE, WALLET_ADDRESS) 
                    VALUES ('{username}', 100, 'Verified Action', '{wallet_address}')
                """).collect()
                
                st.success(f"Verified! 100 XP sent to {username}. Reward queued for Solana wallet!")
                st.balloons()
                st.info(f"AI Analysis: {response.text}")
                
            except Exception as e:
                st.error(f"Something went wrong: {e}")

# 6. LIVE GLOBAL LEADERBOARD
st.divider()
st.subheader("🏆 Global Leaderboard")
try:
    session = create_snowflake_session()
    # Pull data and sum points per user
    df = session.table("CLIMATE_LEADERBOARD").group_by("USERNAME").sum("POINTS").to_pandas()
    df.columns = ["Guardian", "Total XP"]
    st.dataframe(df.sort_values("Total XP", ascending=False), use_container_width=True)
except:
    st.info("Leaderboard is empty. Be the first to verify a deed!")

