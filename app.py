import streamlit as st
import google.generativeai as genai
from datetime import datetime

# Configure Streamlit page
st.set_page_config(
    page_title="Aether Chain - Green Deed Tracker",
    page_icon="🌱",
    layout="wide"
)

# Initialize session state for storing deeds
if "deeds" not in st.session_state:
    st.session_state.deeds = []

# Main UI
st.title("🌍 Aether Chain - Green Deed Verifier")
st.markdown("Verify your green deeds and track your environmental impact!")

# Sidebar for API configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    api_key = st.text_input("Enter your Google Gemini API Key:", type="password")
    
    if api_key:
        genai.configure(api_key=api_key)
        st.success("✅ API Key configured!")
    else:
        st.warning("⚠️ Please enter your Gemini API Key to proceed")

# Main content area
if api_key:
    st.divider()
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📝 Submit a Green Deed")
        
        deed_description = st.text_area(
            "Describe your green deed:",
            placeholder="e.g., I planted a tree in my backyard today",
            height=100
        )
        
        deed_category = st.selectbox(
            "Select a category:",
            ["🌳 Planting", "♻️ Recycling", "💡 Energy", "💧 Water", "🚶 Transportation", "🍽️ Diet", "Other"]
        )
    
    with col2:
        st.subheader("📊 Stats")
        st.metric("Total Deeds Verified", len(st.session_state.deeds))
    
    if st.button("✅ Verify Deed with Gemini", use_container_width=True, type="primary"):
        if not deed_description.strip():
            st.error("❌ Please enter a deed description!")
        else:
            with st.spinner("🔍 Verifying with Gemini..."):
                try:
                    # Create prompt for Gemini
                    prompt = f"""You are an environmental deed verifier. Analyze the following green deed and provide:
1. Verification: Is this a legitimate green deed? (Yes/No)
2. Impact Score: Rate the environmental impact from 1-10
3. Summary: A brief 1-2 sentence summary of the deed
4. Suggestion: One way to amplify this good deed

Green Deed Description: "{deed_description}"
Category: {deed_category}

Provide your response in this exact format:
VERIFICATION: [Yes/No]
IMPACT_SCORE: [1-10]
SUMMARY: [Your summary]
SUGGESTION: [Your suggestion]"""

                    # Call Gemini API
                    model = genai.GenerativeModel("gemini-pro")
                    response = model.generate_content(prompt)
                    
                    # Display verification result
                    st.success("✅ Deed Verified!")
                    
                    # Store deed in session state
                    deed_data = {
                        "description": deed_description,
                        "category": deed_category,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "gemini_response": response.text
                    }
                    st.session_state.deeds.append(deed_data)
                    st.write(response.text)
                    st.balloons()
                    
                except Exception as e:
                    st.error(f"❌ Error verifying deed: {str(e)}")
    
    st.divider()
    
    # Display history
    if st.session_state.deeds:
        st.subheader("📜 Verified Deeds History")
        for deed in reversed(st.session_state.deeds):
            with st.expander(f"{deed['timestamp']} - {deed['category']}"):
                st.write(f"**Deed:** {deed['description']}")
                st.info(deed['gemini_response'])
else:
    st.info("👈 Please enter your Google Gemini API Key in the sidebar to get started!")

