import streamlit as st

st.set_page_config(
    page_title="AI Career Market Analyzer",
    page_icon="🧭",
    layout="wide",
)

st.title("🧭 AI Career Market Analyzer")
st.markdown("**System is running.** Upload your resume to begin.")

uploaded_file = st.file_uploader(
    "Upload your resume (PDF / DOCX)",
    type=["pdf", "docx"],
)

if uploaded_file:
    st.success(f"File uploaded: {uploaded_file.name}")
    st.info("Analysis module coming soon...")
