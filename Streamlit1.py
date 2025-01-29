# app.py
import streamlit as st
import pandas as pd
import os
from main4 import ResumeProcessor

def initialize():
    if 'processor' not in st.session_state:
        st.session_state.processor = ResumeProcessor()
    if not os.path.exists('uploads'):
        os.makedirs('uploads')  

def handle_delete(candidate_id):
    if st.session_state.processor.delete_candidate(candidate_id):
        st.success("Candidate deleted successfully!")
        st.rerun()  # Using st.rerun() instead of experimental_rerun

def main():
    initialize()
    st.title("Enzigma PDF Resume Processor")

    # Upload Section
    with st.container():
        st.subheader("Upload Documents")
        uploaded_files = st.file_uploader("Drop PDF files here", type=['pdf'], accept_multiple_files=True)
        
        if uploaded_files:
            with st.spinner('Processing PDFs...'):
                for file in uploaded_files:
                    file_path = os.path.join('uploads', file.name)
                    with open(file_path, 'wb') as f:
                        f.write(file.getbuffer())
                    result = st.session_state.processor.process_pdf(file_path)
                    if result:
                        st.success(f"Successfully processed {file.name}")
                    else:
                        st.error(f"Failed to process {file.name}")

    # Display Records
    with st.container():
        st.subheader("Candidate Records")
        
        # Search box
        search = st.text_input("Search records...", "")
        
        # Get and display records
        candidates = st.session_state.processor.get_all_candidates()
        if candidates:
            df = pd.DataFrame(candidates)
            
            # Filter based on search
            if search:
                mask = df.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)
                df = df[mask]
            
            # Display as table with action buttons
            for _, row in df.iterrows():
                col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                
                with col1:
                    st.write(f"**{row['first_name']} {row['middle_name']} {row['last_name']}**")
                    st.write(f"ID: #{row['candidate_id']}")
                
                with col2:
                    st.write(f"📧 {row['email']}")
                    st.write(f"📱 {row['mobile']}")
                
                with col3:
                    st.write(f"Status: ✅ Completed")
                    st.write(f"Date: {row['processed_date'][:10]}")
                
                with col4:
                    if st.button("View", key=f"view_{row['candidate_id']}"):
                        st.session_state.selected_candidate = row['candidate_id']
                    if st.button("Delete", key=f"delete_{row['candidate_id']}"):
                        handle_delete(row['candidate_id'])
                
                st.divider()

        # View Candidate Details Modal
        if 'selected_candidate' in st.session_state:
            candidate = st.session_state.processor.get_candidate_by_id(st.session_state.selected_candidate)
            if candidate:
                with st.expander("Candidate Details", expanded=True):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.subheader("Personal Information")
                        st.write(f"**Name:** {candidate['first_name']} {candidate['middle_name']} {candidate['last_name']}")
                        st.write(f"**Email:** {candidate['email']}")
                        st.write(f"**Mobile:** {candidate['mobile']}")
                        st.write(f"**DOB:** {candidate['date_of_birth']}")
                        st.write(f"**Gender:** {candidate['gender']}")
                        
                        st.subheader("Identification")
                        st.write(f"**Passport:** {candidate['passport_number']}")
                        st.write(f"**PAN:** {candidate['pan_number']}")
                        st.write(f"**Visa Status:** {candidate['visa_status']}")
                    
                    with col2:
                        st.subheader("Current Address")
                        st.write(candidate['current_address'])
                        
                        st.subheader("Permanent Address")
                        st.write(candidate['permanent_address'])
                        
                        st.subheader("Emergency Contact")
                        st.write(f"**Name:** {candidate['emergency_contact_name']}")
                        st.write(f"**Number:** {candidate['emergency_contact_number']}")
                    
                    if st.button("Close"):
                        del st.session_state.selected_candidate

if __name__ == "__main__":
    main()