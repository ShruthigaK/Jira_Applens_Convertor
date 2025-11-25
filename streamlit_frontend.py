import streamlit as st
import pandas as pd
import io
from data_transformer import run_transformation_pipeline
import tempfile
import os
from datetime import datetime

st.title("Jira to Applens Converter")

# Reset button
if st.button("Reset", type="secondary"):
    st.rerun()

uploaded_file = st.file_uploader("Upload Jira CSV", type=['csv'])

if uploaded_file:
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_input:
        tmp_input.write(uploaded_file.getvalue())
        input_path = tmp_input.name
    
    default_filename = f"Applens_{datetime.now().strftime('%Y%m%d')}.xlsx"
    output_filename = st.text_input("Output filename", default_filename)
    
    if st.button("Convert"):
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_output:
            output_path = tmp_output.name
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        success = run_transformation_pipeline(input_path, output_path)
        
        if success:
            progress_bar.progress(100)
            status_text.success("Conversion completed!")
            
            # Load converted data
            converted_df = pd.read_excel(output_path)
            
            # Download options
            col1, col2 = st.columns(2)
            
            with col1:
                with open(output_path, 'rb') as f:
                    st.download_button(
                        label=" Download Excel File",
                        data=f.read(),
                        file_name=output_filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            
            with col2:
                csv_data = converted_df.to_csv(index=False)
                st.download_button(
                    label="📄 Download as CSV",
                    data=csv_data,
                    file_name=output_filename.replace('.xlsx', '.csv'),
                    mime="text/csv"
                )
        else:
            status_text.error("Conversion failed!")
        
        os.unlink(input_path)
        os.unlink(output_path)
