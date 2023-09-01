# Modified from Johannes Rieke's example code

import streamlit as st
import pandas as pd
from snowflake.snowpark import Session

import io
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from datetime import datetime, timedelta
import time

st.set_page_config(layout="wide")

# Establish Snowflake session
@st.cache_resource
def create_session():
    return Session.builder.configs(st.secrets.snowflake).create()

session = create_session()

res = None
db_name = "DTT_PROD"
schema_name = "LMS"
table_name = None
source_name = None
dataToExport = [
    {
        'type': 'SUKI',
        'reports': [
            "PAR_REASON",
            "PAR_HISTORICAL",
            "PAR_CLIENT_HISTORICAL",
            "INT_MONITORING__COLLECTIONS",
            "INT_MONITORING__LATAG",
            "INT_MONITORING__PAR_HISTORICAL_PLUS",
            "INT_MONITORING__WHO_DOES",
        ]
    },
    {
        'type': 'PCNI',
        'reports': [
            "INT_MONITORING__PAR_HISTORICAL_PLUS",
            "INT_MONITORING__WHO_DOES",
        ]
    }
]

st.title("Snowflake Data Extractor")

# Load data table
def load_data(table_name, from_date, to_date):
    ## Read in data table
    table = session.table(table_name)
    
    ## Do some computation on it
    table = table.limit(100)


    
    ## Collect the results. This will run the query and download the data
    table = table.collect()
    return table

# Select and display data table

def func(table_name):
    source_name = db_name+"."+schema_name+"."+table_name
    data = load_data(source_name, from_date, to_date)
    df = pd.DataFrame(data)
    output = io.BytesIO()
    excel_writer = pd.ExcelWriter(output, engine='openpyxl')
    df.to_excel(excel_writer, sheet_name='Sheet1', index=False)
    
    # Access the XlsxWriter workbook and worksheet objects.
    workbook = excel_writer.book
    worksheet = excel_writer.sheets['Sheet1']
    excel_writer.close()
    
    # Move the BytesIO stream position to the beginning of the file
    output.seek(0)
    
    return {"output": output, "df": df}

from_date = st.sidebar.date_input("From", datetime.today() - timedelta(days=7))
to_date = st.sidebar.date_input("To", datetime.today())

tabItems = [item['type'] for item in dataToExport]
tabs = st.tabs(tabItems)

for index, tab in enumerate(tabItems):
    with tabs[index]:
        matching_dict = next((item for item in dataToExport if item['type'] == tabItems[index]), None)
        if matching_dict:
            # Export Button
            st.text('Select a table to export.')
            if(session): 
                for i, table in enumerate(matching_dict['reports']):
                    if st.button(f'' + table + ' (' + matching_dict['type'] + ')'):
                        table_name = table
                        res = func(table_name)

                        if(res):
                            with st.expander("Check exported item"):
                                if(res["df"].empty):    
                                    st.warning(table_name + " table is empty.")
                                else:
                                    st.success(table_name + " is generated. Exported file is ready for download.")
                                    formatted_from_date = from_date.strftime("%d %B %Y")
                                    formatted_to_date = to_date.strftime("%d %B %Y")
                                    st.text(f"This preview is from '{formatted_from_date }' to '{formatted_to_date}'")
                                    st.write(res["df"])
                                    st.download_button(
                                        label=f"Download {table_name} ({matching_dict['type']})",
                                        data=res["output"],
                                        file_name=f'{table_name}.xlsx',
                                        # mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                                        )
        

# st.markdown(f"", unsafe_allow_html=True)
# st.markdown(f"<div class='col'>{col2_button}</div>", unsafe_allow_html=True)
# st.markdown(f"<div class='col'>{col3_button}</div>", unsafe_allow_html=True)
# st.markdown("</div>", unsafe_allow_html=True)