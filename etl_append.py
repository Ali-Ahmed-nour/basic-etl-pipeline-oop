#!/usr/bin/env python
# coding: utf-8

# # 📊 ETL Append Pipeline
# This notebook demonstrates an ETL pipeline that reads data from multiple file formats (CSV, JSON, XML), appends them vertically, and exports the result into a single CSV file.
# 
# **ETL Stages Covered:**
# - **Extract**: Read from multiple formats
# - **Transform**: Combine data with consistent structure
# - **Load**: Save as a clean CSV file
# 
# ✅ Built with reusability and logging in mind.
# 

# In[4]:


import os
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import logging


# In[5]:


# Change paths as needed
source_path = r"F:\projects\coursera\ibm data engineer_ETL_project\source"
target_path = r"F:\projects\coursera\ibm data engineer_ETL_project\ETL_Result"
expected_columns = ['name', 'height', 'weight'] # change columns manually each time depending on the files

# Generate timestamp for output file
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')

# Name of output file with timestamp
output_file_name = f"appended_output_{timestamp}.csv"
output_file_path = os.path.join(target_path, output_file_name)


# In[6]:


# Configure logging settings

log_path = os.path.join(target_path, "etl_append_log.log")

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)


# ## Class Definition: ETLAppend
# 
# This class handles the ETL process for structured files (CSV, JSON, and XML).  
# It reads each supported file format from the specified source directory, ensures consistency based on the expected columns, appends all data vertically into one unified DataFrame, and finally saves the result as a CSV file in the target directory.
# 
# ### Key Features:
# - Reads CSV, JSON (line-delimited), and XML files
# - Supports excluding files by name
# - Combines data based on consistent column names
# - Saves output with optional timestamp-based naming
# - Logs each step of the process for traceability
# 

# class ETLAppend:
#     def __init__(self, source_path, target_path, expected_columns):
#         self.source_path = source_path
#         self.target_path = target_path
#         self.columns = expected_columns
#         logging.info("ETLAppend initialized.")
# 
#     def read_csv(self, file_path):
#         logging.info(f"Reading CSV file: {file_path}")
#         return pd.read_csv(file_path)
# 
#     def read_json(self, file_path):
#         logging.info(f"Reading JSON file: {file_path}")
#         return pd.read_json(file_path, lines=True)
# 
#     def read_xml(self, file_path):
#         logging.info(f"Reading XML file: {file_path}")
#         df = pd.DataFrame(columns=self.columns)
#         tree = ET.parse(file_path)
#         root = tree.getroot()
#         for element in root:
#             row = {}
#             for col in self.columns:
#                 el = element.find(col)
#                 row[col] = float(el.text) if col in ['height', 'weight'] else el.text
#                 row[col] = row[col] if row[col] is not None else ""
#             df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
#         return df
# 
#     def read_all_files(self, exclude_file=None):
#         logging.info("Started reading all files in source directory.")
#         combined_df = pd.DataFrame(columns=self.columns)
#         readers = [("*.csv", self.read_csv), ("*.json", self.read_json), ("*.xml", self.read_xml)]
#         for pattern, reader in readers:
#             for file_path in glob.glob(os.path.join(self.source_path, pattern)):
#                 if exclude_file and os.path.basename(file_path) == exclude_file:
#                     logging.info(f"Skipping file: {file_path}")
#                     continue
#                 df = reader(file_path)
#                 combined_df = pd.concat([combined_df, df], ignore_index=True)
#         logging.info("Finished combining all files.")
#         return combined_df
# 
#     def save_to_csv(self, df, file_name=None):
#         os.makedirs(self.target_path, exist_ok=True)
#         if not file_name:
#             today = datetime.now().strftime("%Y-%m-%d")
#             file_name = f"data_output_{today}.csv"
#         output_path = os.path.join(self.target_path, file_name)
#         df.to_csv(output_path, index=False)
#         logging.info(f"Data saved to: {output_path}")
#         print("Data saved to:", output_path)
# 

# In[7]:


class ETLAppend() :
    def __init__(self, source_path, target_path, columns) :
        self.source_path = source_path
        self.output_file_path = output_file_path
        
        self.columns = columns
        logging.info("ETL Append Job Started.")

        # Automatically run the full ETL pipeline upon object creation
        self.run()  


    def extract_from_csv(self, file_path):
        logging.info(f"Reading CSV: {file_path}")
        return pd.read_csv(file_path)


    def extract_from_json(self, file_path):
        logging.info(f"Reading JSON: {file_path}")
        return pd.read_json(file_path, lines=True)


    def extract_from_xml(self, file_path):
        logging.info(f"Reading XML: {file_path}")
        df = pd.DataFrame(columns=self.columns)
        tree = ET.parse(file_path)
        root = tree.getroot()
        for person in root:
            row = {}
            for col in self.columns:
                el = person.find(col)
                row[col] = float(el.text) if col in ['height', 'weight'] else el.text
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        return df


    def extract_all(self) :
        logging.info("Starting data extraction")
        combined_df = pd.DataFrame(columns = self.columns)
        extractors = [
                        ("*.csv", self.extract_from_csv),
                        ("*.json", self.extract_from_json),
                        ("*.xml", self.extract_from_xml)
            
                      ]
        for pattern, extractor in extractors :
            for file_path in glob.glob(os.path.join(self.source_path, pattern)) :
                df = extractor(file_path)
                if not df.empty :
                    combined_df = pd.concat([combined_df, df] , ignore_index=True)
        logging.info("Extraction phase ended.")
        return combined_df


    def transform (self, df) :
        logging.info("Transform phase started.")
        
        df['height'] = round((df['height'] * 0.0254), 2)
        df['weight'] = round((df['weight'] * 0.45359237), 2)
        
        logging.info("Transform phase ended.")
        
        return df

    def load (self, appended_df) :
        appended_df.to_csv(self.output_file_path, index=False)

        logging.info(f"Load phase completed. File saved to {target_path}")
        print(f"Data saved to: {target_path}")


    def run (self) :
        logging.info("ETL Job Started.")
        
        df_extracted = self.extract_all()
        df_transformed = self.transform(df_extracted)
        self.load(df_transformed)

        logging.info("ETL Job Ended.")
        


# In[8]:


etl = ETLAppend(source_path, target_path, expected_columns)

