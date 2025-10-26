import requests
import pandas as pd
import xmltodict
import json
from io import BytesIO
import os

# --- Configuration ---

# Base API URL for OECD
OECD_API_BASE = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetData"

# The directory to save data to. We make sure this directory exists.
# This script will save files to the "data/" folder in your project.
DATA_DIR = "data"

# --- YOUR MAIN SHOPPING LIST ---
# This is the most important part of the file.
# Add all the datasets you want to download here.
#
# Format:
# {
#   "name": "UNIQUE_FILENAME_YOU_CHOOSE",
#   "dataset_code": "OECD_DATASET_ID",
#   "query": "THE_SPECIFIC_DATA_QUERY"
# },
#
datasets_to_fetch = [
    {
        "name": "USA_GDP_QUARTERLY",
        "dataset_code": "QNA",
        "query": "USA.B1_GE.CPCARSA.Q"
    },
    
    # --- ADD MORE DATASETS BELOW ---
    # As an example, I will add Inflation (CPI) for Japan
    {
        "name": "JPN_INFLATION_YOY",
        "dataset_code": "KEI",  # Key Economic Indicators
        "query": "JPN.CPI.TOT.IXOB.GY.M" # Japan, CPI, Total, Year-on-Year Growth, Monthly
    },
    
    # Example: 3-Month Interest Rates for the Euro Area (19 countries)
    {
        "name": "EURO_AREA_3M_INTEREST_RATE",
        "dataset_code": "MEI_FIN", # Main Economic Indicators - Financial
        "query": "EA.IR3T.M" # Euro Area, 3-Month Interest Rate, Monthly
    },

    # Example: Unemployment Rate for the United Kingdom
    {
        "name": "UK_UNEMPLOYMENT_RATE",
        "dataset_code": "KEI", # Key Economic Indicators
        "query": "GBR.LR.LREM64TT.STSA.M" # Great Britain, Labour, Unemployment Rate, Seasonally Adjusted, Monthly
    },

    # Example: Long-term interest rates for Australia
    {
        "name": "AUS_LONG_INTEREST_RATE",
        "dataset_code": "MEI_FIN",
        "query": "AUS.IRLT.M" # Australia, Long-Term Interest Rate, Monthly
    }
]
# -------------------------------


def fetch_oecd_data(dataset_code, query="ALL"):
    """
    Fetch data from OECD SDMX API and return as Python dictionary (from JSON)
    """
    # Build the full URL for the API request
    url = f"{OECD_API_BASE}/{dataset_code}/{query}?contentType=xml"
    print(f"Fetching from: {url}")
    
    # Make the request to the OECD server
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code != 200:
        # If not successful, raise an error
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}. Response: {response.text}")

    # --- Convert Data ---
    # 1. The data comes as XML. We parse the XML into a Python dictionary.
    data = xmltodict.parse(response.content)
    
    # 2. We convert it to JSON format (this makes it cleaner)
    # and then back into a Python dictionary.
    json_data = json.loads(json.dumps(data))
    
    return json_data

# This is the main part of the script that runs
if __name__ == "__main__":
    print("=======================================")
    print("STARTING OECD AUTOMATIC UPDATE SCRIPT")
    print("=======================================")
    
    # Create the 'data/' directory if it doesn't already exist
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"Created directory: {DATA_DIR}")

    total_datasets = len(datasets_to_fetch)
    success_count = 0

    # --- Loop through every dataset in the 'datasets_to_fetch' list ---
    for i, dataset in enumerate(datasets_to_fetch):
        print(f"\n--- Processing {i+1}/{total_datasets}: {dataset['name']} ---")
        
        try:
            # 1. Fetch the data using the function we defined above
            data = fetch_oecd_data(dataset['dataset_code'], dataset['query'])
            
            # 2. Define the save path
            # This uses the 'name' to create a unique filename
            # e.g., "data/USA_GDP_QUARTERLY.json"
            file_path = os.path.join(DATA_DIR, f"{dataset['name']}.json")
            
            # 3. Save the data to its own file
            # The 'w' means 'write' (it will overwrite the old file)
            # indent=2 makes the JSON file nice and readable
            with open(file_path, "w") as f:
                json.dump(data, f, indent=2)
            
            print(f"✅ Successfully saved to '{file_path}'")
            success_count += 1

        except Exception as e:
            # If anything goes wrong (e.g., dataset code is wrong, OECD server is down)
            # this 'try...except' block will catch the error.
            # It will print the error message and continue to the next dataset.
            # This stops the whole script from crashing.
            print(f"❌ ERROR fetching or saving {dataset['name']}:")
            print(f"   {e}")
            # We continue to the next loop item
            pass
    
    # --- Script Finished ---
    print("\n=======================================")
    print(f"SCRIPT COMPLETE. ")
    print(f"Successfully fetched {success_count} / {total_datasets} datasets.")
    print("=======================================")
