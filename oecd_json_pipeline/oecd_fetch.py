import requests
import pandas as pd
import xmltodict
import json
from io import BytesIO

OECD_API_BASE = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetData"

def fetch_oecd_data(dataset_code, query="ALL"):
    """Fetch data from OECD SDMX API and return as DataFrame"""
    url = f"{OECD_API_BASE}/{dataset_code}/{query}?contentType=xml"
    print(f"Fetching from: {url}")
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch: {response.status_code}")

    data = xmltodict.parse(response.content)
    json_data = json.loads(json.dumps(data))
    return json_data

if __name__ == "__main__":
    dataset_code = "QNA"   # Quarterly National Accounts (as example)
    query = "USA.B1_GE.CPCARSA.Q"
    data = fetch_oecd_data(dataset_code, query)
    with open("oecd_data.json", "w") as f:
        json.dump(data, f, indent=2)
    print("✅ OECD data fetched and saved as 'oecd_data.json'")


