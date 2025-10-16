import json
import pandas as pd

# Load OECD JSON
json_file = "oecd_json_pipeline/oecd_data.json"
with open(json_file, "r") as f:
    data = json.load(f)

# SDMX JSON structure: dataSets -> series
series_block = data.get("dataSets", [{}])[0].get("series", {})
structure = data.get("structure", {}).get("dimensions", {}).get("series", [])
series_values = [[v["id"] for v in dim.get("values", [])] for dim in structure]

# Create DataFrame
records = {}
times = [v["id"] for v in data.get("structure", {}).get("dimensions", {}).get("observation", [])[0].get("values", [])]

for key, val in series_block.items():
    parts = key.split(":")
    colname = ".".join([series_values[i][int(p)] for i, p in enumerate(parts)])
    obs = val.get("observations", {})
    series_data = [None]*len(times)
    for idx, v in obs.items():
        series_data[int(idx)] = float(v[0]) if isinstance(v, list) else float(v)
    records[colname] = series_data

df = pd.DataFrame(records, index=pd.Index(times, name="period"))

# Example: filter USA GDP from 2022-Q1 to 2024-Q4
df_usa = df[[c for c in df.columns if "USA" in c]]
df_usa = df_usa.loc["2022-Q1":"2024-Q4"]

print(df_usa)
python query_gdp.py
 filter USA GDP from 2022-Q1 to 2024-Q4
