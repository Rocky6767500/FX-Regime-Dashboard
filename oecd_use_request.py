import requests
import pandas as pd
from pathlib import Path
import json

# CONFIG
JSON_URL = "https://sdmx.oecd.org/public.sdmxapi/rest/data/QNA/Q..USA+CAN+JPN+GBR+DEU+FRA+ITA+AUS.B1_GE.CQRSA/all?startTime=2000-Q1&endTime=2025-Q3&contentType=json"
OUTPUT_PARQUET = Path("g7_gdp.parquet")

# FETCH JSON
resp = requests.get(JSON_URL, timeout=60)
resp.raise_for_status()
data = resp.json()

# SAVE RAW JSON (optional)
Path("raw_oecd.json").write_text(json.dumps(data))

# PARSE JSON into pandas DataFrame
def parse_sdmx_json(data):
    # Extract time labels
    obs_dims = data.get("structure", {}).get("dimensions", {}).get("observation", [])
    times = [v["id"] for v in obs_dims[0]["values"]] if obs_dims else []

    # Series dimensions
    series_dims = data.get("structure", {}).get("dimensions", {}).get("series", [])
    series_dim_values = [[v["id"] for v in sd.get("values", [])] for sd in series_dims]

    # Series block
    series_block = data.get("dataSets", [{}])[0].get("series", {})
    records = {}

    for s_key, s_obj in series_block.items():
        parts = s_key.split(":")
        labels = [series_dim_values[i][int(p)] for i,p in enumerate(parts)]
        colname = ".".join(labels)

        obs = s_obj.get("observations", {})
        vals = [None]*len(times)
        for idx, v in obs.items():
            pos = int(idx)
            val = v[0] if isinstance(v, list) else v
            try: val = float(val)
            except: pass
            vals[pos] = val
        records[colname] = vals

    df = pd.DataFrame(records, index=pd.Index(times, name="period"))
    return df

df = parse_sdmx_json(data)

# SAVE PARQUET
df.to_parquet(OUTPUT_PARQUET)
print("Saved DataFrame to", OUTPUT_PARQUET)

# OPTIONAL: quick query example
country = "CAN"
period = "2022-Q1"
cols = [c for c in df.columns if country in c]
print(df.loc[df.index == period, cols])
