# query_from_saved.py
import json, pandas as pd
from pathlib import Path

PATH = Path("oecd_json_pipeline/oecd_data.json")  # update path if different

def load_parsed_from_xmljson(path):
    j = json.loads(path.read_text())
    # attempt to find common keys from xmltodict shape:
    # Many xml->dict outputs put data under keys like 'GenericData' -> 'DataSet' -> 'Series'
    # We try a few common patterns defensively.
    def get_series_list(d):
        # pattern 1: GenericData / DataSet / Series
        try:
            return d['GenericData']['DataSet']['Series']
        except:
            pass
        # pattern 2: CompactData / DataSet / Series
        try:
            return d['CompactData']['DataSet']['Series']
        except:
            pass
        # pattern 3: Raw dict that might have message > dataSets...
        if isinstance(d, dict):
            # try the SDMX-JSON-like structure conversion
            try:
                return d['DataSet']['Series']
            except:
                pass
        raise RuntimeError("Could not find Series list in JSON - inspect the file manually.")
    series_list = get_series_list(j)
    # Normalize series_list to list
    if isinstance(series_list, dict):
        # single series case: wrap
        series_list = [series_list]
    records = {}
    times = set()
    for s in series_list:
        # get series key values
        sk = s.get('SeriesKey', {}).get('Value', [])
        if isinstance(sk, list):
            label = ".".join([v.get('@value','') for v in sk])
        else:
            label = sk.get('@value','series')
        obs = s.get('Obs', [])
        if isinstance(obs, dict):
            obs = [obs]
        tmp = {}
        for o in obs:
            # SDMX generic uses ObsDimension/@value and ObsValue/@value
            t = o.get('ObsDimension', {}).get('@value') or o.get('Time', {}).get('#text')
            v = o.get('ObsValue', {}).get('@value') or o.get('ObsValue', {}).get('#text')
            times.add(t)
            try:
                tmp[t] = float(v) if v is not None else None
            except:
                tmp[t] = v
        records[label] = tmp
    times = sorted(times)
    df = pd.DataFrame({k: [records[k].get(t) for t in times] for k in records}, index=times)
    df.index.name = "period"
    return df

def main():
    p = Path("oecd_json_pipeline/oecd_data.json")
    if not p.exists():
        print("Saved file not found at", p)
        return
    df = load_parsed_from_xmljson(p)
    # show columns to find USA GDP series
    print("Columns found (sample):", df.columns[:30].tolist())
    # find likely USA GDP columns (heuristic)
    usa_cols = [c for c in df.columns if "USA" in c and "B1_GE" in c]
    if not usa_cols:
        usa_cols = [c for c in df.columns if "USA" in c]
    print("Matched USA columns:", usa_cols)
    # filter for 2022-2024
    mask = [i for i in df.index if i.startswith("2022") or i.startswith("2023") or i.startswith("2024")]
    print("\nValues for 2022-2024:\n")
    print(df.loc[mask, usa_cols])

if __name__ == "__main__":
    main()
