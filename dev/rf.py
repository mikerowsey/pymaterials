import json
from pathlib import Path

import pandas as pd
import requests

from src import constants
from src.materials import load_json


def build_schedule():
    # --- Load mappings
    validation = load_json(constants.VALIDATE_JSON)    # {raw_key: validated_key}
    translation = load_json(constants.TRANSLATE_JSON)  # {validated_key: factor}

    # --- Fetch HTML (no BeautifulSoup)
    resp = requests.get(constants.URL, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()

    # Parse all HTML tables; use the first one
    tables = pd.read_html(resp.text)
    if not tables:
        raise ValueError("No <table> found at URL.")
    df = tables[0]

    # --- Keep only needed columns and capture dates
    df = df.drop(columns=[1, 2, 3, 4])
    df.columns = range(df.shape[1])

    dates = df.iloc[3, 1:].tolist()
    Path(constants.DATES_JSON).write_text(json.dumps(dates))

    # --- Data rows and cleanup
    df = df.iloc[5:, :].reset_index(drop=True)

    # Normalize first column token
    first_col = df.iloc[:, 0].astype(str)
    df.iloc[:, 0] = first_col.str.extract(r'^([\w-]+)', expand=False).fillna(first_col)

    # Make numeric, fill NaN with 0
    numeric = df.iloc[:, 1:].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    df = pd.concat([df.iloc[:, [0]], numeric], axis=1)

    # Aggregate duplicates
    df = df.groupby(df.columns[0], as_index=False).sum()

    # Apply validation (rename keys) and ignore rows
    df.iloc[:, 0] = df.iloc[:, 0].replace(validation)
    df = df[df.iloc[:, 0] != constants.IGNORE_ME]

    # Check translation coverage and apply factors
    keys = df.iloc[:, 0]
    factors = keys.map(translation)
    missing = keys[factors.isna()].unique().tolist()
    if missing:
        for k in missing:
            print(f"Schedule Validation: {k} not found.")
        print(constants.VALIDATION_FAIL)
        raise SystemExit(1)

    df.iloc[:, 1:] = df.iloc[:, 1:].mul(factors.to_numpy(), axis=0)

    # --- Build schedule dict {key: [values...]} and save
    df = df.set_index(df.columns[0])
    schedule = {k: v for k, v in zip(df.index.tolist(), df.values.tolist())}
    Path(constants.SCHEDULE_JSON).write_text(json.dumps(schedule))

if __name__ == "__main__":
    build_schedule()
