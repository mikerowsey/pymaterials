from io import StringIO
from pathlib import Path
import json
import logging
import re
from typing import Dict, List

import numpy as np
import pandas as pd
import requests

log = logging.getLogger(__name__)

def save_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def make_dict(csv_path: Path, pn: str, qty: str, factor: str) -> Dict[str, float]:
    df = pd.read_csv(csv_path)
    df = df.rename(columns={df.columns[0]: pn, df.columns[1]: qty, df.columns[2]: factor})
    df[qty] = pd.to_numeric(df[qty], errors="coerce").fillna(0) * pd.to_numeric(df[factor], errors="coerce").fillna(1)
    out = df.groupby(pn, as_index=False, dropna=False)[qty].sum()
    return dict(zip(out[pn], out[qty]))

def build_schedule(url: str, validate_path: Path, translate_path: Path, dates_out: Path, schedule_out: Path) -> None:
    validation = load_json(validate_path)
    translation = load_json(translate_path)

    resp = requests.get(url, timeout=20)
    resp.raise_for_status()

    tables = pd.read_html(resp.text, flavor="lxml")
    if not tables:
        raise ValueError("No tables found at URL")
    df = tables[0]

    # robust column trimming / header detection here…
    # get dates row explicitly
    # clean first column:
    df = df.replace(np.nan, 0)
    df.columns = range(df.shape[1])

    dates = df.iloc[3, 1:].tolist()
    save_json(dates_out, dates)

    df = df.iloc[5:].reset_index(drop=True)  # consider guarding with shape checks
    df[0] = df[0].astype(str).str.extract(r"^([\w.-]+)", expand=False)

    num_cols = df.columns[1:]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    df = df.groupby(0, as_index=False).sum()

    # validate keys
    df[0] = df[0].map(lambda k: validation.get(k, k))

    mult = pd.Series(translation)
    missing = df[0][~df[0].isin(mult.index)].unique().tolist()
    if missing:
        raise KeyError(f"Missing translations for: {missing[:10]}{'…' if len(missing)>10 else ''}")

    df[num_cols] = df[0].map(mult).to_frame().values * df[num_cols].values

    schedule = df.set_index(0).to_dict(orient="list")
    save_json(schedule_out, schedule)



def save_schedule():
    resp = requests.get("https://www.toki.co.jp/purchasing/TLIHTML.files/sheet001.htm", timeout=20)
    resp.raise_for_status()
    tables = pd.read_html(StringIO(resp.text), flavor="lxml")
    if not tables:
        raise ValueError("No tables found at URL")
    df = tables[0]
    dates = df.iloc[3, 1:].tolist()
    dates = dates[4:]
    df = df[5:]
    df = df.drop([1, 2, 3, 4], axis=1)
    for index, row in df.iterrows():
        temp = re.split("[^\\w-][^\\d.]+", row[0])  # type: ignore
        df.at[index, 0] = temp[0]

    df = df.replace([r'\x81@',np.nan], 0, regex=True)
    df = df.groupby(0, as_index=False).sum()
    df.reset_index(drop=True, inplace=True)
    df.columns = range(df.shape[1])
    print(df)


save_schedule()