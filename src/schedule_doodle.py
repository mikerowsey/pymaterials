import pandas as pd
import requests
from pandas import DataFrame


def get_schedule() -> DataFrame:
    """
    Fetch and normalize the schedule table.
    - Loads the first HTML table from the source URL.
    - Skips header rows and the first 4 date columns (as in original logic).
    - Cleans the first column to keep only PN-like tokens.
    - Converts numeric columns, groups by the first column, and sums quantities.
    Returns:
        pandas.DataFrame: grouped and summed schedule.
    """
    url = "https://www.toki.co.jp/purchasing/TLIHTML.files/sheet001.htm"
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()

    tables = pd.read_html(resp.text, flavor="lxml")
    if not tables:
        raise ValueError("No tables found at URL")

    df = tables[0]

    # Normalize columns to numeric indices for safer slicing
    df.columns = range(df.shape[1])

    # Extract dates row if needed elsewhere (kept for compatibility of structure)
    # Original logic used row 3 for dates and then skipped first 4 date columns.
    # We keep that behavior but don't return/use dates here.
    # dates = df.iloc[3, 1:].tolist()
    # dates = dates[4:]

    # Skip header rows
    df = df.iloc[5:, :].reset_index(drop=True)

    # Drop the first 4 date columns, keep column 0 + remaining date columns
    keep_cols = [0] + list(range(5, df.shape[1]))
    df = df.iloc[:, keep_cols]

    # Clean first column: keep leading token containing letters/digits/_/.- (vectorized)
    df[0] = df[0].astype(str).str.extract(r"^([\w.-]+)", expand=False)

    # Replace odd markers and coerce numerics
    df = df.replace({r"\x81@": 0}, regex=True)
    num_cols = df.columns[1:]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    # Group by cleaned key and sum across numeric columns
    df = df.groupby(0, as_index=False, dropna=False).sum()

    # Reindex columns as simple 0..N for consistency
    df.columns = range(df.shape[1])
    return df


print(get_schedule())