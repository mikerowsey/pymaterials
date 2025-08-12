# import re
from io import StringIO
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
import requests

from time import perf_counter


def save_schedule(
    url: str = "https://www.toki.co.jp/purchasing/TLIHTML.files/sheet001.htm",
    timeout: int = 20,
    to_csv: Optional[str] = None,
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Fetch the Toki schedule table, clean it, and return:
      - A grouped numeric DataFrame
      - Corresponding date headers
    """

    # --- Fetch & decode HTML ---
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    html_text = response.content.decode("cp932", errors="ignore")

    # --- Parse tables ---
    table_list = pd.read_html(StringIO(html_text), flavor="lxml")
    if not table_list:
        raise ValueError("No tables found at URL")

    raw_table = table_list[0].copy()

    # --- Validate shape ---
    if raw_table.shape[0] < 6:
        raise ValueError(f"Unexpected table shape: needs >=6 rows, got {raw_table.shape[0]}")
    if raw_table.shape[1] < 6:
        raise ValueError(f"Unexpected table shape: needs >=6 cols, got {raw_table.shape[1]}")

    # --- Extract date headers ---
    date_list = [
        pd.to_datetime("today").strftime("%m/%d/%Y"),
        pd.to_datetime(raw_table.iloc[1, 0]).strftime("%m/%d/%Y")
    ]

    header_row_values = raw_table.iloc[3, 5:].tolist()
    if not header_row_values:
        raise ValueError("Not enough date cells found in header row.")

    # Convert header values to datetime strings and extend the list
    converted_dates = (
        pd.to_datetime(pd.Series(header_row_values), errors="coerce")
        .dt.strftime('%m/%d/%Y')
        .dropna()  # Optional: remove NaT values
        .tolist()
    )
    date_list.extend(converted_dates)

    # --- Slice data ---
    sliced_table = raw_table.iloc[5:, :].reset_index(drop=True)

    # Drop columns 1..4 by position
    columns_to_drop = sliced_table.columns[1:5]
    sliced_table = sliced_table.drop(columns=columns_to_drop, errors="ignore")

    # --- Normalize first column ---
    first_column_name = sliced_table.columns[0]
    sliced_table[first_column_name] = (
        sliced_table[first_column_name]
        .astype(str)
        .str.extract(r"^([\w-]+)", expand=False)
        .fillna(sliced_table[first_column_name].astype(str))
    )

    # --- Replace special codes & NaNs ---
    numeric_column_names = sliced_table.columns[1:]
    cleaned_numeric_data = (
        sliced_table[numeric_column_names]
        .replace([r"\x81@", r"\x81q"], "0", regex=True)
        .replace({np.nan: 0})
    )

    # --- Coerce to numeric ---
    cleaned_numeric_data = cleaned_numeric_data.apply(pd.to_numeric, errors="coerce").fillna(0)

    # --- Combine cleaned numeric data back ---
    cleaned_table = sliced_table.copy()
    cleaned_table[numeric_column_names] = cleaned_numeric_data

    # --- Group & sum ---
    grouped_table = (
        cleaned_table.groupby(first_column_name, as_index=False)[numeric_column_names]
        .sum(numeric_only=True)
        .reset_index(drop=True)
    )

    # --- Reset column names to 0..N-1 ---
    grouped_table.columns = range(grouped_table.shape[1])

    # --- Save if requested ---
    if to_csv:
        grouped_table.to_csv(to_csv, index=False)

    return grouped_table, date_list

if __name__ == "__main__":
    start = perf_counter()
    df, dates = save_schedule()
    end = perf_counter()
    print(f"Success: {(end - start):.3f} seconds")
    print(df)
    print(dates)

