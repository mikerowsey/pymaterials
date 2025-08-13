import pandas as pd
import requests
from pandas.core.interchange.dataframe_protocol import DataFrame


def get_raw_scedule() -> DataFrame:
    resp = requests.get("https://www.toki.co.jp/purchasing/TLIHTML.files/sheet001.htm",
                        timeout=20,
                        headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    tables = pd.read_html(resp.text)
    if not tables:
        raise ValueError("No <table> found at URL.")
    return tables[0]


if __name__ == "__main__":
    df = get_raw_scedule()
    print(df)