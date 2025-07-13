import json
import re
import requests
import numpy as np
import warnings
import pandas as pd
import constants
from bs4 import BeautifulSoup
from time import perf_counter

warnings.simplefilter(action="ignore", category=FutureWarning)


def prep_data():

    data_df = pd.read_csv(constants.DATA_TXT)
    translate_dict = dict(zip(data_df["Part Number"], data_df["Custom_Real_01"]))

    with open(".translation.json", "w") as outfile:
        json.dump(translate_dict, outfile)

    valid_df = pd.read_csv(constants.VALIDATE_CSV)
    valid_dict = dict(zip(valid_df["TOKI"], valid_df["TLI"]))

    with open(constants.VALIDATE_JSON, "w") as outfile:
        json.dump(valid_dict, outfile)

    bl_df = pd.read_csv(constants.BL)
    bl_df.columns = ["pn", "qty", "factor"]
    bl_df["qty"] = bl_df["qty"] * bl_df["factor"]
    bl_df = bl_df.groupby("pn", as_index=False).sum()
    bl_dict = dict(zip(bl_df["pn"], bl_df["qty"]))

    with open(constants.BL_JSON, "w") as outfile:
        json.dump(bl_dict, outfile)

    hfr_df = pd.read_csv(constants.HFR)
    hfr_df.columns = ["pn", "qty", "factor"]
    hfr_df["qty"] = hfr_df["qty"] * hfr_df["factor"]
    hfr_df = hfr_df.groupby("pn", as_index=False).sum()
    hfr_dict = dict(zip(hfr_df["pn"], hfr_df["qty"]))

    with open(constants.HFR_JSON, "w") as outfile:
        json.dump(hfr_dict, outfile)


def build_schedule():

    with open(".validation.json", "r") as infile:
        validation = json.load(infile)
    with open(".translation.json", "r") as infile:
        translation = json.load(infile)

    html = requests.get(constants.URL)

    soup = BeautifulSoup(html.content, "html.parser")
    table = soup.find_all("table")
    df = pd.read_html(str(table))[0]

    df = df.drop(columns=[1, 2, 3, 4])
    df.columns = range(df.columns.size)
    dates = df.iloc[3][1:]

    with open(".dates.json", "w") as outfile:
        json.dump(dates.to_list(), outfile)

    df = df.replace(np.nan, 0)
    df = df[5:]
    df.reset_index(drop=True, inplace=True)

    for index, row in df.iterrows():
        temp = re.split("[^\\w-][^\\d.]+", row[0])  # type: ignore
        df.at[index, 0] = temp[0]

    for column in df:
        if column > 0:
            df[column] = df[column].astype(float)

    df = df.groupby(0, as_index=False).sum()

    for index, row in df.iterrows():
        key = row[0]
        if key in validation:
            value = validation[key]
            df.at[index, 0] = value

    translated = True

    for index, row in df.iterrows():
        key = row[0]
        if key == "IGNORE_ME":
            print(f"Schedule Validation: Line #{index + 1} ignored")  # type: ignore
            continue
        if key in translation:
            value = translation[key]
            for i in range(1, row.size):
                df.at[index, i] = value * df.at[index, i]
        else:
            translated = False
            print(f"Schedule Validation: {key} not found.")

    if not translated:
        print(constants.VALIDATION_FAIL)
        exit(1)

    df.set_index(0, inplace=True)
    schedule = {}

    for index, row in df.iterrows():
        temp = []
        for item in row:
            temp.append(item)
        schedule[index] = temp

    with open(".schedule.json", "w") as outfile:
        json.dump(schedule, outfile)


def build_report():

    with open(".dates.json", "r") as infile:
        dates = json.load(infile)

    enumerated_dates = []

    for index, date in enumerate(dates):
        enumerated_dates.append(f"{date}-{index + 1}")

    constants.HEADER.extend(enumerated_dates)

    df = pd.DataFrame(columns=constants.HEADER)  # type: ignore
    data = pd.read_csv(constants.DATA_TXT)
    df[constants.PN] = data["Part Number"]
    df["On Hand"] = data["QtyRealTimeOnHand"]
    df["On Order"] = data["QtyOnPurchaseOrder"]
    df["Reorder"] = data["Minimum_Stock_Level"]
    df = df.replace(np.nan, 0)

    with open(constants.BL_JSON, "r") as infile:
        bl = json.load(infile)

    with open(constants.HFR_JSON, "r") as infile:
        hfr = json.load(infile)

    with open(constants.SCHEDULE_JSON, "r") as infile:
        schedule = json.load(infile)

    for row in df.index:
        key = df.loc[row, "Part Number"]

        if key in bl:
            df.loc[row, "Backlog"] = bl[key]

        if key in hfr:
            df.loc[row, "HFR"] = hfr[key]
            df.loc[row, "Released"] = bl[key] - hfr[key]
        if key in schedule:
            for index, date in enumerate(enumerated_dates):
                val = (schedule[key])[index]
                df.loc[row, date] = val

        df.loc[row, "T-Avail"] = (
            df.loc[row, "On Hand"] + df.loc[row, "On Order"] - df.loc[row, "Backlog"]
        )

        df.loc[row, "R-Avail"] = df.loc[row, "T-Avail"] + df.loc[row, "HFR"]

    writer = pd.ExcelWriter(constants.MATERIALS, engine="xlsxwriter")  # type: ignore

    df.to_excel(writer, sheet_name="Sheet1", index=False)
    writer.close()


if __name__ == "__main__":
    print(constants.COPYRIGHT)
    start = perf_counter()
    prep_data()
    build_schedule()
    build_report()
    end = perf_counter()
    print(f"Success: {(end - start):.3f} seconds")
