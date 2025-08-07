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

def write_json_file(write_where: str, write_what: dict) -> None:
    with open(write_where, "w") as outfile:
        json.dump(write_what, outfile)

def read_json_file(read_where: str) -> dict:
    with open(read_where, "r") as infile:
        return json.load(infile)

def make_sales_dict(read_what: str, pn: str, qty: str, factor: str) -> dict:
    df = pd.read_csv(read_what)
    df.columns = [pn, qty, factor]
    df[qty] = df[qty] * df[factor]
    df = df.groupby(constants.PN, as_index=False).sum()
    return dict(zip(df[pn], df[qty]))

def prep_data():

    data_df = pd.read_csv(constants.DATA_TXT)
    translate_dict = dict(zip(data_df[constants.PN], data_df[constants.CR1]))
    write_json_file(constants.TRANSLATE_JSON, translate_dict)

    valid_df = pd.read_csv(constants.VALIDATE_CSV)
    valid_dict = dict(zip(valid_df["TOKI"], valid_df["TLI"]))
    write_json_file(constants.VALIDATE_JSON, valid_dict)

    bl_dict = make_sales_dict(constants.BL_TXT, constants.PN, constants.QTY, constants.FACTOR)
    write_json_file(constants.BL_JSON, bl_dict)

    hfr_dict = make_sales_dict(constants.HFR_TXT, constants.PN, constants.QTY, constants.FACTOR)
    write_json_file(constants.HFR_JSON, hfr_dict)


def build_schedule():

    validation = read_json_file(constants.VALIDATE_JSON)
    translation = read_json_file(constants.TRANSLATE_JSON)

    html = requests.get(constants.URL)

    soup = BeautifulSoup(html.content, "html.parser")
    table = soup.find_all("table")
    df = pd.read_html(str(table))[0]

    df = df.drop(columns=[1, 2, 3, 4])
    df.columns = range(df.columns.size)
    dates = df.iloc[3][1:]

    with open(constants.DATES_JSON, "w") as outfile:
        json.dump(dates.to_list(), outfile)

    df = df.replace(np.nan, 0)
    df = df[5:]
    df.reset_index(drop=True, inplace=True)

    for index, row in df.iterrows():
        temp = re.split("[^\\w-][^\\d.]+", row[0])  # type: ignore
        df.at[index, 0] = temp[0]

    for column in df.columns[1:]:
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
        if key == constants.IGNORE_ME:
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

    with open(constants.SCHEDULE_JSON, "w") as outfile:
        json.dump(schedule, outfile)


def build_report():

    with open(constants.DATES_JSON, "r") as infile:
        dates = json.load(infile)

    enumerated_dates = []

    for index, date in enumerate(dates):
        enumerated_dates.append(f"{date}-{index + 1}")

    constants.HEADER.extend(enumerated_dates)

    df = pd.DataFrame(columns=constants.HEADER)  # type: ignore
    data = pd.read_csv(constants.DATA_TXT)
    df[constants.PN] = data[constants.PN]
    df[constants.OH] = data[constants.QRT]
    df[constants.OO] = data[constants.QPO]
    df[constants.RO] = data[constants.MSL]
    df = df.replace(np.nan, 0)

    with open(constants.BL_JSON, "r") as infile:
        bl = json.load(infile)

    with open(constants.HFR_JSON, "r") as infile:
        hfr = json.load(infile)

    with open(constants.SCHEDULE_JSON, "r") as infile:
        schedule = json.load(infile)

    for row in df.index:
        key = df.loc[row, constants.PN]

        if key in bl:
            df.loc[row, constants.BL] = bl[key]

        if key in hfr:
            df.loc[row, constants.HFR] = hfr[key]
            df.loc[row, constants.REL] = bl[key] - hfr[key]
        if key in schedule:
            for index, date in enumerate(enumerated_dates):
                val = (schedule[key])[index]
                df.loc[row, date] = val

        df.loc[row, constants.T_AVAIL] = (
            df.loc[row, constants.OH]
            + df.loc[row, constants.OO]
            - df.loc[row, constants.BL]
        )

        df.loc[row, constants.R_AVAIL] = (
            df.loc[row, constants.T_AVAIL] + df.loc[row, constants.HFR]
        )

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
