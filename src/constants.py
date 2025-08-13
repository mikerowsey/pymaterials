COPYRIGHT = """
----------------------------------------------------------
Materials 15.2.0 \u00a92002-2025 Michael N. Rowsey
----------------------------------------------------------
"""

DATA_CSV = "../data/data.csv"
DATA_CSV = "../data/data.csv"
HFR_CSV = "../data/hfr.csv"
HFR_JSON = "../data/.hfr.json"
BL_CSV = "../data/bl.csv"
BL_JSON = "../data/.bl.json"
SCHEDULE_JSON = "../data/.schedule.json"
VALIDATE_CSV = "../data/validate.csv"
VALIDATE_JSON = "../data/.validate.json"
TRANSLATE_JSON = "../data/.translate.json"
DATES_JSON = "../data/.dates.json"
MATERIALS = "_materials.xlsx"

URL = "https://www.toki.co.jp/purchasing/TLIHTML.files/sheet001.htm"
VALIDATION_FAIL = "\nSchedule could not be validated. Please update validation.csv"

PN = "Part Number"
OH = "On Hand"
BL = "Backlog"
REL = "Released"
HFR = "HFR"
OO = "On Order"
T_AVAIL = "T-Avail"
R_AVAIL = "R-Avail"
RO = "Reorder"

QTY = "Quantity"
FACTOR = "Factor"
CR1 = "Custom_Real_01"
QRT = "QtyRealTimeOnHand"
QPO = "QtyOnPurchaseOrder"
MSL = "Minimum_Stock_Level"
IGNORE_ME = "IGNORE_ME"

HEADER = [
    PN,
    OH,
    BL,
    REL,
    HFR,
    OO,
    T_AVAIL,
    R_AVAIL,
    RO,
]
