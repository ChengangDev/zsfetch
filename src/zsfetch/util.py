
from datetime import datetime
import logging as lg
import pandas as pd

logger = lg.getLogger(__name__)
logger.setLevel(lg.WARNING)

calendar_columns = [
    "exchangeCD",
    "calendarDate",
    "isOpen",
    "prevTradeDate",
    "isWeekEnd",
    "isMonthEnd",
    "isQuarterEnd",
    "isYearEnd"
]


class Calendar:
    def __init__(self, csv='marketcalendar.csv'):
        self._cal = pd.DataFrame()
        self._cal = pd.DataFrame.from_csv(path=csv)


def isodate_to_milliseconds(isodate='2000-01-01'):
    if isodate.index('-') != 4:
        raise Exception("Wrong iso date format:{}".format(isodate))
    epoch = datetime.utcfromtimestamp(0)
    d = datetime.strptime(isodate, '%Y-%m-%d')
    ms = (d - epoch).total_seconds() * 1000
    return int(ms)


def check_isodatestr_or_raise(date):
    if len(date) != 10 or not isinstance(date, str) or date[4] != '-' or date[7] != '-':
        raise Exception("Wrong isoformat date:{}", date)


def check_same_columns_or_raise(col1, col2):
    ok = True
    if len(col1) == len(col2):
        for c in col1:
            if c not in col2:
                ok = False
                break
    else:
        ok = False

    if not ok:
        raise Exception("columns not match:\n{}\n{}".format(col1, col2))

