# -*- coding: utf-8 -*-

import logging
import time
import pandas as pd
import requests
from zsfetch.util import check_isodatestr_or_raise

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

share_columns = [
    # "ETF_TYPE",
    # "NUM",
    "SEC_CODE",
    "SEC_NAME",
    "STAT_DATE",
    "TOT_VOL"             #亿份
]

tracked_szse_money_fund = [
    {"SEC_CODE": "159001", "SEC_NAME": "保证金"},
    {"SEC_CODE": "159003", "SEC_NAME": "招商快线"},
    {"SEC_CODE": "159005", "SEC_NAME": "添富快钱"}
]


def get_money_fund_share(date='2018-04-27', retry=3, pause=1):
    check_isodatestr_or_raise(date)
    headers = {
        'Host': 'www.szse.cn',
        'Referer': 'http://www.szse.cn/main/marketdata/jypz/etflb/'
    }
    url = "http://www.szse.cn/main/marketdata/jypz/etflb/"
    params = {}
    logger.debug("url:{} params:{}".format(url, params))
    for _ in range(retry):
        try:
            resp = requests.get(url, params=params, headers=headers)
            span = resp.text
            span = span[:span.find("MESSAGE_ID")]
            span = span[:span.rfind("</span>")]
            date = span[span.rfind(">") + 1:]

            for fund in tracked_szse_money_fund:
                index_pos = resp.text.find("<u>{}</u>".format(fund['SEC_CODE']))
                td = resp.text[index_pos:]  # first td
                td = td[td.find("<td ") + 4:]   # second td
                td = td[td.find("<td ") + 4:]   # third td
                td = td[td.find("<td ") + 4:]   # forth td
                td = td[td.find(">") + 1:]
                td = td[:td.find("<")]
                td = td.replace(",", "")
                fund['TOT_VOL'] = float(td)/100000000
                fund['STAT_DATE'] = date
            df = pd.DataFrame(tracked_szse_money_fund, columns=share_columns)
            return df
        except Exception as e:
            logger.warning(e)

        time.sleep(pause)

    return pd.DataFrame(columns=share_columns)
