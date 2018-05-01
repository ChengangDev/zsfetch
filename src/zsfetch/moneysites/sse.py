# -*- coding: utf-8 -*-

import datetime
import logging
import time
import pandas as pd
import requests
from zsfetch.util import check_isodatestr_or_raise

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

ohlc_columns = [
    "change",  # :0.06,
    "changeData",  # :0.056,
    "closeMarketValue",  # :38964.33, #万元
    "closeNegoValue",  # :38964.33,
    "closePrice",  # :100.034,
    "closeProfitRate",  # :0.0,
    "closeTrAmt",  # :2171.18,
    "closeTrTx",  # :0.02,
    "closeTrVol",  # :21.7,
    "closeTxDate",  # :"2018-04-27",
    "evgAmt",  # :0.0,
    "evgPrice",  # :100.03399999999999,
    "evgVol",  # :0.0,
    "id",  # :"511600",
    "last_close_price",  # :99.978,
    "maxHighPrice",  # :100.05,
    "maxHighPriceDate",  # :"2018-04-27",
    "maxTrAmt",  # :2171.18,
    "maxTrAmtDate",  # :"2018-04-27",
    "maxTrVol",  # :21.7,
    "maxTrVolDate",  # :"2018-04-27",
    "minLowPrice",  # :100.026,
    "minLowPriceDate",  # :"",
    "minTrAmt",  # :0.0,
    "minTrAmtDate",  # :"2018-04-27",
    "minTrVol",  # :0.0,
    "minTrVolDate",  # :"",
    "openPrice",  # :100.03,
    "openTxDate",  # :"2018-04-27",
    "productName",  # :"货币ETF",
    "totalAmt",  # :2171.18,  万元
    "totalChange",  # :0.02,
    "totalExchRate",  # :5.5722,
    "totalPrice",  # :2.1711779496E7,
    "totalTx",  # :0.02,
    "totalTxDate",  # :0.0,
    "totalVol",  # :21.7
]

share_columns = [
    "ETF_TYPE",
    "NUM",
    "SEC_CODE",
    "SEC_NAME",
    "STAT_DATE",
    "TOT_VOL"
]


def get_money_fund_ohlc(fund_index='511990', date='2018-04-27', retry=3, pause=1):
    check_isodatestr_or_raise(date)
    headers = {
        'Host': 'query.sse.com.cn',
        'Referer': 'http://www.sse.com.cn/assortment/fund/list/tcurrencyfundinfo/turnover/index.shtml'
    }
    url = "http://query.sse.com.cn/security/fund/queryAllQuatAbelNew.do?"
    params = {
        'searchDate': date,
        'FUNDID': fund_index
    }
    logger.debug("url:{} params:{}".format(url, params))
    for _ in range(retry):
        try:
            resp = requests.get(url, params=params, headers=headers)
            js = resp.json()
            ohlc = js['result'][0]
            df = pd.DataFrame(ohlc, index=[0], columns=ohlc_columns)
            return df
        except Exception as e:
            logger.warning(e)

        time.sleep(pause)

    return pd.DataFrame(columns=ohlc_columns)


def get_money_fund_share(date='2018-04-27', retry=3, pause=1):
    check_isodatestr_or_raise(date)
    headers = {
        'Host': 'query.sse.com.cn',
        'Referer': 'http://www.sse.com.cn/market/funddata/volumn/tcuvolumn/'
    }
    url = "http://query.sse.com.cn/commonQuery.do?"
    params = {
        'isPagination': 'false',
        'sqlId': 'COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_JYXJJ_SEARCH_L',
        'STAT_DATE': date
    }
    logger.debug("url:{} params:{}".format(url, params))
    for _ in range(retry):
        try:
            resp = requests.get(url, params=params, headers=headers)
            js = resp.json()
            fund_shares = js['result']
            df = pd.DataFrame(fund_shares, columns=share_columns)
            return df
        except Exception as e:
            logger.warning(e)

        time.sleep(pause)

    return pd.DataFrame(columns=share_columns)
