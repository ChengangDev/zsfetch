# -*- coding: utf-8 -*-

import datetime
import logging
import time
import pandas as pd
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


daily_summary_columns = [
    'CALL_OR_PUT',
    'CHANGEFLAG',
    'CONTRACTFLAG',
    'CONTRACT_ID',
    'CONTRACT_SYMBOL',
    'CONTRACT_UNIT',
    'DAILY_PRICE_DOWNLIMIT',
    'DAILY_PRICE_UPLIMIT',
    'DELISTFLAG',
    'DELIVERY_DATE',
    'END_DATE',                    #
    'EXERCISE_DATE',               # 行权日
    'EXERCISE_PRICE',
    'EXPIRE_DATE',
    'LMTORD_MAXFLOOR',
    'LMTORD_MINFLOOR',
    'MARGIN_RATIO_PARAM1',
    'MARGIN_RATIO_PARAM2',
    'MARGIN_UNIT',
    'MKTORD_MAXFLOOR',
    'MKTORD_MINFLOOR',
    'NUM',
    'OPTION_TYPE',
    'PRICE_LIMIT_TYPE',
    'ROUND_LOT',
    'SECURITYNAMEBYID',
    'SECURITY_CLOSEPX',
    'SECURITY_ID',
    'SETTL_PRICE',
    'START_DATE',                  # 上市日
    'TIMESAVE',
    'UNDERLYING_CLOSEPX',
    'UNDERLYING_TYPE',
    'UPDATE_VERSION'
]

greeks_columns = [
    'CONTRACT_ID',      # 510050C1804M02500
    'CONTRACT_SYMBOL',  # 50ETF购4月2500
    'CONTRACT_TYPE',    # "认购"
    'DELTA_VALUE',
    'GAMMA_VALUE',
    'IMPLC_VOLATLTY',
    'RHO_VALUE',
    'SECURITY_ID',      # 10001277
    'THETA_VALUE',
    'TRADE_DATE',       # yyyy-mm-dd
    'VEGA_VALUE'
]


def get_trading_option_daily_summary(asset_code='510050', retry=3, pause=1):
    '''

    :param asset_code:
    :param retry:
    :param pause:
    :return:
    '''
    headers = {
        'Host': 'query.sse.com.cn',
        'Referer': 'http://www.sse.com.cn/assortment/options/disclo/preinfo/'
    }
    url = "http://query.sse.com.cn/commonQuery.do?"
    params = {
        'isPagination': 'true',
        'expireDate': '',
        # 'timesave': '20180201',
        'securityId': asset_code,
        'sqlId': 'SSE_ZQPZ_YSP_GGQQZSXT_XXPL_DRHY_SEARCH_L',
        'pageHelp.pageSize': '10000',
        'pageHelp.pageNo': '1',
        'pageHelp.beginPage': '1',
        'pageHelp.cacheSize': '1',
        'pageHelp.endPage': '5'
    }
    logger.debug("url:{} params:{}".format(url, params))
    for _ in range(retry):
        try:
            resp = requests.get(url, params=params, headers=headers)
            js = resp.json()
            summary = js['pageHelp']['data']
            df = pd.DataFrame(summary, columns=daily_summary_columns)
            return df
        except Exception as e:
            logger.warning(e)

        time.sleep(pause)

    return pd.DataFrame(columns=daily_summary_columns)


def get_greeks(trade_date='', retry=3, pause=1):
    '''
    http://query.sse.com.cn/commonQuery.do?sqlId=SSE_ZQPZ_YSP_GGQQZSXT_YSHQ_QQFXZB_DATE_L&isPagination=false&trade_date=20180109
    :param trade_date:
    :param retry:
    :param pause:
    :return:
    '''
    if trade_date == '':
        trade_date = datetime.date.today().isoformat()[:10]
    trade_date = trade_date.replace('-', '')
    if len(trade_date) != 8:
        raise Exception("'{}' is not yyyy-mm-dd or yyyymmdd format".format(trade_date))
    headers = {
        'Host': 'query.sse.com.cn',
        'Referer': 'http://www.sse.com.cn/assortment/options/disclo/preinfo/'
    }
    url = "http://query.sse.com.cn/commonQuery.do?"
    params = {
        'isPagination': 'false',
        'trade_date': trade_date,
        'sqlId': 'SSE_ZQPZ_YSP_GGQQZSXT_YSHQ_QQFXZB_DATE_L'
    }
    logger.debug("url:{} params:{}".format(url, params))
    for _ in range(retry):
        try:
            resp = requests.get(url, params=params, headers=headers)
            js = resp.json()
            greeks = js['result']
            df = pd.DataFrame(greeks, columns=greeks_columns)
            return df
        except Exception as e:
            logger.warning(e)

        time.sleep(pause)

    return pd.DataFrame(columns=greeks_columns)
