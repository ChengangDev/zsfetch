# -*- coding: utf-8 -*-

from urllib.request import urlopen, Request
import json
import logging
import time
import pandas as pd

logger = logging.getLogger(__name__)

assets = [
    {'code': '510050', 'name': '50ETF'},
]

T_columns = [
    'bid_num',             # 买量
    'bid',                 # 买价
    'latest_price',
    'ask',
    'ask_num',
    'hold',                # 持仓
    'change_ratio',        # 涨幅
    'strike',
    'pre_close',
    'open',
    'ceiling',
    'floor',

    'ask5',
    'ask5_num',
    'ask4',
    'ask4_num',
    'ask3',
    'ask3_num',
    'ask2',
    'ask2_num',
    'ask1',
    'ask1_num',
    'bid1',
    'bid1_num',
    'bid2',
    'bid2_num',
    'bid3',
    'bid3_num',
    'bid4',
    'bid4_num',
    'bid5',
    'bid5_num',

    'time',
    'is_main',
    'status_code',
    'type',
    'asset_code',
    'short_name',
    'amplitude',
    'high',
    'low',
    'volume',
    'amount',
    'label'
]

ohlc_columns = [
    'd',
    'o',
    'h',
    'l',
    'c',
    'v'
]

SINA_OPTION_INDEX_PREFIX = 'CON_OP_'


def get_trading_months(retry=3, pause=1):
    """

    :param retry:
    :param pause:
    :return: ['2018-04', '2018-05', '2018-06', '2018-09']
    """
    url = "http://stock.finance.sina.com.cn/futures/api/openapi.php/StockOptionService.getStockName"
    logger.debug(url)
    for _ in range(retry):
        try:
            req = Request(url)
            res = urlopen(req, timeout=9).read()
            js = json.loads(res.decode('utf-8'))

            # cate = js['result']['data']['cateList']
            months = js['result']['data']['contractMonth']

            return months[1:]
        except Exception as e:
            logger.warning(e)

        time.sleep(pause)
    return []


def get_trading_expire_date(year_month='2018-04', retry=3, pause=1):
    """

    :param year_month: e.g. '2018-07'
    :param retry:
    :param pause:
    :return: '2018-04-25'
    """
    if year_month.index('-') != 4 or len(year_month) != 7:
        raise Exception("Wrong year_month format:{0}.".format(year_month))
    url = "http://stock.finance.sina.com.cn/futures/api/openapi.php/StockOptionService.getRemainderDay"
    url = url + "?date=" + year_month
    logger.debug(url)
    for _ in range(retry):
        try:
            req = Request(url)
            res = urlopen(req, timeout=9).read()
            js = json.loads(res.decode('utf-8'))
            day = js['result']['data']['expireDay']
            return day
        except Exception as e:
            logger.warning(e)
        time.sleep(pause)
    return ''


def get_trading_option_list(asset_code, year_month, retry=3, pause=1):
    """

    :param asset_code: e.g. '510050'
    :param year_month: e.g. '2018-04'
    :param retry:
    :param pause:
    :return:  DataFrame(columns=columns, index=option_index_list)
    """
    if len(asset_code) != 6:
        raise Exception("Wrong asset code format:{0}.".format(asset_code))
    if len(year_month) != 7 or year_month.index('-') != 4:
        raise Exception("Wrong year_month format:{0}.".format(year_month))
    option_code_10 = "{0}{1}{2}".format(asset_code, year_month[2:4], year_month[5:])
    url = "http://hq.sinajs.cn/list=OP_UP_{0},OP_DOWN_{1}".\
        format(option_code_10, option_code_10)
    logger.debug(url)
    df = pd.DataFrame(columns=T_columns)
    for t in range(retry):
        try:
            req = Request(url)
            res = urlopen(req, timeout=9).read()
            str = res.decode('utf-8')
            list = str.split('\n')
            str_up = list[0][list[0].index('"')+1:list[0].rindex('"')]
            str_down = list[1][list[1].index('"')+1:list[1].rindex('"')]
            str_up_down = str_up + str_down
            str_up_down = str_up_down[:-1]      # trim last ','
            index = str_up_down.split(',')
            index = [e.replace(SINA_OPTION_INDEX_PREFIX, '') for e in index] # replace prefix

            td_url = "http://hq.sinajs.cn/list={0}".format(str_up_down)
            logger.debug(td_url)
            td_req = Request(td_url)
            td_res = urlopen(td_req, timeout=9).read()
        except Exception as e:
            logger.warning("Try {0} times".format(t+1))
        else:
            str = td_res.decode('gbk')
            list = str.split('\n')
            for i in range(len(list)):
                if len(list[i]) < 20:  # trim empty element
                    logger.debug("skip empty element:list[{0}]:{1}".format(i, list[i]))
                    continue
                str_hq = list[i][list[i].index('"')+1:list[i].rindex('"')]
                list_detail = str_hq.split(',')
                if len(list_detail) != len(T_columns):
                    logger.error(T_columns)
                    logger.error(list_detail)
                    raise Exception("Option detail mismatch.")
                df.loc[i] = list_detail

            df.index = index
            return df
        time.sleep(pause)
    return df


def get_trading_option_history_ohlc(option_index, retry=3, pause=1):
    """

    :param option_index: e.g. '10001209'  from get_trading_option_list()
    :param retry:
    :param pause:
    :return: DataFrame(columns=OHLC_columns)
    """
    if len(option_index) != 8:
        raise Exception("Wrong option_index format:{0}.".format(option_index))
    url = "http://stock.finance.sina.com.cn/futures/api/openapi.php/StockOptionDaylineService.getSymbolInfo" \
          "?symbol={0}{1}".format(SINA_OPTION_INDEX_PREFIX, option_index)
    logger.debug(url)
    df = pd.DataFrame(columns=ohlc_columns)
    for _ in range(retry):
        try:
            req = Request(url)
            res = urlopen(req, timeout=9).read()
            js = json.loads(res.decode('utf-8'))
            list_data = js['result']['data']
            df = pd.DataFrame(list_data)
            return df
        except Exception as e:
            logger.warning(e)
        time.sleep(pause)
    return df


