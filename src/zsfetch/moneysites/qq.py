# -*- coding: utf-8 -*-

import logging
import time
import pandas as pd
import requests
import json

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

gcr_ohlc_columns = [
    'd',
    'o',
    'c',
    'h',
    'l',
    'v'
]


def get_gcr_ohlc(gcr_index='sh204001', count_of_recent_trading_days=360, retry=3, pause=1):
    '''
    http://web.ifzq.gtimg.cn/appstock/app/kline/kline?_var=kline_day&param=sh204001,day,,,10,
    :param gcr_index:
    :param count_of_recent_trading_days:
    :param retry:
    :param pause:
    :return:
    '''
    if len(gcr_index) != 8:
        raise Exception("Wrong gcr index format:{}".format(gcr_index))
    headers = {
        'Host': 'web.ifzq.gtimg.cn',
        'Referer': 'http://gu.qq.com/sh204001/zq'
    }
    url = "http://web.ifzq.gtimg.cn/appstock/app/kline/kline?"
    params = {
        '_var': 'kline_day',
        'param': '{},day,,,{},'.format(gcr_index, count_of_recent_trading_days)
    }
    logger.debug("url:{} params:{}".format(url, params))
    for _ in range(retry):
        try:
            resp = requests.get(url, params=params, headers=headers)
            text = resp.text.replace("kline_day=", "")
            js = json.loads(text)
            ohlc = js['data'][gcr_index]['day']
            # convert list of list to list of dict
            for i in range(len(ohlc)):
                ohlc_dict = {}
                for j in range(len(gcr_ohlc_columns)):
                    ohlc_dict[gcr_ohlc_columns[j]] = ohlc[i][j]
                ohlc_dict['v'] = int(float(ohlc_dict['v']))
                ohlc[i] = ohlc_dict

            df = pd.DataFrame(ohlc, columns=gcr_ohlc_columns)
            return df
        except Exception as e:
            logger.warning(e)
        time.sleep(pause)

    return pd.DataFrame(columns=gcr_ohlc_columns)


