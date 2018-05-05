# -*- coding: utf-8 -*-

import datetime
import logging
import time
import pandas as pd
import requests
import json
from zsfetch.util import check_isodatestr_or_raise

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

hsg_flow_columns = [
    'DateTime',  # "2014-11-17T00:00:00"
    'GGHSMoney',  # 1768
    'GGSSMoney',  # "-"
    'HSMoney',  # 13000
    'NorthMoney',  # 13000
    'SSMoney',  # "-"
    'SouthSumMoney'  # 1768
]


def get_hsg_flow(fr_date='', to_date='', retry=3, pause=1):
    '''
    http://dcfm.eastmoney.com/EM_MutiSvcExpandInterface/api/js/get?type=HSGTZJZS&token=70f12f2f4f091e459a279469fe49eca5&filter=(DateTime%3C^2018-04-19^)
    :param fr_date:
    :param to_date:
    :param retry:
    :param pause:
    :return:
    '''
    headers = {
        'Host': 'dcfm.eastmoney.com',
        'Referer': 'http://data.eastmoney.com/hsgt/index.html'
    }
    url = "http://dcfm.eastmoney.com/EM_MutiSvcExpandInterface/api/js/get?"
    params = {
        'type': 'HSGTZJZS',
        'token': '70f12f2f4f091e459a279469fe49eca5',
        # 'filter': '(DateTime>^{}^)'.format(fr_date)
    }
    fr_filter = ""
    to_filter = ""
    if len(fr_date) > 0:
        check_isodatestr_or_raise(fr_date)
        fr_filter = "DateTime>=^{}^".format(fr_date)
    if len(to_date) > 0:
        check_isodatestr_or_raise(to_date)
        to_filter = "DateTime<=^{}^".format(to_date)
    if len(fr_filter) != 0 and len(to_filter) != 0:
        params['filter'] = "({} and {})".format(fr_filter, to_filter)
    elif len(fr_filter) != 0:
        params['filter'] = "({})".format(fr_filter)
    elif len(to_filter) != 0:
        params['filter'] = "({})".format(to_filter)

    logger.debug("url:{} params:{}".format(url, params))
    for _ in range(retry):
        try:
            resp = requests.get(url, params=params, headers=headers)
            js = resp.json()
            flow = js
            df = pd.DataFrame(flow, columns=hsg_flow_columns)
            return df
        except Exception as e:
            logger.warning(e)
        time.sleep(pause)

    return pd.DataFrame(columns=hsg_flow_columns)


