import pymongo
import logging as lg
import pandas as pd
from zsfetch.moneysites import sse

logger = lg.getLogger(__name__)
logger.setLevel(lg.WARNING)

COL_MONEY_FUND_INDEX = "money_fund_index"
COL_TRADE_DATE = "trade_date"
COL_MILLISECONDS = "milliseconds"

ohlc_columns = [
    COL_MONEY_FUND_INDEX,
    COL_TRADE_DATE,
    COL_MILLISECONDS
] + sse.ohlc_columns

share_columns = [
    COL_MONEY_FUND_INDEX,
    COL_TRADE_DATE,
    COL_MILLISECONDS
] + sse.share_columns


class MoneyDB:
    '''

    '''
    def __init__(self, dbname='money'):
        if dbname == '':
            dbname = 'money'
        self._client = pymongo.MongoClient()
        self._db = self._client[dbname]
        self._coll_dayline = self._db['ohlc']                 # from sse
        self._coll_share = self._db['share']                  # from sse

    def get_ohlc(self, fund_index, fr_ms=-1, to_ms=-1):
        filter = {}
        if len(fund_index) == 0 or fund_index is None:
            pass  # can be empty
        elif isinstance(fund_index, list):
            filter[COL_MONEY_FUND_INDEX] = {"$in": fund_index}
        elif isinstance(fund_index, str) or isinstance(fund_index, int):
            filter[COL_MONEY_FUND_INDEX] = fund_index
        else:
            raise Exception("Wrong dtype of fund_index:{}".format(fund_index))
        within = {}
        if fr_ms != -1:
            within['$gte'] = fr_ms
        if to_ms != -1:
            within['$lte'] = to_ms
        if bool(within):
            filter[COL_MILLISECONDS] = within
        logger.debug("filter:{}".format(filter))
        res = list(self._coll_dayline.find(filter))
        logger.debug("ohlc count:{}".format(len(res)))
        ohlc = pd.DataFrame(res)
        return ohlc

    def get_share(self, fund_index, fr_ms=-1, to_ms=-1):
        filter = {}
        if len(fund_index) == 0 or fund_index is None:
            pass  # can be empty
        elif isinstance(fund_index, list):
            filter[COL_MONEY_FUND_INDEX] = {"$in": fund_index}
        elif isinstance(fund_index, str) or isinstance(fund_index, int):
            filter[COL_MONEY_FUND_INDEX] = fund_index
        else:
            raise Exception("Wrong dtype of fund_index:{}".format(fund_index))
        within = {}
        if fr_ms != -1:
            within['$gte'] = fr_ms
        if to_ms != -1:
            within['$lte'] = to_ms
        if bool(within):
            filter[COL_MILLISECONDS] = within
        logger.debug("filter:{}".format(filter))
        res = list(self._coll_dayline.find(filter))
        logger.debug("share count:{}".format(len(res)))
        share = pd.DataFrame(res)
        return share

    def upsert_share(self, share):

        for _, row in share.iterrows():
            fund_index = row[COL_MONEY_FUND_INDEX]
            milliseconds = row[COL_MILLISECONDS]
            if fund_index is None or milliseconds is None:
                logger.error("Wrong format, no index:{}".format(row))
                continue
            filter = {COL_MONEY_FUND_INDEX: fund_index, COL_MILLISECONDS: milliseconds}
            val = {"$set": row.to_dict()}
            result = self._coll_share.update_many(filter, val, True)
            logger.debug("update:{}{}".format(filter, result))

