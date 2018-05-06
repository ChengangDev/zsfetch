import pymongo
import logging as lg
import pandas as pd
from zsfetch.optionsites import sina
from zsfetch.optionsites import sse

logger = lg.getLogger(__name__)
logger.setLevel(lg.WARNING)

COL_OPTION_INDEX = 'option_index'
COL_OPTION_CODE = 'option_code'
COL_MILLISECONDS = 'milliseconds'               # from epoch
COL_TRADE_DATE = 'trade_date'                   # yyyy-mm-dd

daily_summary_columns = [
    COL_OPTION_INDEX,
    COL_OPTION_CODE,
    COL_TRADE_DATE
] + sse.daily_summary_columns

greeks_columns = [
    COL_OPTION_INDEX,
    COL_OPTION_CODE,
    COL_TRADE_DATE
] + sse.greeks_columns

ohlc_columns = [
    COL_OPTION_INDEX,
    COL_TRADE_DATE,            # yyyy-mm-dd
    COL_MILLISECONDS           # from epoch
] + sina.ohlc_columns


class OptionDB:
    '''

    '''
    def __init__(self, dbname='option'):
        if dbname == '':
            dbname = 'option'
        logger.info("using db:{}".format(dbname))
        self._client = pymongo.MongoClient()
        self._db = self._client[dbname]
        self._coll_ohlc = self._db['ohlc']                 # from sina
        self._coll_daily_summary = self._db['daily_summary']  # from sse
        self._coll_greeks = self._db['greeks']                # from sse
        # self._coll_contract = self._db['contract']

    def get_ohlc(self, option_index, fr_ms=-1, to_ms=-1):
        '''

        :param option_index:
        :param fr_ms:
        :param to_ms:
        :return: DataFrame
        '''
        filter = {}
        if len(option_index) == 0 or option_index is None:
            raise Exception("option_index can not be empty")
        elif isinstance(option_index, list):
            filter[COL_OPTION_INDEX] = {"$in": option_index}
        elif isinstance(option_index, str) or isinstance(option_index, int):
            filter[COL_OPTION_INDEX] = option_index
        else:
            raise Exception("Wrong dtype of option_index:{}".format(option_index))
        within = {}
        if fr_ms != -1:
            within['$gte'] = fr_ms
        if to_ms != -1:
            within['$lte'] = to_ms
        if bool(within):
            filter[COL_MILLISECONDS] = within
        logger.debug("filter:{}".format(filter))
        res = list(self._coll_ohlc.find(filter))
        logger.debug("results:{}".format(len(res)))
        ohlc = pd.DataFrame(res)
        return ohlc

    def upsert_ohlc(self, ohlc):
        '''

        :param ohlc: DataFrame
        :return:
        '''
        for i, row in ohlc.iterrows():
            option_index = row[COL_OPTION_INDEX]
            milliseconds = row[COL_MILLISECONDS]
            if option_index is None or milliseconds is None:
                logger.error("Wrong format, no index:{}".format(row))
                continue
            filter = {COL_OPTION_INDEX: option_index, COL_MILLISECONDS: milliseconds}
            val = {"$set": row.to_dict()}
            result = self._coll_ohlc.update_many(filter, val, True)
            logger.debug("upsert ohlc:{}:{} matched:{} modified:{}"
                         .format(i, filter, result.matched_count, result.modified_count))

    def get_daily_summary(self):
        '''
        recent trading day summary, may be out of date.
        :return:
        '''
        res = list(self._coll_daily_summary.find().sort(COL_TRADE_DATE, pymongo.DESCENDING).limit(1))
        filter = {COL_TRADE_DATE: '-1'}
        if len(res) == 1:
            recent_date = res[0][COL_TRADE_DATE]
            logger.debug("recent trading date:{}".format(recent_date))
            filter = {COL_TRADE_DATE: recent_date}
        else:
            logger.warning("this is an empty summary")
        res = list(self._coll_daily_summary.find(filter))
        logger.debug("count:{}".format(len(res)))
        summary = pd.DataFrame(res)
        return summary

    def upsert_daily_summary(self, daily_summary):
        for _, row in daily_summary.iterrows():
            option_index = row[COL_OPTION_INDEX]
            trade_date = row[COL_TRADE_DATE]  # yyyy-mm-dd
            if option_index is None or trade_date is None or trade_date.count('-') != 2:
                logger.error("Wrong format, index or date:{}".format(row))
                continue
            filter = {COL_OPTION_INDEX: option_index, COL_TRADE_DATE: trade_date}
            val = {"$set": row.to_dict()}
            result = self._coll_daily_summary.update_many(filter, val, True)
            logger.debug("update:{}{}".format(filter, result))

    def upsert_greeks(self, greeks):
        for _, row in greeks.iterrows():
            option_index = row[COL_OPTION_INDEX]
            trade_date = row[COL_TRADE_DATE]  # yyyy-mm-dd
            if option_index is None or trade_date is None or trade_date.count('-') != 2:
                logger.error("Wrong format, index or date:{}".format(row))
                continue
            filter = {COL_OPTION_INDEX: option_index, COL_TRADE_DATE: trade_date}
            val = {"$set": row.to_dict()}
            result = self._coll_greeks.update_one(filter, val, True)
            logger.debug("update:{}{}".format(filter, result))

    def get_greeks(self, option_index, fr_ms=-1, to_ms=-1):
        '''

        :param option_index:
        :param fr_ms: -1
        :param to_ms: -1  default for all
        :return:
        '''
        filter = {}
        if len(option_index) == 0 or option_index is None:
            pass  # can be empty
        elif isinstance(option_index, list):
            filter[COL_OPTION_INDEX] = {"$in": option_index}
        elif isinstance(option_index, str) or isinstance(option_index, int):
            filter[COL_OPTION_INDEX] = option_index
        else:
            raise Exception("Wrong dtype of option_index:{}".format(option_index))
        within = {}
        if fr_ms != -1:
            within['$gte'] = fr_ms
        if to_ms != -1:
            within['$lte'] = to_ms
        if bool(within):
            filter[COL_MILLISECONDS] = within
        logger.debug("filter:{}".format(filter))
        res = list(self._coll_greeks.find(filter))
        logger.debug("results:{}".format(len(res)))
        greeks = pd.DataFrame(res)
        return greeks






