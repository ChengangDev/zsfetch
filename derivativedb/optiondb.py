import pymongo
import datetime
import time
import logging as lg
import pandas as pd
from optionsites import sse, sina

logger = lg.getLogger(__name__)
logger.setLevel(lg.WARNING)

COL_OPTION_INDEX = 'option_index'
COL_OPTION_CODE = 'option_code'
COL_MILLISECONDS = 'milliseconds'               # from epoch
COL_TRADE_DATE = 'trade_date'                   # yyyy-mm-dd
COL_VALID_DATES = 'valid_dates'                 # [,,,]

contract_columns = [
    COL_OPTION_INDEX,
    COL_OPTION_CODE,
    COL_VALID_DATES
] + sse.daily_summary_columns

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


def isodate_to_milliseconds(isodate='2000-01-01'):
    if isodate.index('-') != 4:
        raise Exception("Wrong iso date format:{}".format(isodate))
    epoch = datetime.datetime.utcfromtimestamp(0)
    d = datetime.datetime.strptime(isodate, '%Y-%m-%d')
    ms = (d - epoch).total_seconds() * 1000
    return int(ms)


class OptionDB:
    '''

    '''
    def __init__(self):
        self._client = pymongo.MongoClient()
        self._db = self._client['option']
        self._coll_dayline = self._db['ohlc']
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
        res = list(self._coll_dayline.find(filter))
        logger.debug("results:{}".format(len(res)))
        ohlc = pd.DataFrame(res)
        return ohlc

    def upsert_ohlc(self, ohlc):
        '''

        :param ohlc: DataFrame
        :return:
        '''
        for _, row in ohlc.iterrows():
            option_index = row[COL_OPTION_INDEX]
            milliseconds = row[COL_MILLISECONDS]
            if option_index is None or milliseconds is None:
                logger.error("Wrong format, no index:{}".format(row))
                continue
            filter = {COL_OPTION_INDEX: option_index, COL_MILLISECONDS: milliseconds}
            val = {"$set": row.to_dict()}
            result = self._coll_dayline.update_many(filter, val, True)
            logger.debug("update:{}{}".format(filter, result))

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
            result = self._coll_greeks.update(filter, val, True)
            logger.debug("update:{}{}".format(filter, result))

    def get_greeks(self, option_index, fr_ms=-1, to_ms=-1):
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
        res = list(self._coll_greeks.find(filter))
        logger.debug("results:{}".format(len(res)))
        greeks = pd.DataFrame(res)
        return greeks

    def sync_today(self):
        logger.info("Get daily summary...")
        daily_summary = sse.get_trading_option_daily_summary()
        daily_summary[COL_OPTION_INDEX] = daily_summary['SECURITY_ID']
        daily_summary[COL_OPTION_CODE] = daily_summary['CONTRACT_ID']
        daily_summary[COL_TRADE_DATE] = daily_summary['TIMESAVE']
        self.upsert_daily_summary(daily_summary)
        logger.debug("\n{}".format(daily_summary.head(1)))

        # count of trading options
        count = len(daily_summary.index)
        trade_date = daily_summary.iloc[0][COL_TRADE_DATE]
        milliseconds = isodate_to_milliseconds(trade_date)
        logger.info("sync data of {}".format(trade_date))
        cache_greeks = self.get_greeks(daily_summary[COL_OPTION_INDEX].tolist(), milliseconds, milliseconds)
        if len(cache_greeks.index) == count:
            logger.info("greeks already sync.")
        else:
            logger.info("greeks is syncing...")
            greeks = sse.get_greeks(trade_date)
            greeks[COL_OPTION_INDEX] = greeks['SECURITY_ID']
            greeks[COL_OPTION_CODE] = greeks['CONTRACT_ID']
            greeks[COL_TRADE_DATE] = greeks['TRADE_DATE']
            greeks[COL_MILLISECONDS] = [isodate_to_milliseconds(greeks.loc[i][COL_TRADE_DATE]) for i in greeks.index]
            # for i in greeks.index:
            #    greeks.loc[i][COL_MILLISECONDS] = isodate_to_milliseconds(greeks.loc[i][COL_TRADE_DATE])
            self.upsert_greeks(greeks)
            logger.debug("\n{}".format(greeks.head(1)))
            logger.info("greeks sync done.")

        cache_ohlc = self.get_ohlc(daily_summary[COL_OPTION_INDEX].tolist(), milliseconds, milliseconds)
        cache_index_list = []
        if len(cache_ohlc.index):
            cache_ohlc[COL_OPTION_INDEX].tolist()

        if len(cache_ohlc.index) == count:
            logger.info("ohlc already sync.")
        else:
            logger.info("ohlc is syncing...")
            for i, row in daily_summary.iterrows():
                option_index = row[COL_OPTION_INDEX]
                if option_index in cache_index_list:
                    logger.info("({}/{}){} exists.".format(i + 1, count, option_index))
                    continue
                logger.info("{} is syncing...".format(option_index))
                ohlc = sina.get_trading_option_history_ohlc(option_index)
                time.sleep(3)
                ohlc[COL_OPTION_INDEX] = [option_index for _ in ohlc.index]
                ohlc[COL_TRADE_DATE] = ohlc['d']
                ohlc[COL_MILLISECONDS] = [isodate_to_milliseconds(ohlc.loc[i][COL_TRADE_DATE]) for i in ohlc.index]
                # for i in ohlc.index:
                #     ohlc.loc[i][COL_MILLISECONDS] = isodate_to_milliseconds(ohlc.loc[i][COL_TRADE_DATE])
                self.upsert_ohlc(ohlc)
                logger.debug("\n{}".format(ohlc.head(1)))
                logger.info("({}/{}){} is done.".format(i + 1, count, option_index))
            logger.info("ohlc is done.")






