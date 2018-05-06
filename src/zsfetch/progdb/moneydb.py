import pymongo
import logging as lg
import pandas as pd
from zsfetch.moneysites import sse
from zsfetch.moneysites import qq
from zsfetch.moneysites import eastmoney

logger = lg.getLogger(__name__)
logger.setLevel(lg.WARNING)

COL_MONEY_FUND_INDEX = "money_fund_index"
COL_TRADE_DATE = "trade_date"
COL_MILLISECONDS = "milliseconds"

COL_GCR_INDEX = "gcr_index"  # gcr
COL_GCR_DURATION = "gcr_duration"
COL_GCR_AMOUNT = "gcr_amount"  # 成交额 元
COL_GCR_LOCKED_AMOUNT = "gcr_locked_amount"  # 锁定的金额

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

hsg_flow_columns = [
    COL_TRADE_DATE,
    COL_MILLISECONDS
] + eastmoney.hsg_flow_columns

gcr_ohlc_columns = [
    COL_GCR_INDEX,
    COL_TRADE_DATE,
    COL_MILLISECONDS,
    COL_GCR_DURATION,
    COL_GCR_AMOUNT
] + qq.gcr_ohlc_columns

tracked_gcr = {
    'sh204001': {COL_GCR_DURATION: 1, COL_GCR_AMOUNT: 1000},
    'sh204002': {COL_GCR_DURATION: 2, COL_GCR_AMOUNT: 1000},
    'sh204003': {COL_GCR_DURATION: 3, COL_GCR_AMOUNT: 1000},
    'sh204004': {COL_GCR_DURATION: 4, COL_GCR_AMOUNT: 1000},
    'sh204007': {COL_GCR_DURATION: 7, COL_GCR_AMOUNT: 1000},
    'sh204014': {COL_GCR_DURATION: 14, COL_GCR_AMOUNT: 1000},
    'sh204028': {COL_GCR_DURATION: 28, COL_GCR_AMOUNT: 1000},
    'sh204091': {COL_GCR_DURATION: 91, COL_GCR_AMOUNT: 1000},
    'sh204182': {COL_GCR_DURATION: 182, COL_GCR_AMOUNT: 1000},

    'sz131810': {COL_GCR_DURATION: 1, COL_GCR_AMOUNT: 1000},
    'sz131811': {COL_GCR_DURATION: 2, COL_GCR_AMOUNT: 1000},
    'sz131800': {COL_GCR_DURATION: 3, COL_GCR_AMOUNT: 1000},
    'sz131809': {COL_GCR_DURATION: 4, COL_GCR_AMOUNT: 1000},
    'sz131801': {COL_GCR_DURATION: 7, COL_GCR_AMOUNT: 1000},
    'sz131802': {COL_GCR_DURATION: 14, COL_GCR_AMOUNT: 1000},
    'sz131803': {COL_GCR_DURATION: 28, COL_GCR_AMOUNT: 1000},
    'sz131805': {COL_GCR_DURATION: 91, COL_GCR_AMOUNT: 1000},
    'sz131806': {COL_GCR_DURATION: 182, COL_GCR_AMOUNT: 1000}
}

_MAX_GCR_DURATION = max([tracked_gcr[gcr][COL_GCR_DURATION] for gcr in tracked_gcr])


class MoneyDB:
    '''

    '''
    def __init__(self, dbname='money'):
        if dbname == '':
            dbname = 'money'
        logger.info("using db:{}".format(dbname))
        self._client = pymongo.MongoClient()
        self._db = self._client[dbname]
        self._coll_dayline = self._db['ohlc']                 # from sse
        self._coll_share = self._db['share']                  # from sse
        self._coll_gcr_ohlc = self._db['gcr_ohlc']            # from qq
        self._coll_hsg_flow = self._db['hsg_flow']

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
        res = list(self._coll_share.find(filter))
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

    def get_gcr_ohlc(self, gcr_index='', fr_ms=-1, to_ms=-1):
        filter = {}
        if len(gcr_index) == 0 or gcr_index is None:
            pass  # can be empty
        elif isinstance(gcr_index, list):
            filter[COL_GCR_INDEX] = {"$in": gcr_index}
        elif isinstance(gcr_index, str):
            filter[COL_GCR_INDEX] = gcr_index
        else:
            raise Exception("Wrong dtype of gcr_index:{}".format(gcr_index))
        within = {}
        if fr_ms != -1:
            within['$gte'] = fr_ms
        if to_ms != -1:
            within['$lte'] = to_ms
        if bool(within):
            filter[COL_MILLISECONDS] = within
        logger.debug("filter:{}".format(filter))
        res = list(self._coll_gcr_ohlc.find(filter))
        logger.debug("gcr ohlc count:{}".format(len(res)))
        ohlc = pd.DataFrame(res)
        return ohlc

    def upsert_gcr_ohlc(self, gcr_ohlc):

        for _, row in gcr_ohlc.iterrows():
            gcr_index = row[COL_GCR_INDEX]
            milliseconds = row[COL_MILLISECONDS]
            if gcr_index is None or milliseconds is None:
                logger.error("Wrong format, no index:{}".format(row))
                continue
            filter = {COL_GCR_INDEX: gcr_index, COL_MILLISECONDS: milliseconds}
            val = {"$set": row.to_dict()}
            result = self._coll_gcr_ohlc.update_many(filter, val, True)
            logger.debug("update gcr ohlc:{}{}".format(filter, result))

    def update_gcr_locked_amount(self, date_ms):
        unit_ms = 24 * 3600 * 1000
        for gcr_index in tracked_gcr:
            locked_amount = 0
            duration = tracked_gcr[gcr_index][COL_GCR_DURATION]
            fr_ms = date_ms - unit_ms * (duration - 1)
            to_ms = date_ms
            filter = {
                COL_MILLISECONDS: {
                    '$gte': fr_ms,
                    '$lte': to_ms
                },
                COL_GCR_INDEX: gcr_index,
                COL_GCR_DURATION: duration
            }
            res = list(self._coll_gcr_ohlc.find(filter=filter))
            logger.debug("gcr:{} duration:{} count:{} filter:{}".format(gcr_index, duration, len(res), filter))
            for gcr in res:
                locked_amount += int(gcr[COL_GCR_AMOUNT])

            filter = {COL_MILLISECONDS: date_ms, COL_GCR_INDEX: gcr_index}
            val = {"$set": {COL_GCR_LOCKED_AMOUNT: locked_amount}}
            res = self._coll_gcr_ohlc.update_one(filter, val)
            logger.debug("update gcr ohlc:filter:{} val:{} matched:{} modified:{}".format(
                filter, val, res.modified_count, res.modified_count))

    def get_hsg_flow(self, fr_ms=-1, to_ms=-1):
        filter = {}
        within = {}
        if fr_ms != -1:
            within['$gte'] = fr_ms
        if to_ms != -1:
            within['$lte'] = to_ms
        if bool(within):
            filter[COL_MILLISECONDS] = within
        logger.debug("hsg flow filter:{}".format(filter))
        res = list(self._coll_hsg_flow.find(filter))
        logger.debug("hsg flow count:{}".format(len(res)))
        flow = pd.DataFrame(res)
        return flow

    def upsert_hsg_flow(self, hsg_flow):

        for _, row in hsg_flow.iterrows():
            milliseconds = row[COL_MILLISECONDS]
            filter = {COL_MILLISECONDS: milliseconds}
            val = {"$set": row.to_dict()}
            result = self._coll_hsg_flow.update_many(filter, val, True)
            logger.debug("update hsg flow:{}{}".format(filter, result))

