
import time
from datetime import timedelta, datetime
import logging as lg
import pandas as pd
from zsfetch.optionsites import sina
from zsfetch.optionsites import sse
from zsfetch.derivativedb import optiondb

logger = lg.getLogger(__name__)
logger.setLevel(lg.WARNING)


_opdb = optiondb.OptionDB()


def _sync_greeks(trade_date):
    logger.info("{}:greeks is syncing...".format(trade_date))
    greeks = sse.get_greeks(trade_date)
    greeks[optiondb.COL_OPTION_INDEX] = greeks['SECURITY_ID']
    greeks[optiondb.COL_OPTION_CODE] = greeks['CONTRACT_ID']
    greeks[optiondb.COL_TRADE_DATE] = greeks['TRADE_DATE']
    greeks[optiondb.COL_MILLISECONDS] = [optiondb.isodate_to_milliseconds(
        greeks.loc[i][optiondb.COL_TRADE_DATE]) for i in greeks.index]
    # for i in greeks.index:
    #    greeks.loc[i][COL_MILLISECONDS] = isodate_to_milliseconds(greeks.loc[i][COL_TRADE_DATE])
    _opdb.upsert_greeks(greeks)
    logger.debug("\n{}".format(greeks.head(1)))
    logger.info("greeks sync done.")


def sync_greeks(fr_date='2018-01-01', to_date=''):
    end = datetime.today()
    if to_date != '':
        end = datetime.strptime(to_date, '%Y-%m-%d')
    start = datetime.strptime(fr_date, '%Y-%m-%d')
    delta = timedelta(days=1)
    while start <= end:
        trade_date = start.isoformat()[:10]
        logger.info("{}/{}:".format(trade_date, end.isoformat()[:10]))
        _sync_greeks(trade_date)
        start += delta


def sync_today():
    logger.info("Get daily summary...")
    daily_summary = sse.get_trading_option_daily_summary()
    daily_summary[optiondb.COL_OPTION_INDEX] = daily_summary['SECURITY_ID']
    daily_summary[optiondb.COL_OPTION_CODE] = daily_summary['CONTRACT_ID']
    daily_summary[optiondb.COL_TRADE_DATE] = daily_summary['TIMESAVE']
    _opdb.upsert_daily_summary(daily_summary)
    logger.debug("\n{}".format(daily_summary.head(1)))

    # count of trading options
    count = len(daily_summary.index)
    trade_date = daily_summary.iloc[0][optiondb.COL_TRADE_DATE]
    milliseconds = optiondb.isodate_to_milliseconds(trade_date)
    logger.info("sync data of {}".format(trade_date))
    cache_greeks = _opdb.get_greeks(daily_summary[optiondb.COL_OPTION_INDEX].tolist(), milliseconds, milliseconds)
    if len(cache_greeks.index) == count:
        logger.info("greeks already sync.")
    else:
        _sync_greeks(trade_date)

    cache_ohlc = _opdb.get_ohlc(daily_summary[optiondb.COL_OPTION_INDEX].tolist(), milliseconds, milliseconds)
    cache_index_list = []
    if len(cache_ohlc.index):
        cache_ohlc[optiondb.COL_OPTION_INDEX].tolist()

    if len(cache_ohlc.index) == count:
        logger.info("ohlc already sync.")
    else:
        logger.info("ohlc is syncing...")
        for i, row in daily_summary.iterrows():
            option_index = row[optiondb.COL_OPTION_INDEX]
            if option_index in cache_index_list:
                logger.info("({}/{}){} exists.".format(i + 1, count, option_index))
                continue
            logger.info("{} is syncing...".format(option_index))
            ohlc = sina.get_trading_option_history_ohlc(option_index)
            time.sleep(3)
            ohlc[optiondb.COL_OPTION_INDEX] = [option_index for _ in ohlc.index]
            ohlc[optiondb.COL_TRADE_DATE] = ohlc['d']
            ohlc[optiondb.COL_MILLISECONDS] = [optiondb.isodate_to_milliseconds(
                ohlc.loc[i][optiondb.COL_TRADE_DATE]) for i in ohlc.index]
            # for i in ohlc.index:
            #     ohlc.loc[i][COL_MILLISECONDS] = isodate_to_milliseconds(ohlc.loc[i][COL_TRADE_DATE])
            _opdb.upsert_ohlc(ohlc)
            logger.debug("\n{}".format(ohlc.head(1)))
            logger.info("({}/{}){} is done.".format(i + 1, count, option_index))
        logger.info("ohlc is done.")

