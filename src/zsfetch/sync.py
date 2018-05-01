
import time
from datetime import timedelta, datetime
import logging as lg
import pandas as pd
from zsfetch import optionsites
from zsfetch import moneysites
from zsfetch.progdb import optiondb
from zsfetch.progdb import moneydb
from zsfetch.util import isodate_to_milliseconds
from zsfetch.util import check_isodatestr_or_raise

logger = lg.getLogger(__name__)
logger.setLevel(lg.INFO)


class OptionSync:
    def __init__(self, dbname='option'):
        self._opdb = optiondb.OptionDB(dbname)

    def _sync_greeks(self, trade_date):
        logger.info("{}:greeks is syncing...".format(trade_date))
        greeks = optionsites.sse.get_greeks(trade_date)
        greeks[optiondb.COL_OPTION_INDEX] = greeks['SECURITY_ID']
        greeks[optiondb.COL_OPTION_CODE] = greeks['CONTRACT_ID']
        greeks[optiondb.COL_TRADE_DATE] = greeks['TRADE_DATE']
        greeks[optiondb.COL_MILLISECONDS] = [isodate_to_milliseconds(
            greeks.loc[i][optiondb.COL_TRADE_DATE]) for i in greeks.index]

        self._opdb.upsert_greeks(greeks)
        logger.debug("\n{}".format(greeks.head(1)))
        logger.info("greeks sync done.")

    def sync_greeks(self, fr_date='2018-01-01', to_date='', wait=1):
        end = datetime.today()
        if to_date != '':
            end = datetime.strptime(to_date, '%Y-%m-%d')
        start = datetime.strptime(fr_date, '%Y-%m-%d')
        delta = timedelta(days=1)
        while start <= end:
            trade_date = start.isoformat()[:10]
            logger.info("{}/{}:".format(trade_date, end.isoformat()[:10]))
            self._sync_greeks(trade_date)
            start += delta
            time.sleep(wait)

    def sync_summary(self):
        logger.info("Get daily summary...")
        daily_summary = optionsites.sse.get_trading_option_daily_summary()
        daily_summary[optiondb.COL_OPTION_INDEX] = daily_summary['SECURITY_ID']
        daily_summary[optiondb.COL_OPTION_CODE] = daily_summary['CONTRACT_ID']
        daily_summary[optiondb.COL_TRADE_DATE] = daily_summary['TIMESAVE']
        self._opdb.upsert_daily_summary(daily_summary)
        logger.debug("\n{}".format(daily_summary.head(1)))
        return daily_summary


class MoneySync:
    def __init__(self, dbname='money'):
        self._modb = moneydb.MoneyDB(dbname=dbname)

    def sync_money_ohlc(self):
        pass

    def _sync_money_share(self, trade_date):
        check_isodatestr_or_raise(trade_date)
        logger.info("{}:share is syncing...".format(trade_date))
        share = moneysites.sse.get_money_fund_share(trade_date)
        share[moneydb.COL_MONEY_FUND_INDEX] = share['SEC_CODE']
        share[moneydb.COL_TRADE_DATE] = share['STAT_DATE']
        share[moneydb.COL_MILLISECONDS] = [isodate_to_milliseconds(
            share.loc[i][moneydb.COL_TRADE_DATE]) for i in share.index]
        self._modb.upsert_share(share)
        logger.debug("\n{}".format(share.head(1)))
        logger.info("{}:share sync is done:{}".format(trade_date, len(share.index)))

    def sync_money_share(self, fr_date='2018-01-01', to_date='', wait=1):
        end = datetime.today()
        if to_date != '':
            end = datetime.strptime(to_date, '%Y-%m-%d')
        start = datetime.strptime(fr_date, '%Y-%m-%d')
        delta = timedelta(days=1)
        count = int((end - start) / delta) + 1
        i = 1
        while start <= end:
            trade_date = start.isoformat()[:10]
            logger.info("{}({}/{}):".format(trade_date, i, count))
            if start.weekday() < 5:
                self._sync_money_share(trade_date)
                time.sleep(wait)
            else:
                logger.info("skip weekend:{}".format(start.strftime("%A")))
            start += delta
            i += 1


def sync_today():
    opsync = OptionSync()
    daily_summary = opsync.sync_summary()
    # count of trading options
    count = len(daily_summary.index)
    trade_date = daily_summary.iloc[0][optiondb.COL_TRADE_DATE]
    milliseconds = isodate_to_milliseconds(trade_date)
    logger.info("sync data of {}".format(trade_date))
    # cache_greeks = _opdb.get_greeks(daily_summary[optiondb.COL_OPTION_INDEX].tolist(), milliseconds, milliseconds)
    # if len(cache_greeks.index) == count:
    #     logger.info("greeks already sync.")
    # else:
    #     _sync_greeks(trade_date)
    logger.info("ohlc is syncing...")
    for i, row in daily_summary.iterrows():
        option_index = row[optiondb.COL_OPTION_INDEX]
        logger.info("{} is syncing...".format(option_index))
        ohlc = optionsites.sina.get_trading_option_history_ohlc(option_index)
        time.sleep(3)
        ohlc[optiondb.COL_OPTION_INDEX] = [option_index for _ in ohlc.index]
        ohlc[optiondb.COL_TRADE_DATE] = ohlc['d']
        ohlc[optiondb.COL_MILLISECONDS] = [isodate_to_milliseconds(
            ohlc.loc[i][optiondb.COL_TRADE_DATE]) for i in ohlc.index]

        logger.debug("\n{}".format(ohlc.head(1)))
        logger.info("({}/{}){} is done.".format(i + 1, count, option_index))
    logger.info("ohlc is done.")


if __name__ == "__main__":
    print("start sync...")
    infoFormatter = "%(asctime)s:%(levelname)s:%(filename)s: -- %(message)s"
    lg.basicConfig(level=lg.INFO, format=infoFormatter)
    mosync = MoneySync()
    mosync.sync_money_share('2018-04-11')
