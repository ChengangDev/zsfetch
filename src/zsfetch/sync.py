
import time
from datetime import timedelta, datetime
import logging as lg
from zsfetch import optionsites
from zsfetch import moneysites
from zsfetch.progdb import optiondb
from zsfetch.progdb import moneydb
from zsfetch.util import isodate_to_milliseconds
from zsfetch.util import check_isodatestr_or_raise

logger = lg.getLogger(__name__)
logger.setLevel(lg.INFO)


class Sync:
    def sync_today(self, force_update=False, missed_days=14):
        pass

    def sync_all_time(self, force_update=False):
        pass


class OptionSync(Sync):
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

    def sync_greeks(self, fr_date='2015-02-09', to_date='', force_update=False, wait=1):
        '''
        must be called before sync_ohlc([], False)
        :param fr_date:
        :param to_date:
        :param force_update:
        :param wait:
        :return:
        '''
        end = datetime.today()
        if to_date != '':
            end = datetime.strptime(to_date, '%Y-%m-%d')
        start = datetime.strptime(fr_date, '%Y-%m-%d')
        delta = timedelta(days=1)
        count = int((end - start) / delta) + 1
        i = 1
        while start <= end:
            trade_date = start.isoformat()[:10]
            logger.info("({}/{}){}:".format(i, count, trade_date))
            if start.weekday() < 5:  # need trade calendar
                milliseconds = isodate_to_milliseconds(trade_date)
                cached_greeks = self._opdb.get_greeks(option_index='', fr_ms=milliseconds, to_ms=milliseconds)
                if len(cached_greeks.index) == 0 or force_update:
                    self._sync_greeks(trade_date)
                    time.sleep(wait)
                else:
                    logger.info("{} greeks already exists and not force update.".format(trade_date))
            else:
                logger.info("skip weekend:{} {}".format(trade_date, start.strftime("%A")))

            start += delta
            i += 1

    def _sync_ohlc(self, option_index):
        option_index = str(option_index)
        logger.info("{}:ohlc is syncing...".format(option_index))
        ohlc = optionsites.sina.get_option_history_ohlc(option_index)
        ohlc[optiondb.COL_OPTION_INDEX] = [option_index for _ in ohlc.index]
        ohlc[optiondb.COL_TRADE_DATE] = ohlc['d']
        ohlc[optiondb.COL_MILLISECONDS] = [isodate_to_milliseconds(
            ohlc.loc[i][optiondb.COL_TRADE_DATE]) for i in ohlc.index]
        logger.debug("online ohlc:{}\n{}".format(len(ohlc.index), ohlc.head(1)))
        self._opdb.upsert_ohlc(ohlc)

    def sync_ohlc(self, option_index_list, force_update=False, wait=1):
        '''

        :param option_index_list:
        :param force_update:  if False, must be called after sync_greeks()
        :param wait
        :return:
        '''
        if not isinstance(option_index_list, list):
            raise Exception("option_index_list must be a list:{}".format(option_index_list))
        for i, option_index in enumerate(option_index_list):
            logger.info("({}/{}){} ohlc is syncing...".format(i+1, len(option_index_list), option_index))
            cached_ohlc = self._opdb.get_ohlc(option_index)
            cached_greeks = self._opdb.get_greeks(option_index)  # using greeks num to check up-to-date
            if not force_update:
                if len(cached_ohlc.index) >= len(cached_greeks.index):
                    logger.info("{} ohlc already exists:{}>={} and skip.".format(
                        option_index, len(cached_ohlc.index), len(cached_greeks.index)))
                    continue
            else:
                if len(cached_ohlc.index) == len(cached_greeks.index):
                    logger.info("{} ohlc already exists:{}, but force update.".format(
                        option_index, len(cached_ohlc.index)))
            self._sync_ohlc(option_index)
            time.sleep(wait)

    def sync_summary(self):
        '''
        update when new, adjust, delete contracts
        :return:
        '''
        logger.info("sync daily summary...")
        daily_summary = optionsites.sse.get_trading_option_daily_summary()
        daily_summary[optiondb.COL_OPTION_INDEX] = daily_summary['SECURITY_ID']
        daily_summary[optiondb.COL_OPTION_CODE] = daily_summary['CONTRACT_ID']
        daily_summary[optiondb.COL_TRADE_DATE] = daily_summary['TIMESAVE']
        self._opdb.upsert_daily_summary(daily_summary)
        logger.debug("\n{}".format(daily_summary.head(1)))
        return daily_summary

    def sync_all_time(self, force_update=False):
        logger.info("syncing all time...")
        self.sync_greeks(force_update=force_update)
        option_index_list = [str(i) for i in range(10000001, 10001400)]
        logger.info(option_index_list)
        self.sync_ohlc(option_index_list, force_update)

    def sync_today(self, force_update=False, missed_days=14):
        daily_summary = self.sync_summary()
        # count of trading options
        count = len(daily_summary.index)

        trade_date = datetime.today().isoformat()[:10]
        start = datetime.today() - timedelta(days=missed_days)
        start_date = start.isoformat()[:10]
        logger.info("syncing greeks from {} to {}...".format(start_date, trade_date))
        self.sync_greeks(fr_date=start_date, force_update=force_update)
        logger.info("syncing ohlc...")
        option_index_list = []
        for _, row in daily_summary.iterrows():
            option_index_list.append(row[optiondb.COL_OPTION_INDEX])
        self.sync_ohlc(option_index_list, force_update)
        logger.info("sync today is done.")


class MoneySync(Sync):
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

    def sync_money_share(self, fr_date='2018-01-01', to_date='', force_update=False, wait=1):
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
                milliseconds = isodate_to_milliseconds(trade_date)
                cached_share = self._modb.get_share(fund_index='', fr_ms=milliseconds, to_ms=milliseconds)
                if len(cached_share.index) == 0 or force_update:
                    self._sync_money_share(trade_date)
                    time.sleep(wait)
                else:
                    logger.info("{} share already exists and not force update.".format(trade_date))
            else:
                logger.info("skip weekend:{}".format(start.strftime("%A")))
            start += delta
            i += 1

    def sync_gcr_ohlc(self, gcr_index='sh204001', count_of_recent_trading_days=14):
        logger.info("syncing gcr ohlc {}:days:{}".format(gcr_index, count_of_recent_trading_days))
        if gcr_index not in moneydb.tracked_gcr:
            logger.error("gcr {} is not tracked.".format(gcr_index))
            return
        duration = moneydb.tracked_gcr[gcr_index][moneydb.COL_GCR_DURATION]
        amt_multiple = moneydb.tracked_gcr[gcr_index][moneydb.COL_GCR_AMOUNT]
        ohlc = moneysites.qq.get_gcr_ohlc(gcr_index=gcr_index,
                                          count_of_recent_trading_days=count_of_recent_trading_days)
        ohlc[moneydb.COL_GCR_INDEX] = [gcr_index for _ in ohlc.index]
        ohlc[moneydb.COL_GCR_DURATION] = [duration for _ in ohlc.index]
        ohlc[moneydb.COL_GCR_AMOUNT] = [amt_multiple * ohlc.loc[i]['v'] for i in ohlc.index]
        ohlc[moneydb.COL_TRADE_DATE] = ohlc['d']
        ohlc[moneydb.COL_MILLISECONDS] = [isodate_to_milliseconds(
            ohlc.loc[i][moneydb.COL_TRADE_DATE]) for i in ohlc.index]
        logger.debug("grc_ohlc:\n{}".format(ohlc.tail(1)))
        self._modb.upsert_gcr_ohlc(ohlc)
        logger.info("{} gcr ohlc sync is done:{}".format(gcr_index, len(ohlc.index)))

    def update_gcr_locked_amount(self, fr_date='2018-01-01', to_date=''):
        end = datetime.today()
        if to_date != '':
            end = datetime.strptime(to_date, '%Y-%m-%d')
        start = datetime.strptime(fr_date, '%Y-%m-%d')
        delta = timedelta(days=1)
        count = int((end - start) / delta) + 1
        i = 1
        while start <= end:
            trade_date = start.isoformat()[:10]
            logger.info("update gcr locked amount: {}({}/{}):".format(trade_date, i, count))
            if start.weekday() < 5:
                milliseconds = isodate_to_milliseconds(trade_date)
                self._modb.update_gcr_locked_amount(milliseconds)
            else:
                logger.info("skip weekend:{}".format(start.strftime("%A")))
            start += delta
            i += 1

    def sync_hsg_flow(self, fr_date='2014-11-17', to_date=''):
        logger.info("syncing hsg flow {} to {}".format(fr_date, to_date))
        flow = moneysites.eastmoney.get_hsg_flow(fr_date, to_date)
        flow[moneydb.COL_TRADE_DATE] = [flow.loc[i]['DateTime'][:10] for i in flow.index]
        flow[moneydb.COL_MILLISECONDS] = [isodate_to_milliseconds(
            flow.loc[i][moneydb.COL_TRADE_DATE]) for i in flow.index]
        logger.debug("hsg flow:\n{}".format(flow.tail(1)))
        self._modb.upsert_hsg_flow(flow)
        logger.info("hsg flow sync is done:{}: {} to {}".format(len(flow.index), fr_date, to_date))

    def sync_all_time(self, force_update=False):
        fr_date = '2013-01-28'
        logger.info("syncing hsg flow all time:")
        self.sync_hsg_flow()

        logger.info("syncing money all time:{}".format(fr_date))
        self.sync_money_share(fr_date=fr_date, force_update=force_update)
        start = datetime.strptime(fr_date, '%Y-%m-%d')
        delta = timedelta(days=1)
        count = int((datetime.today() - start) / delta / 7 * 5)
        logger.info("syncing gcr all time:{}".format(count))
        for gcr_index in moneydb.tracked_gcr:
            self.sync_gcr_ohlc(gcr_index=gcr_index, count_of_recent_trading_days=count)

        logger.info("update gcr locked amount all time:".format(count))
        self.update_gcr_locked_amount(fr_date=fr_date)
        logger.info("syncing is done.")

    def sync_today(self, force_update=False, missed_days=14):
        '''
        清算后
        :param force_update:
        :param missed_days:
        :return:
        '''
        trade_date = datetime.today().isoformat()[:10]
        start = datetime.today() - timedelta(days=missed_days)
        start_date = start.isoformat()[:10]
        logger.info("syncing money share today from {} to {}...".format(start_date, trade_date))
        self.sync_money_share(fr_date=start_date, force_update=force_update)
        for gcr_index in moneydb.tracked_gcr:
            # logger.info("syncing gcr today:{}".format(gcr_index))
            self.sync_gcr_ohlc(gcr_index=gcr_index, count_of_recent_trading_days=missed_days)
        logger.info("update gcr locked amount from: {}".format(start_date))
        self.update_gcr_locked_amount(fr_date=start_date)

        logger.info("{}:ugly szse share is syncing...".format(trade_date))
        share = moneysites.szse.get_money_fund_share(trade_date)
        share[moneydb.COL_MONEY_FUND_INDEX] = share['SEC_CODE']
        share[moneydb.COL_TRADE_DATE] = share['STAT_DATE']
        share[moneydb.COL_MILLISECONDS] = [isodate_to_milliseconds(
            share.loc[i][moneydb.COL_TRADE_DATE]) for i in share.index]
        self._modb.upsert_share(share)

        logger.info("sync today is done.")


if __name__ == "__main__":
    infoFormatter = "%(asctime)s:%(levelname)s:%(filename)s: -- %(message)s"
    lg.basicConfig(level=lg.DEBUG, format=infoFormatter)
    logger.info("start sync...be careful of proxy...")
    sync_list = [MoneySync(), OptionSync()]
    for nc in sync_list:
        nc.sync_today(missed_days=14)
        # nc.sync_all_time()
