import logging as lg
import unittest
from zsfetch.progdb import moneydb
from zsfetch.moneysites import sse
from zsfetch.util import isodate_to_milliseconds

dbgFormatter = "%(levelname)s:%(filename)s:%(lineno)s:%(funcName)s() -- %(message)s"
lg.basicConfig(level=lg.DEBUG, format=dbgFormatter)
moneydb.logger.setLevel(lg.DEBUG)

# using test db
_modb = moneydb.MoneyDB('test_sync_money')


class MyTestCase(unittest.TestCase):
    def test_get_ohlc(self):
        daylines = _modb.get_ohlc('10001284')
        lg.info("\n{}".format(daylines.head(1)))
        self.assertEqual(True, True)

    def test_get_share(self):
        share = _modb.get_share('', isodate_to_milliseconds('2018-04-27'),
                                isodate_to_milliseconds('2018-04-27'))
        lg.info("local daily summary:\n{}".format(share.head(1)))
        online = sse.get_money_fund_share('2018-04-27')
        lg.info("online daily summary:\n{}".format(online.head(1)))
        self.assertEqual(len(share.index), len(online.index))
        self.assertEqual(share.iloc[0][moneydb.COL_TRADE_DATE],
                         online.iloc[0]['STAT_DATE'])

    def test_get_gcr_ohlc(self):
        sh_df = _modb.get_gcr_ohlc('sh204002')
        sh_df = sh_df.sort_values(by=moneydb.COL_MILLISECONDS)
        sz_df = _modb.get_gcr_ohlc('sz131811')
        sz_df = sz_df.sort_values(by=moneydb.COL_MILLISECONDS)
        print(sh_df[moneydb.COL_TRADE_DATE])
        print(sz_df[moneydb.COL_TRADE_DATE])

    def test_update_gcr_locked_amount(self):
        _modb.update_gcr_locked_amount(isodate_to_milliseconds('2018-05-02'))


if __name__ == '__main__':
    unittest.main()
