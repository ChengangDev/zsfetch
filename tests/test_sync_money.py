import logging as lg
import unittest
from zsfetch.progdb import moneydb
from zsfetch import sync

dbgFormatter = "%(levelname)s:%(filename)s:%(lineno)s:%(funcName)s() -- %(message)s"
lg.basicConfig(level=lg.DEBUG, format=dbgFormatter)
sync.logger.setLevel(lg.DEBUG)
moneydb.logger.setLevel(lg.DEBUG)

_mosync = sync.MoneySync(__name__)


class MyTestCase(unittest.TestCase):
    def test_sync_share(self):
        _mosync.sync_money_share(fr_date='2018-04-29')
        self.assertEqual(True, True)

    def test_sync_gcr_ohlc(self):
        for gcr_index in moneydb.tracked_gcr:
            _mosync.sync_gcr_ohlc(gcr_index=gcr_index, count_of_recent_trading_days=222)
        self.assertEqual(True, True)

    def test_sync_today(self):
        _mosync.sync_today(missed_days=14)
        self.assertEqual(True, True)

if __name__ == '__main__':
    unittest.main()
