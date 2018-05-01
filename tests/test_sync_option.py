import logging as lg
import unittest
from zsfetch.progdb import moneydb
from zsfetch import sync

dbgFormatter = "%(levelname)s:%(filename)s:%(lineno)s:%(funcName)s() -- %(message)s"
lg.basicConfig(level=lg.INFO, format=dbgFormatter)
sync.logger.setLevel(lg.INFO)
moneydb.logger.setLevel(lg.INFO)

_mosync = sync.MoneySync(__name__)


class MyTestCase(unittest.TestCase):
    def test_sync_share(self):
        _mosync.sync_money_share(fr_date='2018-04-21')
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
