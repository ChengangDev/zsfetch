import logging as lg
import unittest
from zsfetch.progdb import optiondb
from zsfetch import sync

dbgFormatter = "%(levelname)s:%(filename)s:%(lineno)s:%(funcName)s() -- %(message)s"
lg.basicConfig(level=lg.DEBUG, format=dbgFormatter)
sync.logger.setLevel(lg.DEBUG)
sync._opdb = optiondb.OptionDB(__name__)


class MyTestCase(unittest.TestCase):
    def test_sync_today(self):
        sync.sync_today()
        self.assertEqual(True, True)

    def test_sync_greeks(self):
        sync.sync_greeks()
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
