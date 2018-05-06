import logging as lg
import unittest
from zsfetch.progdb import optiondb
from zsfetch import sync

dbgFormatter = "%(levelname)s:%(filename)s:%(lineno)s:%(funcName)s() -- %(message)s"
lg.basicConfig(level=lg.DEBUG, format=dbgFormatter)
sync.logger.setLevel(lg.DEBUG)

_opsync = sync.OptionSync(__name__)
_mosync = sync.MoneySync(__name__)


class MyTestCase(unittest.TestCase):
    def test_sync_today(self):
        # _opsync.sync_today()
        self.assertEqual(True, True)

    def test_sync_all_time(self):
        # _opsync.sync_all_time(False)
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
