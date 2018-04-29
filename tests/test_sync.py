import logging as lg
import unittest

from zsfetch import sync

dbgFormatter = "%(levelname)s:%(filename)s:%(lineno)s:%(funcName)s() -- %(message)s"
lg.basicConfig(level=lg.DEBUG, format=dbgFormatter)
sync.logger.setLevel(lg.DEBUG)


class MyTestCase(unittest.TestCase):
    def test_sync_today(self):
        sync.sync_today()
        self.assertEqual(True, True)

    def test_sync(self):
        sync.sync_greeks()


if __name__ == '__main__':
    unittest.main()
