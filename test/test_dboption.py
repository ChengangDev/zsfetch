import logging as lg
import unittest

from derivativedb import dboption

dbgFormatter = "%(levelname)s:%(filename)s:%(lineno)s:%(funcName)s() -- %(message)s"
lg.basicConfig(level=lg.DEBUG, format=dbgFormatter)
dboption.logger.setLevel(lg.DEBUG)


class MyTestCase(unittest.TestCase):
    def test_get_ohlc(self):
        cache = dboption.DbOption()
        daylines = cache.get_ohlc('10001284')
        lg.info(daylines)
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
