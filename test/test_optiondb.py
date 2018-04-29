import logging as lg
import unittest

from derivativedb import optiondb

dbgFormatter = "%(levelname)s:%(filename)s:%(lineno)s:%(funcName)s() -- %(message)s"
lg.basicConfig(level=lg.DEBUG, format=dbgFormatter)
optiondb.logger.setLevel(lg.DEBUG)


class MyTestCase(unittest.TestCase):
    def test_get_ohlc(self):
        cache = optiondb.OptionDB()
        daylines = cache.get_ohlc('10001284')
        lg.info(daylines)
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
