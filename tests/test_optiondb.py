import logging as lg
import unittest
from zsfetch.derivativedb import optiondb
from zsfetch.optionsites import sse

dbgFormatter = "%(levelname)s:%(filename)s:%(lineno)s:%(funcName)s() -- %(message)s"
lg.basicConfig(level=lg.DEBUG, format=dbgFormatter)
optiondb.logger.setLevel(lg.DEBUG)

_opdb = optiondb.OptionDB()


class MyTestCase(unittest.TestCase):
    def test_get_ohlc(self):
        daylines = _opdb.get_ohlc('10001284')
        lg.info("\n{}".format(daylines.head(1)))
        self.assertEqual(True, True)

    def test_get_daily_summary(self):
        daily_summary = _opdb.get_daily_summary()
        lg.info("local daily summary:\n{}".format(daily_summary.head(1)))
        online = sse.get_trading_option_daily_summary()
        lg.info("online daily summary:\n{}".format(online.head(1)))
        self.assertEqual(len(daily_summary.index), len(online.index))
        self.assertEqual(daily_summary.iloc[0][optiondb.COL_TRADE_DATE],
                         online.iloc[0]['TIMESAVE'])


if __name__ == '__main__':
    unittest.main()
