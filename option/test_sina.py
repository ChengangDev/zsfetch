import unittest
import option.sina
from datetime import date
import logging

dbgFormatter = "%(levelname)s:%(filename)s:%(lineno)s %(funcName)s() -- %(message)s"
logging.basicConfig(level=logging.INFO, format=dbgFormatter)
option.sina.logger.setLevel(logging.DEBUG)


class MyTestCase(unittest.TestCase):
    def test_something(self):
        td = date.today()
        months = option.sina.get_trading_months()
        logging.info(months)
        logging.debug(months)
        for i in range(3):
            tm = "{0}-{1:02}".format(td.year, td.month + i)
            day = option.sina.get_trading_expire_date(tm)
            logging.debug(day)
        self.assertEqual(len(months), 4)

        df = option.sina.get_trading_option_list('510050', months[0])

    def test_get_trading_option_history_ohlc(self):
        ohlc = option.sina.get_trading_option_history_ohlc('10001209')
        self.assertEqual(len(ohlc.columns), len(option.sina.ohlc_columns))
        logging.info("\n{}".format(ohlc.head(2)))
        self.assertGreater(len(ohlc.index), 0)
        for row in ohlc.iterrows():
            d = row['d']
            self.assertEqual(len(d), 10)
            self.assertEqual(d.index('-'), 4)


if __name__ == '__main__':
    unittest.main()
