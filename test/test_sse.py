import unittest
import logging
from optionsites import sse

dbgFormatter = "%(levelname)s:%(filename)s:%(lineno)s %(funcName)s() -- %(message)s"
logging.basicConfig(level=logging.INFO, format=dbgFormatter)
sse.logger.setLevel(logging.DEBUG)


class MyTestCase(unittest.TestCase):

    def _check_df_columns(self, df_col, col):
        self.assertEqual(len(col), len(df_col))
        for c in col:
            if c in df_col:
                self.assertEqual(True, True)
            else:
                logging.error("'{}' does not exist in '{}'".format(c, df_col))
                self.assertEqual(True, False)

    def test_get_trading_option_summary(self):
        # empty
        df = sse.get_trading_option_daily_summary('')
        logging.info("\n{}".format(df.head(2)))
        logging.info("\n{}".format(df.tail(2)))
        self._check_df_columns(df.columns, sse.daily_summary_columns)
        # has content
        df = sse.get_trading_option_daily_summary()
        logging.info("\n{}".format(df.head(2)))
        logging.info("\n{}".format(df.tail(2)))
        self.assertGreater(len(df.index), 0)
        self._check_df_columns(df.columns, sse.daily_summary_columns)

    def test_get_greeks(self):
        # empty
        df = sse.get_greeks('2018-04-15')
        logging.info("\n{}".format(df.head(2)))
        logging.info("\n{}".format(df.tail(2)))
        self._check_df_columns(df.columns, sse.greeks_columns)
        # has content
        df = sse.get_greeks('2018-04-13')
        logging.info("\n{}".format(df.head(2)))
        logging.info("\n{}".format(df.tail(2)))
        self.assertGreater(len(df.index), 0)
        self._check_df_columns(df.columns, sse.greeks_columns)


if __name__ == '__main__':
    unittest.main()
