import unittest
import logging as lg
from zsfetch.moneysites import qq
from zsfetch.progdb import moneydb

dbgFormatter = "%(levelname)s:%(filename)s:%(lineno)s:%(funcName)s() -- %(message)s"
lg.basicConfig(level=lg.DEBUG, format=dbgFormatter)
qq.logger.setLevel(lg.DEBUG)


class MyTestCase(unittest.TestCase):

    def _check_df_columns(self, df_col, col):
        self.assertEqual(len(col), len(df_col))
        for c in col:
            if c in df_col:
                self.assertEqual(True, True)
            else:
                lg.error("'{}' does not exist in '{}'".format(c, df_col))
                self.assertEqual(True, False)

    def test_get_money_fund_ohlc(self):
        df = qq.get_gcr_ohlc()
        self.assertEqual(len(df.index), 360)
        self._check_df_columns(df.columns, qq.gcr_ohlc_columns)
        lg.info("\n{}".format(df.tail(5)))

        for gcr_index in moneydb.tracked_gcr:
            df = qq.get_gcr_ohlc(gcr_index=gcr_index, count_of_recent_trading_days=1)
            lg.debug("{}\n{}".format(gcr_index, df))


if __name__ == '__main__':
    unittest.main()
