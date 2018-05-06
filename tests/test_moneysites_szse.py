import unittest
import logging as lg
from zsfetch.moneysites import szse

dbgFormatter = "%(levelname)s:%(filename)s:%(lineno)s:%(funcName)s() -- %(message)s"
lg.basicConfig(level=lg.DEBUG, format=dbgFormatter)
szse.logger.setLevel(lg.DEBUG)


class MyTestCase(unittest.TestCase):

    def _check_df_columns(self, df_col, col):
        self.assertEqual(len(col), len(df_col))
        for c in col:
            if c in df_col:
                self.assertEqual(True, True)
            else:
                lg.error("'{}' does not exist in '{}'".format(c, df_col))
                self.assertEqual(True, False)

    def test_get_money_fund_share(self):
        df = szse.get_money_fund_share()
        self.assertEqual(len(df.index), 3)
        self._check_df_columns(df.columns, szse.share_columns)
        lg.info("\n{}".format(df.head(3)))

if __name__ == '__main__':
    unittest.main()
