import unittest
import logging as lg
from zsfetch.moneysites import eastmoney

dbgFormatter = "%(levelname)s:%(filename)s:%(lineno)s:%(funcName)s() -- %(message)s"
lg.basicConfig(level=lg.DEBUG, format=dbgFormatter)
eastmoney.logger.setLevel(lg.DEBUG)


class MyTestCase(unittest.TestCase):

    def _check_df_columns(self, df_col, col):
        self.assertEqual(len(col), len(df_col))
        for c in col:
            if c in df_col:
                self.assertEqual(True, True)
            else:
                lg.error("'{}' does not exist in '{}'".format(c, df_col))
                self.assertEqual(True, False)

    def test_get_hsg_flow(self):
        df = eastmoney.get_hsg_flow('2017-11-17', '2018-04-04')
        self.assertEqual(len(df.index), 89)
        self._check_df_columns(df.columns, eastmoney.hsg_flow_columns)
        lg.info("\n{}".format(df.head(1)))
        lg.info("\n{}".format(df.tail(1)))


if __name__ == '__main__':
    unittest.main()
