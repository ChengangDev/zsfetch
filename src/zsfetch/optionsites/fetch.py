import logging

from . import sina

logger = logging.getLogger(__name__)


class FetchOption:
    '''

    '''
    def __init__(self):
        return
        # self._cache = dboption.DbOption()

    def get_daylines(self, option_index, fr_date='', to_date='', force_update=False):
        fr_ms = -1
        to_ms = -1

        # cached = self._cache.get_ohlc(option_index, fr_ms, to_ms)
        # if len(cached):
        #     logger.info("hit cache")
        #     return cached

        fresh = sina.get_option_history_ohlc(option_index)
        # if l

    def get_trading_days(self, option_index):
        pass

    def get_minlines(self):
        pass








