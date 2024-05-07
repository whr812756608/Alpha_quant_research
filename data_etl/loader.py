"""
Data Loader API
"""
import logging
import pandas as pd
from data_etl.specific_data_loaders import LOADER_MAPPING

logger = logging.getLogger(__name__)
MIN_START = '1700-01-01'
MAX_END = '2030-1-1'


class DataLoader:
    """Data Loader"""

    def __init__(self):
        """
        """
        self.mapping = LOADER_MAPPING

    def load(self, tbl_name, start=None, end=None, fields=None, **kwargs):
        """Loader interface
        :param tbl_name: table name
        :param start: start
        :param end: end
        :param fields: columns
        :return:
        """
        start = MIN_START if start is None else start
        end = MAX_END if end is None else end
        tbl, tag = tbl_name.split('.')
        return self.mapping[tag][tbl](pd.Timestamp(start), pd.Timestamp(end), fields, **kwargs)

    def load_window(self, tbl_name, date, window, fields=None):
        """Load by window. It reads the latest date raw files, and then concatenate the remaining from 1min_interim.pkl.
            For data other than 1min_raw the behavior is the same as the load function.
        :param tbl_name:
        :param date:
        :param window:
        :param fields:
        :return:
        """
        date = pd.Timestamp(date)
        start = date - pd.Timedelta(days=window)
        if tbl_name != '1min.raw':
            return self.load(tbl_name, start, date, fields)
        latest = self.load('1min.raw', date, date, fields)
        archived = self.load('1min_interim.pkl', start, date - pd.Timedelta(days=1), fields)
        return pd.concat([archived, latest])
