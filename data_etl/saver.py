"""
Data Saver API
"""
from data_etl.specific_data_savers import SAVER_MAPPING


class DataSaver:
    """
    Data Saver API
    """
    def __init__(self):
        self.saver_mapping = SAVER_MAPPING

    def save(self, tbl_name, *args, **kwargs):
        return self.saver_mapping.get(tbl_name)(*args, **kwargs)
