"""
Data Processor API
"""

from data_etl.specific_data_processors import PROC_MAPPING


class DataProcessor:
    """Data Processor API"""
    def __init__(self):
        """
        """
        self.proc_mapping = PROC_MAPPING

    def process(self, tbl_name, *args, **kwargs):
        """Process interface
        :param tbl_name:
        :param args:
        :param kwargs:
        :return:
        """
        return self.proc_mapping.get(tbl_name)(*args, **kwargs)





