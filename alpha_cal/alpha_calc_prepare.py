"""
Load and prepare data before alpha_calc; Steps here should be generic enough that only need to be done once before
individual alpha calculations.
"""

import numpy as np
import pandas as pd
from data_etl.loader import DataLoader


def prepare_alpha_data(start, end, fields=None):
    """Prepare data before alpha calc. Steps here should be generic enough that only need to be done once before
    individual alpha calculations.
    The function does the following:
        - Forward adj prices based on adj_fct;
        - Calculate daily returns;
    :param start: start date
    :param end: end date
    :param fields: selected fields
    :return: processed data
    """
    dl = DataLoader()
    data = dl.load('1min_agg.pkl', start, end, fields)
    skw_data = dl.load('1min_skw.pkl', start, end)
    astd_data = dl.load('1min_astd.pkl', start, end)
    adj_fct = dl.load('adj_fct.raw', start, end)

    # Rescale adj factor based on the first date's value
    adj_fct = adj_fct.sort_values(['code', 'date'])
    adj_fct['first_adjf'] = adj_fct.groupby('code')['cum_adjf'].transform('first')
    adj_fct['cum_adjf'] = adj_fct['cum_adjf'] / adj_fct['first_adjf']

    # Forward adjust stock prices based adj factors
    cols = data.columns.drop(['date', 'code', 'volume'], errors='ignore')
    data = data.merge(adj_fct[['code', 'date', 'cum_adjf']], on=['code', 'date'])
    data[[f'{col}_adjed' for col in cols]] = data[cols].multiply(data['cum_adjf'], axis=0)
    # Calc Rtns
    data = data.sort_values(['code', 'date'])
    data['rtn'] = data.groupby(['code'])['chg_pct'].shift(-1)

    data = pd.merge(data, skw_data, on = ['code','date'], how = 'right')
    data = pd.merge(data, astd_data, on=['code', 'date'], how='right')
    return data

