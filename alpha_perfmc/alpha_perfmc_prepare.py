"""
Prepare data before alpha_perfmc; Steps here should be generic enough that are re-usable for various alpha
performance measures.
"""

import pandas as pd
from data_etl.loader import DataLoader
from alpha_cal.alpha_calc_prepare import prepare_alpha_data


def get_lmt_hit(close, lmt):
    """Determine whether stocks hit the up/down limit, and translate that into 1,0,-1 coding.
    Coding scheme:
        1: hit up limit; -1: hit down limit; 0: neither
    :param close: stock close with these columns: date, code, close
    :param lmt: up/down limit with these columns: date, code, up-limit, down-limit
    :return: data frame with date as index, stock code as columns, and 1,0,-1 as values.
    """
    data = close.merge(lmt, on=['date', 'code'], how='left')
    data[['up_limit', 'down_limit']] *= 1e4  # scale up/down limit to match close
    data['hit'] = 0
    data['hit'] = data['hit'].mask(data['close'] >= data['up_limit'], 1)
    data['hit'] = data['hit'].mask(data['close'] <= data['down_limit'], -1)
    return pd.pivot_table(data[['code', 'date', 'hit']], index='date', columns='code', values='hit')


def prepare_alpha_perfmc(start, end):
    """Prepare data for alpha_perfmc.
    :param start:
    :param end:
    :return:
        - rtn: daily returns with date as index, and stock code as columns;
        - lmt_hit: hit on up/down limit, i.e., result from the get_lmt_hit function;
    """
    data = prepare_alpha_data(start, end, fields=['date', 'code', 'close','chg_pct'])
    rtn = pd.pivot_table(data[['code', 'date', 'rtn']], index='date', columns='code', values='rtn')

    lmt = DataLoader().load('lmt.pkl', start, end)
    lmt_hit = get_lmt_hit(data[['date', 'code', 'close']], lmt)
    return rtn, lmt_hit
