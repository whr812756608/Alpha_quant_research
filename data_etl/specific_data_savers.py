"""
Specific data savers
"""

import os
import pandas as pd
from functools import partial
from data_etl.path_manager import get_path


def save_pkl(data, tbl_name):
    """Save data to pkl.
    :param tbl_name:
    :param data:
    :return:
    """
    data.to_pickle(os.path.join(get_path(tbl_name), tbl_name))


def save_pkl_bycol(tbl_name, data):
    """Save data to pkl by field. Each data has date as index and stock code as columns.
    :param tbl_name:
    :param data:
    :return:
    """
    cols = data.columns.drop(['date', 'code'])
    for col in cols:
        res = pd.pivot_table(data[['code', 'date', col]], index='date', columns='code', values=col)
        res.to_pickle(os.path.join(get_path(tbl_name), f'{col}.pkl'))


def save_pkl_bydate(data, tbl_dir):
    """Save data to pkl by date.
    :param tbl_dir:
    :param data:
    :return:
    """
    dts = data['date'].unique()
    for dt in dts:
        data[data['date'] == dt].to_pickle(os.path.join(tbl_dir, f'{dt:%Y%m%d}.pkl'))


def save_alpha_by_date(data, alpha_key):
    """Save alpha with each date as individual file
    :param data:
    :param alpha_key:
    :return:
    """
    tbl_dir = get_path('alpha.pkl', alpha_key)
    save_pkl_bydate(data.reset_index(), tbl_dir)


def get_saver_mapping():
    """Create mapping from tbl_name to specific saver function
    :return: saver mapping
    """
    mapping = {'1min_interim.pkl': partial(save_pkl_bydate, tbl_dir=get_path('1min_interim.pkl')),
               'alpha.pkl': save_alpha_by_date}
    for tbl in ['1min_agg', '1min_skw', '1min_astd',  'adj_fct', 'halt_date', 'lst_date', 'st_date', 'trd_date', 'idx', 'lmt', 'mkt_val', 'sw',
                'hs300', 'zz500', 'zz800', 'zz1000', 'zz9999']:
        mapping[f'{tbl}.pkl'] = partial(save_pkl, tbl_name=f'{tbl}.pkl')
    return mapping


SAVER_MAPPING = get_saver_mapping()
