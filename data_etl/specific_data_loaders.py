"""
Specific data loaders
"""
import os
import logging
import pandas as pd
from functools import partial
from zipfile import ZipFile
from data_etl.path_manager import get_path

logger = logging.getLogger(__name__)


def load_1min_raw(start, end, fields):
    """Load 1min raw data from zip files
    :param start: start date
    :param end: end date
    :param fields: columns
    :return:
    """
    tbl_dir = get_path('1min.raw')
    all_zips = os.listdir(tbl_dir)
    dt = start
    dat = []
    while dt <= end:
        fname = f'{dt:%Y%m%d}.zip'
        if fname not in all_zips:
            dt += pd.Timedelta(days=1)
            continue
        logger.debug(f'Load 1min raw {dt:%Y-%m-%d}')
        zipf = ZipFile(os.path.join(tbl_dir, fname))
        dat += [pd.read_csv(zipf.open(f.filename)).reindex(columns=fields) for f in zipf.infolist()]
        dt += pd.Timedelta(days=1)
    res = pd.concat(dat)
    res['date'] = pd.to_datetime(res['date'].astype(str))
    return res


def load_halt_date(start, end, fields):
    """Load halt date
    :param start:
    :param end:
    :param fields:
    :return:
    """
    res = pd.read_csv(os.path.join(get_path('halt_date.raw'), 'halt_date.csv'))
    res['halt_date'] = pd.to_datetime(res['halt_date'].astype(str))
    res['restart_date'] = pd.to_datetime(res['restart_date'].astype(str))
    res = res[(res['halt_date'] <= end) & (res['restart_date'] >= start)].reindex(columns=fields).copy()
    return res


def load_lst_date(start, end, fields):
    """Load lst date
    :param start:
    :param end:
    :param fields:
    :return:
    """
    res = pd.read_csv(os.path.join(get_path('lst_date.raw'), 'lst_date.csv'))
    res['lst_date'] = pd.to_datetime(res['lst_date'].astype(str).str.replace('.0', ''))
    res['delst_date'] = pd.to_datetime(res['delst_date'].astype(str).str.replace('.0', ''))
    res = res[((res['lst_date'] <= end) | res['lst_date'].isna()) &
              ((res['delst_date'] >= start) | res['delst_date'].isna())]. \
        reindex(columns=fields).copy()
    return res


def load_st_date(start, end, fields):
    """Load st date
    :param start:
    :param end:
    :param fields:
    :return:
    """
    res = pd.read_csv(os.path.join(get_path('st_date.raw'), 'st_date.csv'))
    res['eff_date'] = pd.to_datetime(res['eff_date'].astype(str))
    res = res[(res['eff_date'] <= end) & (res['eff_date'] >= start)].reindex(columns=fields).copy()
    return res


def load_trd_date(start, end, fields):
    """Load trd date
    :param start:
    :param end:
    :param fields:
    :return:
    """
    res = pd.read_csv(os.path.join(get_path('trd_date.raw'), 'trd_date.csv'))
    date_cols = ['date', 'mon_start', 'mon_end', 'yr_start', 'yr_end', 'pre_trade']
    for col in date_cols:
        res[col] = pd.to_datetime(res[col].astype(str))
    res = res[(res['date'] <= end) & (res['date'] >= start)].reindex(columns=fields).copy()
    return res


def _load_csv_bydt_helper(start, end, fields, tbl_name):
    """Standard helper function that reads csv by date
    :param start:
    :param end:
    :param fields:
    :param tbl_name:
    :return:
    """
    dt = start
    dat = []
    while dt <= end:
        fpath = os.path.join(get_path(tbl_name), str(dt.year), f'{dt:%Y%m%d}.csv')
        if not os.path.exists(fpath):
            dt += pd.Timedelta(days=1)
            continue
        logger.debug(f"load {tbl_name} on {dt:%Y-%m-%d}")
        dat.append(pd.read_csv(fpath).reindex(columns=fields).assign(date=dt))
        dt += pd.Timedelta(days=1)
    return pd.concat(dat)


def _load_pkl_bydt_helper(start, end, fields, tbl_dir):
    """Standard helper function that reads pickle file by date
    :param start:
    :param end:
    :param fields:
    :param tbl_dir:
    :return:
    """
    dt = start
    dat = []
    while dt <= end:
        fpath = os.path.join(tbl_dir, f'{dt:%Y%m%d}.pkl')
        if not os.path.exists(fpath):
            dt += pd.Timedelta(days=1)
            continue
        dat.append(pd.read_pickle(fpath).reindex(columns=fields))
        dt += pd.Timedelta(days=1)
    if len(dat) > 0:
        return pd.concat(dat)


def _load_pkl_helper(start, end, fields, tbl_name):
    """Standard function that reads one pickle file
    :param start:
    :param end:
    :param fields:
    :param tbl_name:
    :return:
    """
    res = pd.read_pickle(os.path.join(get_path(tbl_name), tbl_name))
    res = res[(res['date'] >= start) & (res['date'] <= end)].reindex(columns=fields).copy()
    return res


def load_alpha(start, end, fields, alpha_key):
    tbl_dir = get_path('alpha.pkl', alpha_key)
    return _load_pkl_bydt_helper(start, end, fields, tbl_dir).set_index('date')


def get_loader_mapping():
    """Construct the mapping from tbl name to specific loader functions
    :return: loader mapping
    """
    mapping_raw = {'1min': load_1min_raw}
    for tbl in ['adj_fct', 'idx', 'lmt', 'mkt_val', 'sw', 'hs300', 'zz500', 'zz800', 'zz1000', 'zz9999']:
        mapping_raw[tbl] = partial(_load_csv_bydt_helper, tbl_name=f'{tbl}.raw')
    mapping_raw.update({'halt_date': load_halt_date, 'lst_date': load_lst_date, 'st_date': load_st_date,
                        'trd_date': load_trd_date})

    mapping_pkl = {'1min_interim': partial(_load_pkl_bydt_helper, tbl_dir=get_path('1min_interim.pkl'))}
    for tbl in ['1min_agg', '1min_skw','1min_astd', 'adj_fct', 'halt_date', 'lst_date', 'st_date', 'trd_date', 'idx', 'lmt', 'mkt_val', 'sw',
                'hs300', 'zz500', 'zz800', 'zz1000', 'zz9999']:
        mapping_pkl[tbl] = partial(_load_pkl_helper, tbl_name=f'{tbl}.pkl')
    mapping_pkl['alpha'] = load_alpha

    mapping = {'raw': mapping_raw, 'pkl': mapping_pkl}
    return mapping


LOADER_MAPPING = get_loader_mapping()
