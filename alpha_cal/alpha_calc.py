"""
Alpha Calculator
"""

import logging
import importlib
import multiprocessing
from itertools import product
from functools import partial

import pandas as pd

from data_etl.loader import DataLoader
from data_etl.saver import DataSaver
from alpha_cal.alpha_calc_prepare import prepare_alpha_data

logger = logging.getLogger(__name__)


def filter_for_univ(data, univ_name):
    """Filter data for specific universe
    :param data: input data
    :param univ_name: universe name, such as hs300, zz500, zz800, zz1000, zz9999, all
    :return: filtered data
    """
    if univ_name == 'all':
        return data
    univ_data = DataLoader().load(f'{univ_name}.pkl')
    return data.merge(univ_data[['date', 'code']], on=['date', 'code'])


def import_alpha_by_name(alpha_name):
    """Import the alpha function for the alpha module using alpha_name.
    :param alpha_name: alpha name
    :return: alpha function
    """
    alpha_module = importlib.import_module(f'alphas.dev.{alpha_name}')
    return getattr(alpha_module, alpha_name)

def _flatten_cfg_dict(cfg_dict):
    """Flatten alpha config into the following format:
        alpha_key: (alpha_function_handle, (univ_name, parameters))
    :param cfg_dict: alpha config dictionary
    :return: flattened dictionary with the following structure:
        {alpha_key: (alpha_fun, (univ_name, parameters))}
    """
    fdict = {}
    for alpha_name, val in cfg_dict.items():
        alpha_fun = import_alpha_by_name(alpha_name)
        for args in product(val['univ'], val['args']):
            new_key = f'{alpha_name}-{args[0]}-{args[1]}'
            fdict[new_key] = (alpha_fun, args)
    return fdict


def _single_alpha_calc(alpha_key, val, data):
    """Apply specific alpha calculation.
    :param alpha_key: alpha_name-univ_name-parameters
    :param val: (alpha_fun, (univ_name, parameters))
    :param data: input data
    :return: alpha
    """
    alpha_fun, args = val
    univ_name, parameters = args
    data = filter_for_univ(data, univ_name)
    res = alpha_fun(data, *parameters)
    DataSaver().save('alpha.pkl', res, alpha_key)
    return res


def alpha_calc(cfg_dict, start, end, parallel = False):
    """Alpha Calc.
    :param cfg_dict: alpha config dictionary with the following structure:
        {alpha_name: {'univ': univ_name_list, 'args': args_list}}
    :param start: start date
    :param end: end date
    :return: None
    """
    data = prepare_alpha_data(start, end, fields=['date', 'code', 'pre_close', 'close', 'volume','open', 'low', 'high', 'chg_pct'])

    flattened_dict = _flatten_cfg_dict(cfg_dict)

    helper = partial(_single_alpha_calc, data=data)

    if parallel:
        with multiprocessing.Pool() as pool:
            _ = pool.starmap_async(helper, flattened_dict.items())
            pool.close()
            pool.join()
    else:
        for item in flattened_dict.items():
            helper(*item)


