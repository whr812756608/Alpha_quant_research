"""
Measure alpha performance
"""

import multiprocessing
from functools import partial
import pandas as pd
import os
from data_etl.path_manager import get_path
from data_etl.loader import DataLoader
from alpha_perfmc.alpha_perfmc_prepare import prepare_alpha_perfmc
from alpha_perfmc.alpha_perfmc_plot import *
import matplotlib.pyplot as plt


def adj_alpha(alpha, lmt_hit):
    """Adjust alpha based on up/down limit
    :param alpha: alpha positions
    :param lmt_hit: indicators (1,0,-1) on whether up/down limit is hit or not
    :return:
    """
    lmt_hit = lmt_hit.reindex(index=alpha.index, columns=alpha.columns).fillna(0)
    alpha_pos = alpha.copy()
    pre_pos = pd.Series(0, index=alpha.columns)
    for dt, pos in alpha_pos.iterrows():
        lmt_hit_row = lmt_hit.loc[dt]
        mask = ((pos > pre_pos) & (lmt_hit_row == 1)) | ((pos < pre_pos) & (lmt_hit_row == -1))  # can't long stock if the up-limit is hit, or short stock if the down-limit is hit.
        alpha_pos.loc[dt] = pos.mask(mask, pre_pos)
        pre_pos = alpha_pos.loc[dt]
    return alpha_pos



def calc_perf_stats(alpha, rtn):
    """Calculate performance stats.
    :param alpha: alpha positions
    :param rtn: daily returns
    :return:
    """
    rtn = rtn.reindex(index=alpha.index, columns=alpha.columns)
    alpha = alpha.shift()  # need to align alpha position to the next day's return
    alpha_chg = alpha.diff()
    pnl = (alpha * rtn).sum(axis=1)
    cpnl = pnl.cumsum()
    long_pnl = (alpha[alpha > 0] * rtn).sum(axis=1)
    short_pnl = (alpha[alpha < 0] * rtn).sum(axis=1)
    long_cpnl = long_pnl.cumsum()
    short_cpnl = short_pnl.cumsum()

    cumulative_return = cpnl.iloc[-1]  # last value in the cumulative PnL
    num_days = cpnl.shape[0]
    annualized_return = ((1 + cumulative_return) ** (252 / num_days)) - 1
    daily_std_dev = pnl.std()
    annualized_risk = daily_std_dev * (252 ** 0.5)

    # Drawdown
    drawdown = cpnl - cpnl.cummax()

    # Turnover
    turnover = alpha_chg.abs().sum(axis=1) * 0.5
    turnover = turnover[turnover > 0]
    turnover = turnover.iloc[1:]

    # Information Coefficient
    cross_sectional_ic = alpha.corrwith(rtn, axis=1, method='spearman')  # IC across stocks --> return a time series

    summary = {
        'pnl_sum': pnl.sum(),
        'max_drawdown': drawdown.min(),
        'turnover_avg': turnover.mean(),
        'Rank_IC': cross_sectional_ic.mean(),
        'sharpe': pnl.mean() / pnl.std() * (252 ** 0.5),
        'annualized_return': annualized_return,
        'annualized_risk': annualized_risk,
    }
    summary['info_ratio'] = summary['Rank_IC'] / cross_sectional_ic.std()  # cross-sectional information ratio

    details = {'cum_pnl': cpnl, 'drawdown': drawdown, 'turnover': turnover,
               'cross_sectional_ic': cross_sectional_ic,
               'long_pnl': long_cpnl, 'short_pnl': short_cpnl,
               }

    return pd.Series(summary), details


def _single_alpha_perfmc(alpha_key, start, end, rtn, lmt_hit, plot=True):
    """Calculate single alpha performance metrics.
    :param alpha_key: alpha key
    :param start: start date
    :param end: end date
    :param rtn: daily returns
    :param lmt_hit: limit hit indicators
    :param plot: whether to generate plots or not
    :return: summary stats
    """
    alpha = DataLoader().load('alpha.pkl', start, end, alpha_key=alpha_key)
    alpha_adjed = adj_alpha(alpha, lmt_hit)  # adjust alpha based on up/down limit
    summary, details = calc_perf_stats(alpha_adjed, rtn)
    summary.name = alpha_key
    if plot:
        plot_dir = get_path('alpha.plot', alpha_key)
        for key in ['drawdown']:
            plot_timeseries(details[key], plot_dir, key)
        for key in ['cross_sectional_ic']:
            plot_density(details[key], plot_dir, key)

        turnover_details = {
            'total': details['turnover'],
        }
        plot_turnover_with_idx(turnover_details, plot_dir, 'turnover')

        pnl_details = {
            'cumulative': details['cum_pnl'],
            'long': details['long_pnl'],
            'short': details['short_pnl']
        }
        plot_pnl_with_idx(pnl_details, plot_dir, 'pnl')

    return summary


def alpha_perfmc(cfg_list, start, end, parallel=False):
    """Alpha performance calculation.
    :param cfg_list: config list
    :param start: start date
    :param end: end date
    :param parallel: boolean to use parallel (True) or sequential (False) processing
    :return: DataFrame of results
    """
    rtn, lmt_hit = prepare_alpha_perfmc(start, end)
    helper = partial(_single_alpha_perfmc, start=start, end=end, rtn=rtn, lmt_hit=lmt_hit, plot=True)

    if parallel:
        with multiprocessing.Pool() as pool:
            res = pool.map_async(helper, cfg_list)
            output = res.get()
    else:
        output = [helper(cfg) for cfg in cfg_list]  # Process sequentially in a list comprehension

    output = pd.concat(output, axis=1)  # Concatenate all DataFrame results
    return output


