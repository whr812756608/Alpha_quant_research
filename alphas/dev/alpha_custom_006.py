import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from data_etl.loader import DataLoader
from alpha_cal.alpha_calc_prepare import prepare_alpha_data
from alpha_cal.alpha_calc import filter_for_univ
from alpha_utl.alpha_to_position import calculate_position
from alpha_utl.alpha_helper import*
from alpha_utl.alpha_value_dist_plot import plot_alpha_distribution_on_random_day
from alpha_utl.alpha_pos_dist_plot import plot_pos_distribution_on_random_day
#
# '''
# https://www.joinquant.com/view/community/detail/6adbc8713b225d209d09dc6e4135a41d
# '''

def alpha_custom_006(data, lam = 0.25, rolling_window = 10, switch_positions = False):
    df = data.copy()
    df['amp'] = (df['high_adjed'])/(df['low_adjed'])-1

    def range_plus(x, np_tmp, rolling_window, lam):
        '''
        计算理想振幅因子
        :param x: Series，收盘价_复权，移动窗口数据，长度为N
        :param np_tmp: ndarray，振幅_1因子
        :param rolling_window: int，表示计算过去N天数据
        :param lam: 划分高低振幅的分位数
        :return:
        '''
        # 对窗口内收盘价_复权进行从小到大排序，并提取排序后index
        li = x.sort_values().index.to_list()
        # 利用排序后index，提取对应振幅_1数据
        # 即窗口内，按照收盘价复权排序后的振幅_1
        np_tmp2 = np_tmp[li]
        # ==分别提取高振幅与低振幅
        t = int(rolling_window * lam)  # 窗口前lam%
        v_low = np_tmp2[:t].mean()
        v_high = np_tmp2[-t:].mean()
        return v_high - v_low  # 计算理想振幅因子

    sort_col = 'close_adjed'
    factor_col = 'amp'
    np_tmp = df[factor_col].values  # ndarray，提速
    df['alpha_val'] = df[sort_col].rolling(rolling_window).apply(range_plus, args=(np_tmp, rolling_window, lam,), raw=False)

    pos = calculate_position(df, switch_positions=switch_positions)

    return pos







