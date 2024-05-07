import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from data_etl.loader import DataLoader
from alpha_cal.alpha_calc_prepare import prepare_alpha_data
from alpha_cal.alpha_calc import filter_for_univ
from alpha_utl.alpha_to_position import calculate_position
from alpha_utl.alpha_helper import*
from alpha_utl.alpha_value_dist_plot import plot_alpha_distribution_on_random_day
from alpha_utl.alpha_pos_dist_plot import plot_pos_distribution_on_random_day

def alpha_custom_007(data, skw_window=1, switch_positions=False):
    df = data.copy()
    # Select skewness column based on window size
    skew_col = 'skw_1min' if skw_window == 1 else 'skw_5min'
    df['alpha_val'] = df[skew_col]

    # Determine positions
    low_quantile = df['alpha_val'].quantile(0.20)
    high_quantile = df['alpha_val'].quantile(0.80)
    if switch_positions:
        df['position'] = np.where(df['alpha_val'] <= low_quantile, 1,
                                  np.where(df['alpha_val'] >= high_quantile, -1, 0))
    else:
        df['position'] = np.where(df['alpha_val'] <= low_quantile, -1,
                                  np.where(df['alpha_val'] >= high_quantile, 1, 0))

    # Normalize positions for each day within the data
    for date, group in df.groupby('date'):
        sum_positive = group['position'][group['position'] > 0].sum()
        sum_negative = -group['position'][group['position'] < 0].sum()
        df.loc[group.index, 'position'] = group['position'].apply(
            lambda x: x / sum_positive if x > 0 else (x / sum_negative if x < 0 else 0)
        )

    pos = df.pivot_table(values='position', index='date', columns='code', fill_value=0)

    return pos




