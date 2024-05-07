import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from data_etl.loader import DataLoader
from alpha_cal.alpha_calc_prepare import prepare_alpha_data
from alpha_cal.alpha_calc import filter_for_univ
from alpha_utl.alpha_to_position import calculate_position
from alpha_utl.alpha_value_dist_plot import plot_alpha_distribution_on_random_day
from alpha_utl.alpha_pos_dist_plot import plot_pos_distribution_on_random_day
from alpha_utl.alpha_helper import*


def alpha_custom_004(data, window=15, lag=100, switch_positions = False):
    """Calculate current and lagged rolling volatility to identify periodic patterns."""
    df = data.copy()
    stock_groups = df.groupby('code')
    df['return_pct'] = stock_groups['close'].pct_change()
    df['current_volatility'] = stock_groups['return_pct'].transform(lambda x: x.rolling(window=window).std())
    df['lagged_volatility'] = stock_groups['return_pct'].transform(lambda x: x.shift(lag).rolling(window=window).std())

    df['rank_current_volatility'] = df.groupby('date')['current_volatility'].rank(method='dense', ascending=False)
    df['rank_lagged_volatility'] = df.groupby('date')['lagged_volatility'].rank(method='dense', ascending=False)

    # Calculate the alpha value as the product of the ranks of current and lagged volatility
    df['alpha_val'] = df['current_volatility'] / df['lagged_volatility']

    pos = calculate_position(df,switch_positions = switch_positions)
    return pos
    #return df


plot_pos_distribution_on_random_day(alpha_custom_004, date = '20180109')
