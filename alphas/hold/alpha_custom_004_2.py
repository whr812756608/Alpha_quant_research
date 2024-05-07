import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from data_etl.loader import DataLoader
from alpha_cal.alpha_calc_prepare import prepare_alpha_data
from alpha_cal.alpha_calc import filter_for_univ
from alpha_utl.alpha_to_position import calculate_position
from alpha_utl.alpha_value_dist_plot import plot_alpha_distribution_on_random_day
from alpha_utl.alpha_helper import*

def alpha_custom_004_2(data, window_short=5, window_long=30, lag = 50, switch_positions = False):
    """Calculate current and lagged rolling volatility to identify periodic patterns."""
    df = data.copy()
    stock_groups = df.groupby('code')
    df['return_pct'] = stock_groups['close'].pct_change()
    df['long_volatility'] = stock_groups['return_pct'].transform(lambda x: x.rolling(window=window_long).std())
    df['long_lagged_volatility'] = stock_groups['return_pct'].transform(lambda x: x.shift(lag).rolling(window=window_long).std())
    df['short_lagged_volatility'] = stock_groups['return_pct'].transform(lambda x: x.shift(lag).rolling(window=window_short).std())
    df['short_volatility'] = stock_groups['return_pct'].transform(lambda x: x.rolling(window=window_short).std())

    df['rank_long_volatility'] = df.groupby('date')['long_volatility'].rank(method='dense', ascending=False)
    df['rank_short_volatility'] = df.groupby('date')['short_volatility'].rank(method='dense', ascending=False)
    df['rank_long_lagged_volatility'] = df.groupby('date')['long_lagged_volatility'].rank(method='dense', ascending=False)
    df['rank_short_lagged_volatility'] = df.groupby('date')['short_lagged_volatility'].rank(method='dense', ascending=False)
    # Calculate the alpha value as the product of the ranks of current and lagged volatility
    df['alpha_val'] = (df['rank_long_volatility'] * df['rank_short_volatility'])/(df['rank_short_volatility']*df['rank_short_lagged_volatility'])



    pos = calculate_position(df,switch_positions = switch_positions)
    return pos

    #return df

# plot_alpha_distribution_on_random_day(alpha_custom_004)