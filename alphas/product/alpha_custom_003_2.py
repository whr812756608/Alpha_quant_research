import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from data_etl.loader import DataLoader
from alpha_cal.alpha_calc_prepare import prepare_alpha_data
from alpha_cal.alpha_calc import filter_for_univ
from alpha_utl.alpha_to_position import calculate_position
from alpha_utl.alpha_value_dist_plot import plot_alpha_distribution_on_random_day
from alpha_utl.alpha_helper import*
from alpha_utl.alpha_pos_dist_plot import plot_pos_distribution_on_random_day
import seaborn as sns
def alpha_custom_003_2(data, short_window=10, long_window=30, switch_positions=False):

    df = data.copy()
    stock_groups = df.groupby('code')
    # Calculate returns over different windows
    df['close_short'] = stock_groups['close'].shift(short_window)
    df['momentum_short'] = (df['close'] / df['close_short']) - 1.0

    df['close_long'] = stock_groups['close'].shift(long_window)
    df['momentum_long'] = (df['close'] / df['close_long']) - 1.0

    df['acceleration_short'] = stock_groups['momentum_short'].diff()
    df['acceleration_long'] = stock_groups['momentum_long'].diff()

    # df['correlation_short'] = correlation(df['return_short'],df['acceleration_short'],5)
    # df['correlation_long'] = correlation(df['return_long'], df['acceleration_long'], 5)
    # Calculate correlations to check consistency between short-term and long-term trends
    # df['correlation_short'] = stock_groups.apply(lambda x: x['return_short'].corr(x['acceleration_short']))
    # df['correlation_long'] = stock_groups.apply(lambda x: x['return_long'].corr(x['acceleration_long']))
    df['mom_x_acc_long'] = df['momentum_long'] * df['acceleration_long']
    df['mom_x_acc_short'] = df['momentum_short'] * df['acceleration_short']
    # Define the alpha as a product of the correlations; negative to identify divergence
    df['alpha_val'] = correlation(df['mom_x_acc_long'],df['mom_x_acc_short'],5)

    # Position calculation can include logic to decide based on the sign and magnitude of alpha
    pos = calculate_position(df, switch_positions=switch_positions)
    return pos

plot_pos_distribution_on_random_day(alpha_custom_003_2)


