import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from data_etl.loader import DataLoader
from alpha_cal.alpha_calc_prepare import prepare_alpha_data
from alpha_cal.alpha_calc import filter_for_univ
from alpha_utl.alpha_to_position import calculate_position
from alpha_utl.alpha_value_dist_plot import plot_alpha_distribution_on_random_day
from alpha_utl.alpha_helper import*

def alpha_custom_003(data, short_window=5, long_window=20, switch_positions=False):
    df = data.copy()
    stock_groups = df.groupby('code')

    # Calculate returns over different windows
    df['return_short'] = stock_groups['close'].transform(lambda x: returns_window(x, window=short_window + 1))
    df['return_long'] = stock_groups['close'].transform(lambda x: returns_window(x, window=long_window + 1))

    # Calculate momentum and acceleration
    df['momentum_short'] = stock_groups['return_short'].transform(lambda x: delta(x, 1))
    df['acceleration_short'] = stock_groups['momentum_short'].transform(lambda x: delta(x, 1))

    df['momentum_long'] = stock_groups['return_long'].transform(lambda x: delta(x, 1))
    df['acceleration_long'] = stock_groups['momentum_long'].transform(lambda x: delta(x, 1))

    df['correlation_short'] = correlation(df['return_short'],df['acceleration_short'],5)
    df['correlation_long'] = correlation(df['return_long'], df['acceleration_long'], 5)
    # Calculate correlations to check consistency between short-term and long-term trends
    # df['correlation_short'] = stock_groups.apply(lambda x: x['return_short'].corr(x['acceleration_short']))
    # df['correlation_long'] = stock_groups.apply(lambda x: x['return_long'].corr(x['acceleration_long']))

    # Define the alpha as a product of the correlations; negative to identify divergence
    df['alpha_val'] = df['correlation_short'] + df['correlation_long']

    # Position calculation can include logic to decide based on the sign and magnitude of alpha
    pos = calculate_position(df, switch_positions=switch_positions)
    return pos


