import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from data_etl.loader import DataLoader
from alpha_cal.alpha_calc_prepare import prepare_alpha_data
from alpha_cal.alpha_calc import filter_for_univ
from alpha_utl.alpha_to_position import calculate_position
from alpha_utl.alpha_helper import*
from alpha_utl.alpha_value_dist_plot import plot_alpha_distribution_on_random_day

def alpha_custom_005(data, quant = 0.75, cols = ['volume','close'], switch_positions = False):
    df = data.copy()
    # Grouping data by date to simulate cross-sectional analysis
    df_groups = df.groupby('date')

    # Calculate quantiles within each date for specified columns
    for q in [quant]:  # You can adjust quantiles as needed
        for col in cols:
            df[f'{col}_quantile_{q}'] = df_groups[col].transform(lambda x: x.quantile(q))
            df[f'{col}_quantile_diff_{q}'] = df[f'{col}'] - df[f'{col}_quantile_{q}']

    df['alpha_val'] = df[f'volume_quantile_diff_{q}'] * df[f'close_quantile_diff_{q}']

    pos = calculate_position(df, switch_positions=switch_positions)

    return pos

# plot_alpha_distribution_on_random_day(alpha_quantile_based)




#
