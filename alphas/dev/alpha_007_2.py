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

def alpha_007_2(data, skw_window=1, switch_positions=False):
    df = data.copy()
    # Select skewness column based on window size
    skew_col = 'skw_1min' if skw_window == 1 else 'skw_5min'
    df['alpha_val'] = df[skew_col]

    pos = calculate_position(df, switch_positions)
    return pos

