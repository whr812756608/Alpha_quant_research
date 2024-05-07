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


def alpha_custom_008(data, flip=1, switch_positions=False):
    df = data.copy()
    if flip == 1:
        df['alpha_val'] = df['astd']
    elif flip == -1:
        df['alpha_val'] = -df['astd']

    pos = calculate_position(df,switch_positions)
    return pos
