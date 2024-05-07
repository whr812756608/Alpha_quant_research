import pandas as pd
import matplotlib.pyplot as plt
from alpha_cal.alpha_calc import alpha_calc
from alpha_perfmc.alpha_perfmc import alpha_perfmc
from alpha_perfmc.alpha_perfmc_plot import plot_time_series_cumpnl_idx
from data_etl.loader import DataLoader
from alpha_utl.alpha_test import alpha_test

test = False
start, end = '2018-1-1', '2020-12-31'

cfg_dict = {
            #'alpha_custom_003_2': {'univ': ['zz9999'], 'args': [(5, 30, True), (5, 20, True), (10, 30, True), (10, 20, True)]},
            #'alpha_custom_004_3': {'univ': ['zz9999'], 'args': [(15, 100), (15, 50), (15, 150)]},
            #'alpha_custom_005': {'univ': ['zz9999'], 'args': [(0.75,), (0.5,), (0.9,)]},
            #'alpha_custom_006': {'univ': ['zz9999'], 'args': [(0.25, 10, True)]},
            #'alpha_007': {'univ': ['zz9999'], 'args': [(5, True),(1, True)]},
            'alpha_custom_008': {'univ': ['zz9999'], 'args': [(-1,),(1,)]},
            }
alpha_calc(cfg_dict, start, end)

cfg_list_pfmc = [
# 'alpha_custom_003_2-zz9999-(5, 30, True)',
# 'alpha_custom_003_2-zz9999-(5, 20, True)',
# 'alpha_custom_003_2-zz9999-(10, 30, True)',
# 'alpha_custom_003_2-zz9999-(10, 20, True)',
# 'alpha_custom_004_3-zz9999-(15, 100)',
# 'alpha_custom_004_3-zz9999-(15, 50)',
# 'alpha_custom_004_3-zz9999-(15, 150)',
# 'alpha_custom_006-zz9999-(0.25, 20, True)',
#'alpha_custom_006-zz9999-(0.25, 10, True)',
# 'alpha_custom_006-zz9999-(0.25, 30, True)',
# 'alpha_007_2-zz9999-(5, True)',
# 'alpha_007_2-zz9999-(1, True)',
# 'alpha_custom_007-zz9999-(5,)',
# 'alpha_custom_007-zz9999-(1,)',
'alpha_custom_008-zz9999-(1,)',
'alpha_custom_008-zz9999-(-1,)',
            ]

summary = alpha_perfmc(cfg_list_pfmc, start, end)
for key, value in summary.items():
    print("Key:", key)
    print("Value:", value)

if test:
    alpha_keys = ['alpha_mom_rev_4', 'alpha_mom_rev_6']
    alpha_test(alpha_keys)
    cfg_list_pfmc = [
    alpha_key for alpha_key in alpha_keys
        ]
    summary = alpha_perfmc(cfg_list_pfmc, start, end)
    for key, value in summary.items():
        print("Key:", key)
        print("Value:", value)
        print()










