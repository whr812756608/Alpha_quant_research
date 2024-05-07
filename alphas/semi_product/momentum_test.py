import pandas as pd
from data_etl.loader import DataLoader
from alpha_cal.alpha_calc_prepare import prepare_alpha_data
from alpha_cal.alpha_calc import filter_for_univ
from alpha_utl.alpha_to_position import calculate_position

def momentum_test(data, window = 5, switch_positions = False):
    """Momentum strategy.
    :param data:
    :param lookback: # of days for the lookback period
    :param thrshld: threshold to trigger long/short position
    :return:
    """
    stock_group = data.groupby('code')
    shift_pre = window - 1
    data['close_4'] = stock_group['close'].shift(shift_pre)
    data['alpha_val'] = (data['close']/data['close_4'])-1.0
    pos = calculate_position(data,switch_positions)

    return pos

# def calculate_weight(data, switch_positions = False):
#
#     # Rank and demean alpha values
#     data['rank'] = data.groupby('date')['alpha_val'].rank(method='dense')
#     data['rank_mean'] = data.groupby('date')['rank'].transform('mean')
#     data['demean'] = data['rank'] - data['rank_mean']
#
#     # Calculate normalized weights for positive and negative demeaned values
#     positive_demean = data[data['demean'] > 0].copy()
#     negative_demean = data[data['demean'] < 0].copy()
#     positive_demean['wgt'] = positive_demean['demean'] / positive_demean.groupby('date')['demean'].transform('sum')
#     negative_demean['wgt'] = negative_demean['demean'] / negative_demean.groupby('date')['demean'].transform('sum').abs()
#
#     # Merge back the positive and negative weights into the main DataFrame
#     data['wgt'] = pd.concat([positive_demean['wgt'], negative_demean['wgt']])
#
#     if switch_positions:
#         data['wgt'] *= -1  # Flipping the sign of the weight
#
#     # Return a pivot table to format data into the desired structure
#     #pos = data.pivot_table(values='wgt', index='date', columns='code', fill_value=0)
#
#     return data
#
# def calculate_pos(data, switch_positions=False):
#     # Assign initial weights based on the sign of 'alpha_val'
#     data = data.dropna(subset=['alpha_val'])
#     data['pos'] = data['alpha_val'].apply(lambda x: 1 if x > 0 else -1)
#
#     # Calculate the sum of positive and negative weights for each date
#     positive_sum = data[data['pos'] > 0].groupby('date')['pos'].transform('sum')
#     negative_sum = data[data['pos'] < 0].groupby('date')['pos'].transform('sum').abs()
#
#     # Normalize weights
#     data.loc[data['pos'] > 0, 'pos'] /= positive_sum
#     data.loc[data['pos'] < 0, 'pos'] /= negative_sum
#
#     if switch_positions:
#         data['pos'] *= -1  # Flipping the sign of the weight
#
#     # pos = data.pivot_table(values='pos', index='date', columns='code', fill_value=0)
#
#     return data
#
# import numpy as np
#
# start, end = '2018-1-1', '2018-12-31'
# data = prepare_alpha_data(start, end, fields=['date', 'code', 'pre_close', 'volume','close', 'open', 'low', 'high', 'chg_pct'])
# data = filter_for_univ(data,'zz9999')
#
# data
#
# data = momentum_test(data)
#
# data = calculate_pos(data)
#
# data = calculate_weight(data)
#
#
# df = data.query('date == 20180109').loc[:,['date','code','alpha_val','wgt','pos','rtn','chg_pct']]
# df_0110 = df[['code','wgt','pos']]
# df_0110
#
# df_0110_pnl_test = data.query('date == 20180110').loc[:,['date','code','alpha_val','wgt','pos','rtn','chg_pct']]
#
# pnl_test = pd.merge(df_0110_pnl_test.drop(columns=['pos', 'wgt']), df_0110[['code', 'pos', 'wgt']],
#                     on='code', how='left')
#
# total_positive_sum = df['wgt'][df['wgt'] > 0].sum().sum()
# total_negative_sum = df['wgt'][df['wgt'] < 0].sum().sum()
#
# print("Total Positive Sum:", total_positive_sum)
# print("Total Negative Sum:", total_negative_sum)
# #
# lmt = DataLoader().load('lmt.pkl', start, end)
# lmt_hit = lmt.query('date == 20180110')
#
# lmt_close = data.query('date == 20180110').loc[:,['code','close']]
# lmt_close
#
# lmt_hit = lmt_hit.merge(lmt_close,on ='code',how = 'right')
#
# lmt_hit[['up_limit', 'down_limit']] *= 1e4  # scale up/down limit to match close
# lmt_hit['hit'] = 0
# lmt_hit['hit'] = lmt_hit['hit'].mask(lmt_hit['close'] >= lmt_hit['up_limit'], 1)
# lmt_hit['hit'] = lmt_hit['hit'].mask(lmt_hit['close'] <= lmt_hit['down_limit'], -1)
# codes_with_hits = lmt_hit[lmt_hit['hit'] != 0]['code'].tolist()
# len(codes_with_hits)
# pnl_test.loc[pnl_test['code'].isin(codes_with_hits), ['wgt', 'pos']] = 0
#
# pnl_test.query('wgt == 0')
#
# pnl_test['pnl'] = pnl_test['pos']*pnl_test['rtn']
# pnl_test['pnl_wgt'] = pnl_test['wgt']*pnl_test['rtn']
# pnl_pos = pnl_test['pnl'].sum()
# pnl = pnl_test['pnl_wgt'].sum()
#
# pnl_pos
#
# pnl_test
# pnl_test['IC'] = pnl_test['pos'].corr(pnl_test['rtn'])
# IC = pnl_test['IC'].mean()
# pnl_test['IC']
# IC#


