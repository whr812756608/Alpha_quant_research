from alpha_utl.alpha_helper import*
from alpha_utl.alpha_to_position import*


#Alpha#55	 (-1 * correlation(rank(((close - ts_min(low, 12)) / (ts_max(high, 12) - ts_min(low,12)))), rank(volume), 6))
# postive for long
# negative for short
# switch long short if switch_positions = Ture
def alpha_055(data,ts_window = 12,corr_window = 6, switch_positions = False):
    divisor = (ts_max(data.high, ts_window) - ts_min(data.low, ts_window)).replace(0, 0.0001)
    inner = (data.close - ts_min(data.low, ts_window)) / (divisor)
    data['alpha_val'] = -1 * correlation(rank(inner), rank(data['volume']), corr_window)
    data['alpha_val'].replace([-np.inf, np.inf], 0, inplace=True)

    pos = calculate_position(data, switch_positions)

    return pos


# start, end = '2018-1-1', '2020-12-31'
# data = prepare_alpha_data(start, end, fields=['date', 'code', 'pre_close', 'volume','close', 'open', 'low', 'high', 'chg_pct'])
# pos = alpha_055(data)
# total_positive_sum = pos[:1][pos[:1] > 0].sum().sum()
# total_negative_sum = pos[pos < 0].sum().sum()
#
# print("Total Positive Sum:", total_positive_sum)
# print("Total Negative Sum:", total_negative_sum)