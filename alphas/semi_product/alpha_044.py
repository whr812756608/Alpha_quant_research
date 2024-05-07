from alpha_utl.alpha_helper import*
from alpha_utl.alpha_to_position import*


# Alpha#44	 (-1 * correlation(high, rank(volume), 5))
# postive for long
# negative for short
# switch long short if switch_positions = Ture
def alpha_044(data, corr_window = 5,switch_positions = False):
    # Group by 'code' if the dataframe includes multiple stocks
    data['ranked_volume'] = data.groupby('date')['volume'].rank()

    # Apply the correlation function to each group
    data['corr'] = data.groupby('code').apply(
        lambda group: correlation(group['high'], group['ranked_volume'], window=corr_window)
    ).reset_index(level=0, drop=True)  # This removes the added index from grouping

    # Replace infinite values, fill NaNs, and negate the result
    data['alpha_val'] = -1 * data['corr'].replace([-np.inf, np.inf], 0)

    pos = calculate_position(data,switch_positions)

    return pos

    # data['rank'] = data.groupby('date')['alpha_val'].rank(method='dense')
    # data['rank_mean'] = data.groupby('date')['rank'].transform('mean')
    # data['demean'] = data['rank'] - data['rank_mean']
    # data['demean'].fillna(0)
    #
    # # Calculate normalized weights for positive and negative demeaned values
    # positive_demean = data[data['demean'] > 0].copy()
    # negative_demean = data[data['demean'] < 0].copy()
    # positive_demean['wgt'] = positive_demean['demean'] / positive_demean.groupby('date')['demean'].transform('sum')
    # negative_demean['wgt'] = negative_demean['demean'] / negative_demean.groupby('date')['demean'].transform(
    #     'sum').abs()
    #
    # # Merge back the positive and negative weights into the main DataFrame
    # data['wgt'] = pd.concat([positive_demean['wgt'], negative_demean['wgt']])
    #
    # if switch_positions:
    #     data['wgt'] *= -1  # Flipping the sign of the weight
    #
    # # Return a pivot table to format data into the desired structure
    # pos = data.pivot_table(values='wgt', index='date', columns='code', fill_value=0)
    #
    # return pos

# start, end = '2018-1-1', '2020-12-31'
# data = prepare_alpha_data(start, end, fields=['date', 'code', 'pre_close', 'volume','close', 'open', 'low', 'high', 'chg_pct'])
# pos = alpha_044(data)
# total_positive_sum = pos[:1][pos[:1] > 0].sum().sum()
# total_negative_sum = pos[pos < 0].sum().sum()
#
# print("Total Positive Sum:", total_positive_sum)
# print("Total Negative Sum:", total_negative_sum)

