import pandas as pd

def calculate_position(data, switch_positions=False):
    """
    Calculate normalized weights based on ranks of alpha values, optionally switching signs.

    Parameters:
    data (pd.DataFrame): DataFrame containing the columns 'date', 'code', and 'alpha_val'.
    switch_positions (bool): If True, switch the signs of the calculated weights.

    Returns:
    pd.DataFrame: A pivot table with dates as index, codes as columns, and weights as values.
    """

    # Rank alpha values within each date
    data['rank'] = data.groupby('date')['alpha_val'].rank(method='dense')

    # Calculate mean of ranks for each date and demean the ranks
    data['rank_mean'] = data.groupby('date')['rank'].transform('mean')
    data['demean'] = data['rank'] - data['rank_mean']
    #data['demean'] = data['demean'].fillna(0)  # Fill NA/NaN values with 0

    # Separate data into positive and negative demeaned values
    positive_demean = data[data['demean'] > 0].copy()
    negative_demean = data[data['demean'] < 0].copy()

    # Calculate normalized weights
    positive_demean['wgt'] = positive_demean['demean'] / positive_demean.groupby('date')['demean'].transform('sum')
    negative_demean['wgt'] = negative_demean['demean'] / negative_demean.groupby('date')['demean'].transform('sum').abs()

    # Merge back the positive and negative weights into the main DataFrame
    data['wgt'] = pd.concat([positive_demean['wgt'], negative_demean['wgt']])

    # Optionally flip the signs of the weights
    if switch_positions:
        data['wgt'] *= -1

    # Create a pivot table with dates as index, codes as columns, and weights as values
    pos = data.pivot_table(values='wgt', index='date', columns='code', fill_value=0)
    pos = pos[pos != 0]
    return pos