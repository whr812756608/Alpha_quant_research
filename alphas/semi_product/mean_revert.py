"""
Mean Reverion Strategy
"""

import pandas as pd


def mean_revert(data, lookback, zscore_thrshld):
    """Bollinger band strategy.
    :param data: close price
    :param lookback: # of days for the lookback period
    :param zscore_thrshld: z-score threshold to trigger long/short position
    :return:
    """
    # assume we run alpha target 5 mins before close
    close = pd.pivot_table(data[['code', 'date', 'close_adjed']], index='date', columns='code',
                           values='close_adjed')
    roll_mean = close.rolling(window=lookback, min_periods=lookback).mean()
    roll_std = close.rolling(window=lookback, min_periods=lookback).std()
    zscore = (close - roll_mean) / roll_std

    pos = pd.DataFrame(0.0, index=close.index, columns=close.columns)
    pos[zscore > zscore_thrshld] = -1  # short if z-score exceeds the upper bollinger band
    pos[zscore < -zscore_thrshld] = 1  # long if z-score moves below the lower bollinger band

    # Normalize positions to dollar neutral
    pos[pos == 1] = pos[pos == 1].divide(pos[pos == 1].sum(axis=1), axis=0)
    pos[pos == -1] = -pos[pos == -1].divide(pos[pos == -1].sum(axis=1), axis=0)

    return pos
