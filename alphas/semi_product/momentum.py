"""
Momentum Strategy
"""
import pandas as pd


def momentum(data, lookback = 5, thrshld = 0.02):
    """Momentum strategy.
    :param data:
    :param lookback: # of days for the lookback period
    :param thrshld: threshold to trigger long/short position
    :return:
    """
    rtns = pd.pivot_table(data[['code', 'date', 'rtn']], index='date', columns='code', values='rtn')
    roll_sum = rtns.rolling(window=lookback, min_periods=lookback).sum()
    

    data['alpha_val'] = roll_sum

    pos = pd.DataFrame(0.0, index=rtns.index, columns=rtns.columns)
    pos[roll_sum > thrshld] = 1  # long if the past perf exceeds certain threshold
    pos[roll_sum < -thrshld] = -1  # short if the past perf under-shoots certain threshold

    # Normalize positions to dollar neutral
    pos[pos == 1] = pos[pos == 1].divide(pos[pos == 1].sum(axis=1), axis=0)
    pos[pos == -1] = -pos[pos == -1].divide(pos[pos == -1].sum(axis=1), axis=0)
    return pos
