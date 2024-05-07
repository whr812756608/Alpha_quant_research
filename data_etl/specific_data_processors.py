"""
Specific data processors
"""
import pandas as pd
import numpy as np


def process_adj_fct(adj_fct_raw):
    """Process adj factors.
    :param adj_fct_raw:
    :return:
    """
    adj_fct = adj_fct_raw.sort_values(['code', 'date'])
    adj_fct['first_adjf'] = adj_fct.groupby('code')['cum_adjf'].transform('first')
    adj_fct['cum_adjf'] = adj_fct['cum_adjf'] / adj_fct['first_adjf']
    return adj_fct

def proces_1min_skw(one_min_raw):
    one_min_raw_ = one_min_raw[(one_min_raw['open'] > 0) & (one_min_raw['close'] > 0) & (one_min_raw['low'] > 0)]
    one_min_raw_['rtn_1min'] = np.log(one_min_raw_['close']) - np.log(one_min_raw_.groupby(['code', 'date'])['close'].shift())
    one_min_raw_['rtn_5min'] = np.log(one_min_raw_['close']) - np.log(one_min_raw_.groupby(['code', 'date'])['close'].shift(5))
    one_min_raw_ = one_min_raw_.replace([np.inf, -np.inf], np.nan)  # Replace infinities with NaN
    one_min_raw_ = one_min_raw_.dropna(subset=['rtn_1min', 'rtn_5min'])
    one_min_raw_['skw_1min'] = one_min_raw_.groupby(['code', 'date'])['rtn_1min'].transform('skew')
    one_min_raw_['skw_5min'] = one_min_raw_.groupby(['code', 'date'])['rtn_5min'].transform('skew')
    one_min_raw_gb = one_min_raw_.groupby(['code', 'date'], as_index=False)
    res = one_min_raw_gb[['skw_1min', 'skw_5min']].first()
    return res

def process_1min_astd(one_min_raw):
    # Ensure all prices are greater than 0
    one_min_raw_ = one_min_raw[(one_min_raw['open'] > 0) & (one_min_raw['close'] > 0) &
                               (one_min_raw['low'] > 0) & (one_min_raw['high'] > 0)]

    # Calculate returns for reference
    one_min_raw_['rtn_5min'] = np.log(one_min_raw_['close']) - np.log(
        one_min_raw_.groupby(['code'])['close'].shift(5))
    one_min_raw_ = one_min_raw_.replace([np.inf, -np.inf], np.nan)

    #Concatenate the open, high, low, and close prices into a single column for rolling operations
    prices = one_min_raw_.set_index(['code', 'date', 'time'])[['open', 'high', 'low', 'close']].stack().reset_index()
    prices.columns = ['code', 'date', 'time', 'type', 'price']

    # Sort values to ensure the rolling operation respects the time order
    prices = prices.sort_values(by=['code', 'time'])

    groups = prices.groupby(['code'])

    rolling_mean = groups['price'].rolling(20).mean()
    rolling_std = groups['price'].rolling(20).std()
    rsd = rolling_mean / rolling_std.pow(2)

    prices['rsd'] = rsd.values
    prices = prices[prices['type'] == 'close']

    merged_df = pd.merge(prices[['date', 'code', 'time', 'rsd']],
                         one_min_raw_[['date', 'code', 'time', 'rtn_5min']],
                         on=['date', 'code', 'time'],
                         how='inner')
    merged_df = merged_df.dropna(subset=['rtn_5min', 'rsd'])

    merged_df['ratio'] = merged_df['rtn_5min'] / merged_df['rsd']

    group = merged_df.groupby(['code','date'])

    def calculate_covariance(group):
        return group['ratio'].cov(group['rsd'])

    res = group.apply(calculate_covariance)

    return res


def process_1min(one_min_raw):
    """Aggregate 1min into daily, and also
    - Extract additional features such as volatility;
    - Remove zeros for 'open', 'close', 'low';
    :param one_min_raw:
    :return:
    """
    # Remove rows with zeros in the 'open', 'close' or 'low' column.
    # Todo: Need more study to understand zero prices.
    one_min_raw_ = one_min_raw[(one_min_raw['open'] > 0) & (one_min_raw['close'] > 0) & (one_min_raw['low'] > 0)]
    one_min_raw_gb = one_min_raw_.groupby(['code', 'date'], as_index=False)

    res = one_min_raw_gb[['open', 'pre_close']].first(). \
        merge(one_min_raw_gb['high'].max(), on=['code', 'date']). \
        merge(one_min_raw_gb['low'].min(), on=['code', 'date']). \
        merge(one_min_raw_gb['close'].last(), on=['code', 'date']). \
        merge(one_min_raw_gb[['volume', 'turover']].sum(), on=['code', 'date'])

    close_1455 = one_min_raw_[one_min_raw_['time'] <= 1455].groupby(['code', 'date'], as_index=False)['close'].last()
    res = res.merge(close_1455.rename(columns={'close': 'close_1455'}), on=['code', 'date'])

    # price volatility
    res = res.merge(one_min_raw_gb['close'].std().rename(columns={'close': 'std'}), on=['code', 'date'])

    # first 30-min price volatility
    std_1st_30 = one_min_raw[one_min_raw['time'] <= 1000].groupby(['code', 'date'], as_index=False)['close'].std(). \
        rename(columns={'close': 'std_1st_30'})
    res = res.merge(std_1st_30, on=['code', 'date'])

    return res

# def process_1min(one_min_raw):
#     """Aggregate 1min into daily, and also
#     - Extract additional features such as volatility;
#     - Remove zeros for 'open', 'close', 'low';
#     :param one_min_raw:
#     :return:
#     """
#     # Remove rows with zeros in the 'open', 'close' or 'low' column.
#     # Todo: Need more study to understand zero prices.
#     one_min_raw_ = one_min_raw[(one_min_raw['open'] > 0) & (one_min_raw['close'] > 0) & (one_min_raw['low'] > 0)]
#     one_min_raw_gb = one_min_raw_.groupby(['code', 'date'], as_index=False)
#
#     res = one_min_raw_gb[['open', 'pre_close']].first(). \
#         merge(one_min_raw_gb['high'].max(), on=['code', 'date']). \
#         merge(one_min_raw_gb['low'].min(), on=['code', 'date']). \
#         merge(one_min_raw_gb['close'].last(), on=['code', 'date']). \
#         merge(one_min_raw_gb[['volume', 'turover']].sum(), on=['code', 'date'])
#
#     close_1455 = one_min_raw_[one_min_raw_['time'] <= 1455].groupby(['code', 'date'], as_index=False)['close'].last()
#     res = res.merge(close_1455.rename(columns={'close': 'close_1455'}), on=['code', 'date'])
#
#     # price volatility
#     res = res.merge(one_min_raw_gb['close'].std().rename(columns={'close': 'std'}), on=['code', 'date'])
#
#     # first 30-min price volatility
#     std_1st_30 = one_min_raw[one_min_raw['time'] <= 1000].groupby(['code', 'date'], as_index=False)['close'].std(). \
#         rename(columns={'close': 'std_1st_30'})
#     res = res.merge(std_1st_30, on=['code', 'date'])
#
#     return res

def process_trd_date(trd_date_raw):
    """Process trd dates.
    :param trd_date_raw:
    :return:
    """
    res = trd_date_raw[trd_date_raw['is_open'] == 1].drop(columns='is_open').copy()
    return res


def process_halt_date(halt_date_raw, trd_date):
    """Align halt dates
    :param halt_date_raw:
    :param trd_date:
    :return:
    """
    res = halt_date_raw[['code', 'halt_date', 'restart_date']].drop_duplicates().assign(dummy=1). \
        merge(trd_date[['date']].assign(dummy=1), on='dummy').drop(columns='dummy')
    res = res[(res['date'] >= res['halt_date']) & (res['date'] <= res['restart_date'])]
    res = res.merge(halt_date_raw, on=['code', 'halt_date', 'restart_date'])
    res['halt_time'] = res['halt_time'].mask(res['halt_date'] != res['date'], np.nan)
    res['restart_time'] = res['restart_time'].mask(res['restart_date'] != res['date'], np.nan)
    return res


def process_lst_date(lst_date_raw, trd_date):
    """Align list/delist dates
    :param lst_date_raw:
        L: list, DE: delist, S: stop/pause, O: other, UN: not-listed
    :param trd_date:
    :return:
    """
    res = lst_date_raw.loc[lst_date_raw['lst_status'].isin(['L', 'DE']), ['code', 'lst_status', 'lst_date', 'delst_date']]. \
        drop_duplicates().assign(dummy=1).merge(trd_date[['date']].assign(dummy=1), on='dummy').drop(columns='dummy')
    res = res[((res['lst_date'] <= res['date']) | res['lst_date'].isna()) &
              ((res['delst_date'] >= res['date']) | res['delst_date'].isna())]
    res = res.merge(lst_date_raw, on=['code', 'lst_status', 'lst_date', 'delst_date'])
    return res


def process_st_date(st_date_raw):
    """Status and date
        id = 1, delist ST
        id = 2, list ST
        id = 3, relist
        id = 4, delist
        id = 5, first day in delist period
        id = 6, last day in delist period
    :param st_date_raw:
    :return:
    """
    res = st_date_raw.rename(columns={'eff_date': 'date'})
    return res


PROC_MAPPING = {
    '1min': process_1min,
    '1min_skw': proces_1min_skw,
    '1min_astd': process_1min_astd,
    'adj_fct': process_adj_fct,
    'trd_date': process_trd_date,
    'halt_date': process_halt_date,
    'lst_date': process_lst_date,
    'st_date': process_st_date}
