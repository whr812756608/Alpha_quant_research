import logging
from data_etl.loader import DataLoader
from data_etl.processor import DataProcessor
from data_etl.saver import DataSaver
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)
LOG_LEVEL = logging.INFO
logging.basicConfig(level=LOG_LEVEL)



def process_adj_fct(dl, dp, ds):
    """
    :param dl: data loader
    :param dp: data processor
    :return:
    """
    adj_fct_raw = dl.load('adj_fct.raw')
    processed_data = dp.process('adj_fct', adj_fct_raw)
    ds.save('adj_fct.pkl', processed_data)
    # Save or further use `processed_data`


def process_dates(dl, dp, ds):
    """
    Load, process, and save date-related data.
    :param dl: Data loader
    :param dp: Data processor
    :param ds: Data saver
    :return: None
    """
    # Load and process trading dates
    trd_date_raw = dl.load('trd_date.raw','2018-1-1', '2020-12-31')
    trd_date_processed = dp.process('trd_date', trd_date_raw)
    ds.save('trd_date.pkl', trd_date_processed)  # Save processed trading dates

    # Load, process, and save halt dates
    halt_date_raw = dl.load('halt_date.raw')
    halt_date_processed = dp.process('halt_date', halt_date_raw, trd_date_processed)
    ds.save('halt_date.pkl', halt_date_processed)  # Save processed halt dates

    # Load, process, and save list/delist dates
    lst_date_raw = dl.load('lst_date.raw')
    lst_date_processed = dp.process('lst_date', lst_date_raw, trd_date_processed)
    ds.save('lst_date.pkl', lst_date_processed)  # Save processed list/delist dates

    # Load, process, and save status dates
    st_date_raw = dl.load('st_date.raw')
    st_date_processed = dp.process('st_date', st_date_raw)
    ds.save('st_date.pkl', st_date_processed)  # Save processed status dates

    # Handle the processed data, save or log

def process_and_save_1min_interim(dl, dp, ds):
    """
    Load 1min data, process it and save the interim results using the DataSaver.
    :param dl: DataLoader instance for loading data
    :param dp: DataProcessor instance for processing data
    :param ds: DataSaver instance for saving data
    :return: None
    """
    # Load trading dates assuming 'trd_date.pkl' is the filename where trading dates are stored.
    trd_dates_df = dl.load('trd_date.pkl')
    if trd_dates_df is not None and not trd_dates_df.empty:
        for dt in trd_dates_df['date']:
            logger.info(f'Processing and saving 1min data for date: {dt}')
            # Load one-minute data for the specific date
            one_min_raw = dl.load('1min.raw', str(dt), str(dt))

            # Process the loaded data
            processed_data = dp.process('1min', one_min_raw)

            # Save the processed data using DataSaver
            ds.save('1min_interim.pkl', processed_data)  # Ensure this matches a key in SAVER_MAPPING
    else:
        logger.warning("No trading dates found or trading dates data is empty.")


def process_1min(dl, dp, ds):
    """
    Aggregate 1min into daily; calculate change and percent change, then save.
    :param dl: DataLoader instance for loading data
    :param dp: DataProcessor instance for processing data
    :param ds: DataSaver instance for saving data
    :return: None
    """
    # Load adjustment factors, assumed to be pre-processed and stored in 'adj_fct.pkl'
    adj_fct = dl.load('adj_fct.pkl', fields=['code', 'date', 'cum_adjf'])

    # List to collect monthly aggregated data
    one_min_agg = []
    for year in range(2018, 2021):
        for month in range(1, 13):
            logger.info(f'Processing data for {year}-{month}')
            # Define the start and end of the month
            start_date = pd.Timestamp(f'{year}-{month}-1')
            end_date = start_date + pd.offsets.MonthEnd(0)

            # Load interim one-minute data for the month
            one_min_raw = dl.load('1min.raw', start_date, end_date)

            # Process the loaded data if it is not empty
            if one_min_raw is not None and not one_min_raw.empty:
                processed_data = dp.process('1min', one_min_raw)
                one_min_agg.append(processed_data)

    # Combine all monthly data into a single DataFrame
    if one_min_agg:
        one_min_agg = pd.concat(one_min_agg)
        one_min_agg = one_min_agg.sort_values(['code', 'date'])

        # Calculate change and percent change from the previous close
        one_min_agg['chg'] = one_min_agg['close'] - one_min_agg['pre_close']
        one_min_agg['chg_pct'] = one_min_agg['chg'] / one_min_agg['pre_close']

        # Save the aggregated data
        ds.save('1min_agg.pkl', one_min_agg)
    else:
        logger.warning("No data was processed. Check the data availability and loading logic.")

def process_other_data(dl, ds):
    """
    :param dl:
    :param dp:
    :return:
    """
    for tbl in ['idx', 'lmt', 'mkt_val', 'sw', 'hs300', 'zz500', 'zz800', 'zz1000', 'zz9999']:
        raw = dl.load(f'{tbl}.raw')
        ds.save(f'{tbl}.pkl', raw)

def process_1min_skw(dl, dp, ds):
    """
    Aggregate 1min into daily; calculate change and percent change, then save.
    :param dl: DataLoader instance for loading data
    :param dp: DataProcessor instance for processing data
    :param ds: DataSaver instance for saving data
    :return: None
    """
    # List to collect monthly aggregated data
    one_min_agg = []
    for year in range(2018, 2021):
        for month in range(1, 13):
            logger.info(f'Processing data for {year}-{month}')
            # Define the start and end of the month
            start_date = pd.Timestamp(f'{year}-{month}-1')
            end_date = start_date + pd.offsets.MonthEnd(0)

            # Load interim one-minute data for the month
            one_min_raw = dl.load('1min.raw', start_date, end_date)
            # Process the loaded data if it is not empty
            if one_min_raw is not None and not one_min_raw.empty:
                processed_data = dp.process('1min_skw', one_min_raw)
                one_min_agg.append(processed_data)
    # Combine all monthly data into a single DataFrame
    if one_min_agg:
        # Save the aggregated data
        one_min_agg = pd.concat(one_min_agg)
        ds.save('1min_skw.pkl', one_min_agg)
    else:
        logger.warning("No data was processed. Check the data availability and loading logic.")

def process_1min_astd(dl, dp, ds):
    """
    Aggregate 1min into daily; calculate change and percent change, then save.
    :param dl: DataLoader instance for loading data
    :param dp: DataProcessor instance for processing data
    :param ds: DataSaver instance for saving data
    :return: None
    """
    # List to collect monthly aggregated data
    one_min_agg = []
    for year in range(2018, 2021):
        for month in range(1, 13):
            logger.info(f'Processing data for {year}-{month}')
            # Define the start and end of the month
            start_date = pd.Timestamp(f'{year}-{month}-1')
            end_date = start_date + pd.offsets.MonthEnd(0)

            # Load interim one-minute data for the month
            one_min_raw = dl.load('1min.raw', start_date, end_date)
            # Process the loaded data if it is not empty
            if one_min_raw is not None and not one_min_raw.empty:
                processed_data = dp.process('1min_astd', one_min_raw)
                one_min_agg.append(processed_data)
    # Combine all monthly data into a single DataFrame
    if one_min_agg:
        # Save the aggregated data
        one_min_agg = pd.concat(one_min_agg)
        one_min_agg = one_min_agg.reset_index(name='astd')
        ds.save('1min_astd.pkl', one_min_agg)
    else:
        logger.warning("No data was processed. Check the data availability and loading logic.")

dl = DataLoader()
dp = DataProcessor()
ds = DataSaver()

process_1min_astd(dl, dp, ds)




