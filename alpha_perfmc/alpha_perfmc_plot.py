"""
Set of plot functions for detailed daily alpha performance metrics.
"""

import os
import matplotlib.pyplot as plt
from data_etl.loader import DataLoader


def format_label(label):
    """Format label for plotting.
    :param label:
    :return:
    """
    return ' '.join([w.capitalize() for w in label.split('_')])


def plot_timeseries(ts, plot_dir, label):
    """Create time series plot.
    :param ts: time series
    :param plot_dir: plot dir
    :param label:
    :return:
    """
    plt.figure(figsize=(8, 6))
    ax = ts.plot()
    ax.set_xlabel(None)
    ax.grid(True)
    ax.set_ylabel(format_label(label))
    plt.savefig(os.path.join(plot_dir, f'{label}.png'))
    plt.close()


def plot_turnover_with_idx(turnover_details, plot_dir, labels):
    """Create a time series plot for turnover data.

    :param turnover_details: Dictionary with keys 'total', 'buy', and 'sell' for turnover data
    :param plot_dir: Directory to save the plot
    :param labels: Labels for the plot and axes
    """
    plt.figure(figsize=(8, 6))
    for key, series in turnover_details.items():
        plt.plot(series.index[1:], series[1:], label=f'{key.capitalize()} Turnover')

    plt.title(f'Turnover Over Time')
    plt.xlabel('Date')
    plt.grid(True)
    plt.ylabel(format_label(labels))
    plt.legend()
    plt.savefig(os.path.join(plot_dir, f'{labels}.png'))
    plt.close()



def plot_pnl_with_idx(pnl_details, plot_dir, labels):
    """Create a time series plot for PnL data.

    :param pnl_details: Dictionary with keys 'cumulative', 'long', and 'short' for PnL data
    :param plot_dir: Directory to save the plot
    :param labels: Labels for the plot and axes
    """
    plt.figure(figsize=(8, 6))
    for key, series in pnl_details.items():
        plt.plot(series.index, series, label=f'{key.capitalize()} PnL')

    plt.title(f'PnL Over Time')
    plt.xlabel('Date')
    plt.grid(True)
    plt.ylabel(format_label(labels))
    plt.legend()
    plt.savefig(os.path.join(plot_dir, f'{labels}.png'))


def plot_density(series, plot_dir, label):
    """Create density plot.
    :param series: Series
    :param plot_dir: plot dir
    :param label:
    :return:
    """
    plt.figure(figsize=(8, 6))
    plt.hist(series, bins=20, density=True, alpha=0.5, color='blue')
    series.plot(kind='kde', color='red')
    plt.xlabel(format_label(label))
    plt.ylabel('Density')
    plt.savefig(os.path.join(plot_dir, f'{label}.png'))
    plt.close()


def plot_time_series_cumpnl_idx(idx_names, start, end):
    """
    Plot and save the cumulative percentage change for specified indices within a given date range.

    :param idx_names: List of index names as strings (e.g., ['hs_300', 'zz_500', 'zz_800', 'zz_1000']).
    :param start: The start date as a string in 'YYYY-MM-DD' format.
    :param end: The end date as a string in 'YYYY-MM-DD' format.
    """
    # Load the data
    dl = DataLoader()
    df = dl.load('idx.pkl')
    df.set_index('date', inplace=True)
    df['rtn'] = df.groupby('code')['chg_pct'].shift(-1)
    df['cumulative_rtn'] = df['rtn'].cumsum()

    # Plot directory
    plot_dir = "D:/Quant_qishi/plot"
    # Ensure the plot directory exists
    if not os.path.exists(plot_dir):
        os.makedirs(plot_dir)

    # Mapping from index names to their respective query codes
    index_codes = {
        'hs_300': 'code == 300',
        'zz_500': 'code == 905',
        'zz_800': 'code == 906',
        'zz_1000': 'code == 852',
        'zz_9999': 'code == 1',
    }

    for idx_name in idx_names:
        if idx_name in index_codes:
            # Querying the data for the given index code
            data = df.query(index_codes[idx_name])

            # Filter data within the specified date range
            data = data[(data.index >= start) & (data.index <= end)]

            # Check if data is empty
            if data.empty:
                print(f"No data available for {idx_name} in the specified date range.")
                continue  # Skip to the next index if no data

            # Plotting
            plt.figure(figsize=(8, 6))
            plt.plot(data.index, data['cumulative_rtn'], linestyle='-')
            plt.title(f'Cumulative % Change for {idx_name.replace("_", " ").title()}')
            plt.xlabel('Date')
            plt.ylabel('Cumulative % Change')
            plt.grid(True)

            # Saving the plot to the specified directory
            plot_filename = os.path.join(plot_dir, f'{idx_name}.png')
            plt.savefig(plot_filename)

            print(f'Plot saved: {plot_filename}')
        else:
            print(f"Invalid index name provided: {idx_name}")



