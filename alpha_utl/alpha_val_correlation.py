import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from data_etl.loader import DataLoader
from alpha_cal.alpha_calc_prepare import prepare_alpha_data
from alpha_cal.alpha_calc import filter_for_univ
from alpha_utl.alpha_to_position import calculate_position
from alpha_utl.alpha_helper import*
from alpha_utl.alpha_value_dist_plot import plot_alpha_distribution_on_random_day
import seaborn as sns

def plot_alpha_correlations(alpha_functions, num_days=4):
    # Prepare data
    start, end = '2018-1-1', '2020-12-31'
    fields = ['date', 'code', 'pre_close', 'volume', 'close', 'open', 'low', 'high', 'chg_pct']
    data = prepare_alpha_data(start, end, fields)

    # Calculate alpha values using the provided alpha functions
    for i, alpha_func in enumerate(alpha_functions, start=1):
        data[f'alpha_{i:03}'] = alpha_func(data)

    # Calculate daily correlations
    daily_correlation_matrices = data.groupby('date').apply(
        lambda x: x[[f'alpha_{i:03}' for i in range(1, len(alpha_functions) + 1)]].corr()
    )

    # Select random days
    random_dates = np.random.choice(data['date'].unique(), size=num_days, replace=False)

    # Plotting
    fig, axes = plt.subplots(nrows=1, ncols=num_days, figsize=(5 * num_days, 4), sharey=True)
    fig.suptitle('Correlation Matrices for Random Days')

    for idx, date in enumerate(random_dates):
        # Convert numpy.datetime64 to datetime.datetime
        formatted_date = pd.to_datetime(date).strftime('%Y-%m-%d')

        # Extract the correlation matrix for the selected date
        corr_matrix = daily_correlation_matrices.loc[date]
        sns.heatmap(corr_matrix, ax=axes[idx], annot=True, vmin=-1, vmax=1, cmap='coolwarm')
        axes[idx].set_title(f'Date: {formatted_date}')

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.show()


# Example usage:
plot_alpha_correlations(
    alpha_functions=[alpha_custom_003_cor, alpha_custom_004_cor, alpha_custom_005_cor],
    num_days=4
)