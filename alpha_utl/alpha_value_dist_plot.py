import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from data_etl.loader import DataLoader
from alpha_cal.alpha_calc_prepare import prepare_alpha_data
from alpha_cal.alpha_calc import filter_for_univ
from alpha_utl.alpha_to_position import calculate_position
from alpha_utl.alpha_helper import*

def plot_alpha_distribution_on_random_day(alpha_function):
    # Define start and end dates
    start, end = '2018-1-1', '2020-12-31'

    # Prepare data
    fields = ['date', 'code', 'pre_close', 'volume', 'close', 'open', 'low', 'high', 'chg_pct']
    data = prepare_alpha_data(start, end, fields)

    # Calculate alpha values using the provided alpha function
    df = alpha_function(data)

    # Choose a random date from the DataFrame
    random_date = np.random.choice(df['date'].unique())
    print(f"Selected date for plotting: {random_date}")

    # Filter data for the selected date
    day_data = df[df['date'] == random_date]

    # Plotting the histogram of the alpha values
    plt.figure(figsize=(10, 6))
    plt.hist(day_data['alpha_val'], bins=20, alpha=0.75, color='blue', edgecolor='black')
    plt.title(f'Histogram of Alpha Values on {random_date}')
    plt.xlabel('Alpha Value')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.show()