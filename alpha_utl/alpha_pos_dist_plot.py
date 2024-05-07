import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from data_etl.loader import DataLoader
from alpha_cal.alpha_calc_prepare import prepare_alpha_data
from alpha_cal.alpha_calc import filter_for_univ
from alpha_utl.alpha_to_position import calculate_position
from alpha_utl.alpha_helper import*
import seaborn as sns

def plot_pos_distribution_on_random_day(alpha_function, date = False):
    # Define start and end dates
    start, end = '2018-01-01', '2020-12-31'

    # Assume prepare_alpha_data function is defined elsewhere
    # Prepare data
    fields = ['date', 'code', 'pre_close', 'volume', 'close', 'open', 'low', 'high', 'chg_pct']
    data = prepare_alpha_data(start, end, fields)

    # Calculate alpha values using the provided alpha function
    pos = alpha_function(data)

    pos = pos[pos != 0]

    # Choose a random date from the DataFrame's index (pos is a pivot table with dates as index)
    if date:
        selected_date = pd.to_datetime(date)
    else:
        selected_date = np.random.choice(pos.index)
    print(f"Selected date for plotting: {selected_date}")

    # Filter data for the selected date from the pivot table
    day_positions = pos.loc[selected_date]

    positive_pos = day_positions[day_positions > 0]
    negative_pos = day_positions[day_positions < 0]
    zero_pos = day_positions[day_positions == 0]

    # Printing sums and counts
    print("Sum of positive positions:", positive_pos.sum())
    print("Sum of negative positions:", negative_pos.sum())
    print("Total sum of positions:", day_positions.sum())
    print("Count of positive positions:", len(positive_pos))
    print("Count of negative positions:", len(negative_pos))
    print("Count of zero positions:", len(zero_pos))
    print("Total count of positions:", len(day_positions))

    # Plotting the histogram of the alpha values
    plt.figure(figsize=(10, 6))
    sns.histplot(day_positions, bins=30, kde=True, color='red')
    plt.title(f'Histogram of Position Values on {selected_date}')
    plt.xlabel('Position Values')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.show()

