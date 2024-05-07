# Alpha Quantitative Research on China A-share Market

## Overview
This repo is dedicated to the analysis of the China A-share market, utilizing minute-level data from 2018 to 2021, covering over 5000 stocks across major indices including HS300, ZZ500, ZZ1000, and ZZ9999.  The data covers minute-by-minute price and volume data (`1min-pv`) from the 9:30 AM to 3:30 PM trading window.
## Features

- **Data Loader**: Streamlines the process of loading financial data into memory, capable of handling large datasets efficiently. Supports fetching data for a specific date or a range of dates from specified tables and fields.
  - `loading(tab_name, start, end, fields)`: Loads data across a specified date range.
  - `loading(date, tab_name, window, fields)`: Fetches data for a specific day, including data from a configurable window of surrounding days.

- **Data Processor**: Ensures daily alignment and processes data as needed. Capable of handling both raw and change values and optimizes performance through data serialization. It extracts comprehensive insights beyond standard OHLC data from minute-level data.

- **Path Management**: Implements dynamic data path management to ensure flexible and simplified data handling. This system reduces complexity in the data infrastructure, making it easier to manage and scale.

- **Alpha Calculator**: Employs sophisticated strategies to calculate alpha signals. Designed for high performance with support for multiprocessing, utilizing data efficiently loaded and processed by the `data_loader`.
  - Example configuration: `cfg_dict={'alpha1_name': alpha1_algo, [(args_set1),(args_set2)]}`

- **Alpha Performance**: Provides robust tools for evaluating alpha performance and calculating PnL. This component not only measures overall performance but also generates detailed daily statistics and visualizations.
  - Example configuration: `cfg_list = ['alpha1_name-args_set1-univ']`





