import os.path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from data_etl.loader import DataLoader
from data_etl.saver import DataSaver


def alpha_test(alpha_keys):
    alpha_dir = 'D:/Quant_qishi/pkl/alpha'
    ds = DataSaver()  # Initialize DataSaver once for efficiency

    for alpha_key in alpha_keys:
        file_path = os.path.join(alpha_dir, alpha_key + '.pkl')

        try:
            df = pd.read_pickle(file_path)
            ds.save('alpha.pkl', df, alpha_key)  # Saving data for each alpha key
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"An error occurred while processing {alpha_key}: {str(e)}")