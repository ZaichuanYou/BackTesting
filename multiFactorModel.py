import pandas as pd
import numpy as np
import os
import time
import sys
from joblib import Parallel, delayed



def standardize(mean, std, i, data):
    data.iat[i, 9] = (data.iat[i, 9] - mean) / std
    return data

def standardize_index(dirs):
    for dir in dirs:
        files = os.listdir(dir)
        data = {file: pd.read_csv(os.path.join(dir, file), index_col=0, engine='pyarrow') for file in files}
        print("Finished loading data")

        for i in range(len(data[next(iter(data))])):
            tic = time.perf_counter()
            valid_data = {file: df.iat[i, 9] for file, df in data.items() if df.iat[i, 9] is not None and not np.isnan(df.iat[i, 9]) and df.iat[i, 9] != 0}
            if not len(valid_data) == 0:

                mean = np.mean(list(valid_data.values()))
                std = np.std(list(valid_data.values()))

                results = Parallel(n_jobs=-1)(delayed(standardize)(mean, std, i, data[file]) for file in valid_data.keys())
                for file, df in zip(valid_data.keys(), results):
                    data[file] = df


            toc = time.perf_counter()
            print("\r", end="")
            print(f"Processing Data: {i * 100 // len(data[next(iter(data))])}%, time taken last iteration: {toc - tic:0.4f}s")
        for file in files:
            data[file].to_csv(os.path.join(dir, file))

def combine_model(dirs, dest_dir):
    files = os.listdir(dirs[0])
    for ind, file in enumerate(files):
        tic = time.perf_counter()

        data = {dir: pd.read_csv(os.path.join(dir, file), index_col=0, engine='pyarrow')['index'] for dir in dirs}
        full_data = pd.read_csv(os.path.join(dirs[0], file), index_col=0, engine='pyarrow')
        df = pd.DataFrame(data)
        print(df.head())
        df['mean'] = df.mean(axis=1, skipna=False)
        full_data['index'] = df['mean']
        full_data.to_csv(os.path.join(dest_dir, file))

        toc = time.perf_counter()
        print("\r", end="")
        print(f"Processing Data: {ind * 100 // len(files)}%, time taken last iteration: {toc - tic:0.4f}s")

if __name__=='__main__':
    dirs = ['Data/CB_Data_Test', 'Data/CB_Data_Flux']
    # standardize_index(dirs=dirs)
    combine_model(dirs=dirs, dest_dir="Data/Test_Data")
