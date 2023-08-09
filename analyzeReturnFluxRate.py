import numpy as np
import pandas as pd
import os
import sys
import time
from math import sqrt
from cringe import extend_data


follow_interval = 5
adv_moment_num = 10
session_length = 59
day_avg = 5
weighted_avg = False

def analyze_index(data, s_dir, d_dir, cols):
    """
        This will calculate the Amount follow index and store the result at a new column.\n
        Using weighted average by setting weighted_avg to be true.

        params:
            data: directory of current data
            window: look back window of the index calculation
            s_dir: source directory of the data
            d_dir: destination directory of the processed data
            cols: columns that will be keeped in the processed data
            weight_df: weight of weighted average
            weight_mean: mean of weight
    """
    df = pd.read_csv(s_dir+'/{}'.format(data), index_col=0, engine='pyarrow')
    df_result = pd.DataFrame(columns=cols)
    df['time'] = pd.to_datetime(df['time'])

    df = df.dropna()

    index = []
    for i in range(0, day_avg):
        index.extend(list(range(i*241, (i+1)*241-5)))
    index = np.array(index)

    i = 241*day_avg
    while i < len(df):
        
        df_session = df.iloc[index]
        vol_mean = np.mean(df_session['volume'])
        vol_std = np.std(df_session['volume'])
        df_release = df_session[df_session['volume']>vol_mean+vol_std].index
        ret = df.iloc[df_release]['close'].values-df.iloc[df_release-1]['close'].values
        ret_mean = np.mean(ret)
        Factor = sqrt(np.sum((ret-ret_mean)**2/len(df_release)))
        df_result = pd.concat([df_result, df.loc[[i-241]]], ignore_index=True)
        df_result.iat[-1, 8] = Factor
        df_result.iat[-1, 9] = Factor
        i=i+241
        index = index+241

    df_result.to_csv(d_dir+'/{}'.format(data)) 



def analyze_index_HighFreq(data, s_dir, d_dir, cols):
    """
        This will calculate the Amount follow index and store the result at a new column.\n
        This is a modified version which will devide data into high frequency sessions.

        params:
            data: directory of current data
            window: look back window of the index calculation
            s_dir: source directory of the data
            d_dir: destination directory of the processed data
            cols: columns that will be keeped in the processed data
            weight_df: weight of weighted average
            weight_mean: mean of weight
    """
    df = pd.read_csv(s_dir+'/{}'.format(data), index_col=0, engine='pyarrow')
    df_result = pd.DataFrame(columns=cols)

    df = df.dropna()

    index = np.array(list(range(0,59)))

    for i in range(0, len(df), 241):
        for n in range(0, int(236/session_length)):
            df_session = df.iloc[index+n*59]
            vol_mean = np.mean(df_session['volume'])
            vol_std = np.std(df_session['volume'])
            df_release = df_session[df_session['volume']>vol_mean+vol_std].index
            ret = df.iloc[df_release]['close'].values-df.iloc[df_release-1]['close'].values
            if len(ret)==0:
                df_result = pd.concat([df_result, df.loc[[index[0]+n*59]]], ignore_index=True)
                df_result.iat[-1, 8] = 0
                df_result.iat[-1, 9] = 0
            ret_mean = np.mean(ret)
            Factor = sqrt(np.sum((ret-ret_mean)**2/len(df_session)))
            df_result = pd.concat([df_result, df.loc[[index[0]+n*59]]], ignore_index=True)
            df_result.iat[-1, 8] = Factor
            df_result.iat[-1, 9] = Factor
        index = index+241

    df_result.to_csv(d_dir+'/{}'.format(data)) 
    


def analyze_Reverse_Imp(data, s_dir, d_dir, cols):
    """
        This will calculate the Amount follow index and store the result at a new column.\n
        Using weighted average by setting weighted_avg to be true.

        params:
            data: directory of current data
            window: look back window of the index calculation
            s_dir: source directory of the data
            d_dir: destination directory of the processed data
            cols: columns that will be keeped in the processed data
            weight_df: weight of weighted average
            weight_mean: mean of weight
    """
    df = pd.read_csv(s_dir+'/{}'.format(data), index_col=0, engine='pyarrow')
    df_result = pd.DataFrame(columns=cols)
    df['time'] = pd.to_datetime(df['time'])

    df = df.dropna()

    i = 0
    while i < len(df):
        
        df_temp = df.iloc[i:i+236]

        for n in range(0, int(236/session_length)):
            i_start = n*session_length
            i_end = (n+1)*session_length
            
            df_session = df_temp.iloc[i_start:i_end]
            df_session['return'] = df_session['close'].diff()
            
            df_session = df_session.dropna()

            vol_mean = np.mean(df_session['volume'])
            vol_std = np.std(df_session['volume'])
            df_release = df_session[(df_session['volume']>vol_mean+vol_std) & (df_session['return']>0)].index
            if len(df_release) == 0:
                df_result = pd.concat([df_result, df.loc[[i+i_start]]], ignore_index=True)
                df_result.iat[-1, 8] = 0
                df_result.iat[-1, 9] = 0
                continue
            ret = df.iloc[df_release]['close'].values-df.iloc[df_release-1]['close'].values
            Factor = np.mean(ret)
            df_result = pd.concat([df_result, df.loc[[i+i_start]]], ignore_index=True)
            df_result.iat[-1, 8] = Factor
            df_result.iat[-1, 9] = Factor
        i=i+241
    df_result.to_csv(d_dir+'/{}'.format(data))

if __name__ == '__main__':
    s_dir = 'C:/Users/21995/Desktop/量化投资/可转债数据/full_data'
    d_dir = 'C:/Users/21995/Desktop/量化投资/中金/Data/CB_Data_FluxHighFreq'
    cols = ['time', 'SecurityID', 'open', 'high', 'low', 'close', 'volume', 'amount', 'factor', 'index']
    files = os.listdir(s_dir)
    if not os.path.isdir(d_dir):
        os.makedirs(d_dir)
    finishd = os.listdir(d_dir)
    
    for ind, file in enumerate(files):
        # if file in finishd:
        #     continue
        tic = time.perf_counter()
        analyze_index_HighFreq(file, s_dir=s_dir, d_dir=d_dir, cols=cols)
        toc = time.perf_counter()
        print("\r", end="")
        print(f"Processing Data: {int(ind+1)*100//len(files)}%, time taken last file: {toc - tic:0.4f}s, last file: {file}")

        
    extend_data(d_dir, d_dir)