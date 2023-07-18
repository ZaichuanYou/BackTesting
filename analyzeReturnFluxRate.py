import numpy as np
import pandas as pd
import os
import sys


def cal_std(data):
    df = pd.read_csv('C:/Users/21995/Desktop/量化投资/CB_Data/{}'.format(data), index_col=0)
    df = df.dropna()

    # Convert the 'time' column to datetime type
    df['time'] = pd.to_datetime(df['time'])

    dayList = list(pd.date_range(start='2023-06-05 01:00:00',end='2023-07-10 23:00:00', freq='D'))
    
    for day in dayList:

        s_date = day
        e_date = (day + pd.Timedelta(days=1))

        df_temp = df[(df['time'] >= s_date) & (df['time'] < e_date)]
        if len(df_temp) == 0:
            continue
        
        
        # Exclude data before 9:45 am and only consider data from the 16th minute after the market opens
        df_temp = df_temp[df_temp['time'].dt.time < pd.to_datetime('14:56').time()]

        vol_mean = df_temp['volume'].mean()
        vol_std = df_temp['volume'].std()

        # Identify "Volume releasing moments", which has volume 
        M = df_temp.loc[df_temp['volume']>=vol_mean+vol_std]

        # Identify "advantageous moments", which are at least 5 minutes apart
        A = M.copy()
        A['return'] = A['close']-A['open']
        A = A.loc[A['return']>0]
        if len(A)==0:
            return
        ret_std = A['return'].std()
        
        df.loc[(df['time'] >= s_date) & (df['time'] < e_date),'index'] = ret_std

    # Save processed data to new folder
    df.to_csv('C:/Users/21995/Desktop/量化投资/CB_Data_FluxRate/{}'.format(data))



def cal_std(data):
    df = pd.read_csv('C:/Users/21995/Desktop/量化投资/可转债数据/2022_modified/{}'.format(data), index_col=0)
    df = df.dropna()

    # Convert the 'time' column to datetime type
    df['time'] = pd.to_datetime(df['time'])

    dayList = list(pd.date_range(start='2022-01-01 01:00:00',end='2022-12-31 23:00:00', freq='D'))
    
    for day in dayList:

        s_date = day
        e_date = (day + pd.Timedelta(days=1))

        df_temp = df[(df['time'] >= s_date) & (df['time'] < e_date)]
        if len(df_temp) == 0:
            continue
        
        
        # Exclude data before 9:45 am and only consider data from the 16th minute after the market opens
        df_temp = df_temp[df_temp['time'].dt.time < pd.to_datetime('14:56').time()]

        vol_mean = df_temp['volume'].mean()
        vol_std = df_temp['volume'].std()

        # Identify "Volume releasing moments", which has volume 
        M = df_temp.loc[df_temp['volume']>=vol_mean+vol_std]

        # Identify "advantageous moments", which are at least 5 minutes apart
        A = M.copy()
        A['return'] = A['close']-A['open']
        A = A.loc[A['return']>0]
        if len(A)==0:
            return
        ret_std = A['return'].std()
        
        df.loc[(df['time'] >= s_date) & (df['time'] < e_date),'index'] = ret_std

    # Save processed data to new folder
    df.to_csv('C:/Users/21995/Desktop/量化投资/CB_Data_RevMomentum/{}'.format(data))



if __name__ == '__main__':
    
    files = os.listdir('C:/Users/21995/Desktop/量化投资/可转债数据/2022_modified')
    
    for ind, file in enumerate(files):
        cal_std(file)

        print("\r", end="")
        print("Processing Data: {}%: ".format(int(ind+1)*100//len(files)), "▋" * (int((int(ind+1)/len(files)) * 100 // 2)), end="")
        sys.stdout.flush()