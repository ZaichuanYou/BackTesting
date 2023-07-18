import numpy as np
import pandas as pd
import os
import sys

follow_interval = 5
adv_moment_num = 4
session_length = 75
day_avg = 1
weighted_avg = True

def analyze(data, window, s_dir, d_dir):
    df = pd.read_csv(s_dir+'/{}'.format(data), index_col=0)
    df = df.dropna()

    df['time'] = pd.to_datetime(df['time'])

    dayList = list(pd.date_range(start='2022-01-01 01:00:00',end='2022-12-31 23:00:00', freq='D'))
    for day in dayList:

        s_date = day
        e_date = (day + pd.Timedelta(days=1))

        df_temp = df[(df['time'] >= s_date) & (df['time'] < e_date)]
        if len(df_temp) == 0:
            continue

        df_temp = df_temp[df_temp['time'].dt.time >= pd.to_datetime('09:46').time()]

        for i in range(0, int(225/session_length)):
            i_start = i*session_length
            i_end = (i+1)*session_length
            
            df_session = df_temp.iloc[i_start:i_end]
            # Sort the dataframe by trading volume
            df_session = df_session.sort_values(by='volume', ascending=False)

            # Get the 10 minutes with the highest trading volume of the day
            M = df_session.head(adv_moment_num).sort_values(by='time')

            # Identify "advantageous moments", which are at least 5 minutes apart
            A = M.copy()
            for i in range(len(M)-1, 0, -1):
                if (M.iloc[i]['time'] - M.iloc[i-1]['time']).total_seconds() < follow_interval*60:
                    A = A.drop(M.index[i])

            # Define "follow-up moments" as the 5 minutes following each moment in A
            F = {}
            for idx, row in A.iterrows():
                time_end = row['time'] + pd.Timedelta(minutes=follow_interval)
                F[idx] = df_session[(df_session['time'] > row['time']) & (df_session['time'] <= time_end)]

            # Calculate "follow-up ratio" for each moment in A
            R = {}
            for idx, follow_up_df in F.items():
                if not A.loc[idx, 'volume'] == 0:
                    R[idx] = follow_up_df['volume'].sum() / A.loc[idx, 'volume']
                else:
                    R[idx] = 0

            # Calculate the factor as the average of the "follow-up ratios"
            Factor = sum(R.values()) / len(R)

            df.loc[df['time']==df_session.sort_values(by='time').head(1)['time'].iloc[0],'factor'] = Factor

            # If we have more than window day's Factor
            if df['factor'].dropna().size < window:
                continue
            
            # Calculte the mean of last window day's factor and save at new column 'index' at first minute of each day
            if not weighted_avg:
                factor_mean = df.dropna(subset=['factor']).sort_values(by='time').tail(window)['factor'].mean()
                factor_std = df.dropna(subset=['factor']).sort_values(by='time').tail(window)['factor'].std()
                index = (factor_mean+factor_std)/2
            else:
                weight_df = pd.DataFrame(data={'weight':range(1, window+1)})
                weight_df = weight_df.sort_values(by='weight', ascending=False)
                factor_mean = df.dropna(subset=['factor']).sort_values(by='time').tail(window)['factor'].mean()
                factor_weighted_mean = (df.dropna(subset=['factor']).sort_values(by='time').tail(window)['factor']*weight_df['weight']).sum()/factor_mean
                factor_std = df.dropna(subset=['factor']).sort_values(by='time').tail(window)['factor'].std()
                index = (factor_weighted_mean+factor_std)/2
            df.loc[(df['time'] >= s_date) & (df['time'] < e_date),'index'] = index

    df.to_csv(d_dir+'/{}'.format(data)) 

    

if __name__ == '__main__':
    s_dir = 'C:/Users/21995/Desktop/量化投资/可转债数据/2022_modified'
    d_dir = 'C:/Users/21995/Desktop/量化投资/CB_Data_Test'
    files = os.listdir(s_dir)
    
    for ind, file in enumerate(files):
        analyze(file, window=int(225/session_length)*day_avg, s_dir=s_dir, d_dir=d_dir)
        print("\r", end="")
        print("Processing Data: {}%: ".format(int(ind+1)*100//len(files)), "▋" * (int((int(ind+1)/len(files)) * 100 // 2)), end="")
        sys.stdout.flush()