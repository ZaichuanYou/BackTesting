import numpy as np
import pandas as pd
import os
import sys


def Cal_vol(data, window):
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
        df_temp = df_temp[df_temp['time'].dt.time >= pd.to_datetime('09:46').time()]

        # Sort the dataframe by trading volume
        df_temp = df_temp.sort_values(by='volume', ascending=False)

        # Get the 10 minutes with the highest trading volume of the day
        M = df_temp.head(10).sort_values(by='time')

        # Identify "advantageous moments", which are at least 5 minutes apart
        A = M.copy()
        for i in range(len(M)-1, 0, -1):
            if (M.iloc[i]['time'] - M.iloc[i-1]['time']).total_seconds() < 5*60:
                A = A.drop(M.index[i])

        # Define "follow-up moments" as the 5 minutes following each moment in A
        F = {}
        for idx, row in A.iterrows():
            time_end = row['time'] + pd.Timedelta(minutes=5)
            F[idx] = df_temp[(df_temp['time'] > row['time']) & (df_temp['time'] <= time_end)]

        # Calculate "follow-up ratio" for each moment in A
        R = {}
        for idx, follow_up_df in F.items():
            R[idx] = follow_up_df['volume'].sum() / A.loc[idx, 'volume']

        # Calculate the factor as the average of the "follow-up ratios"
        Factor = sum(R.values()) / len(R)

        #print("massive moments: ", M)
        #print("adventageous moments: ", A)
        #print("follow up ratio: ", R)
        #print("Factor of the day: ", Factor)
        
        # Save each day's Factor at a new column 'factor' at first minute of each day
        df.loc[df['time']==df_temp.sort_values(by='time').head(1)['time'].iloc[0],'factor'] = Factor

        # If we have more than window day's Factor
        if df['factor'].dropna().size < window:
            continue
        
        # Calculte the mean of last window day's factor and save at new column 'index' at first minute of each day
        factor_mean = df.dropna(subset=['factor']).sort_values(by='time').tail(window)['factor'].mean()
        factor_std = df.dropna(subset=['factor']).sort_values(by='time').tail(window)['factor'].std()
        index = (factor_mean+factor_std)/2
        df.loc[(df['time'] >= s_date) & (df['time'] < e_date),'index'] = Factor

    # Save processed data to new folder
    df.to_csv('C:/Users/21995/Desktop/量化投资/CB_Data_Test/{}'.format(data))


if __name__ == '__main__':
    
    files = os.listdir('C:/Users/21995/Desktop/量化投资/CB_Data')
    
    for ind, file in enumerate(files):
        Cal_vol(file, window=1)

        print("\r", end="")
        print("Processing Data: {}%: ".format(int(ind+1)*100//len(files)), "▋" * (int((int(ind+1)/len(files)) * 100 // 2)), end="")
        sys.stdout.flush()