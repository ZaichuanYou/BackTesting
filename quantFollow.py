import numpy as np
import pandas as pd
import os
import sys
import time

follow_interval = 5
adv_moment_num = 10
session_length = 225
day_avg = 20
weighted_avg = False



def analyze_index(data, window, s_dir, d_dir, cols, weight_df, weight_mean):

    df = pd.read_csv(s_dir+'/{}'.format(data), index_col=0, engine='pyarrow')
    df_result = pd.DataFrame(columns=cols)
    df['time'] = pd.to_datetime(df['time'])

    df = df.dropna()

    i = 0
    while i < len(df):
        
        df_temp = df.iloc[i+16:i+241]

        for n in range(0, int(225/session_length)):
            i_start = n*session_length
            i_end = (n+1)*session_length
            
            df_session = df_temp.iloc[i_start:i_end]

            # Sort the dataframe by trading volume
            df_session = df_session.sort_values(by='volume', ascending=False)

            # Get the 10 minutes with the highest trading volume of the day
            # M = df_session.loc[df_session['close']-df_session['open']>0].head(adv_moment_num)
            M = df_session.head(adv_moment_num).sort_values(by='time')

            if M['volume'].sum()==0 or M.size == 0:
                df_result = pd.concat([df_result, df.loc[[i+i_start]]], ignore_index=True)
                df_result.iat[-1, 8] = 0
                df_result.iat[-1, 9] = 0
                continue

            # Identify "advantageous moments", which are at least 5 minutes apart
            A = M.copy()
            for a in range(len(M)-1, 0, -1):
                if (M.iloc[a]['time'] - M.iloc[a-1]['time']).total_seconds() < follow_interval*60:
                    A = A.drop(M.index[a])

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
            df_result = pd.concat([df_result, df.loc[[i+16+i_start]]], ignore_index=True)
            df_result.iat[-1, 8] = Factor
        i=i+241

    
    if not weighted_avg:
        session = df_result['factor'].rolling(window=window)
        df_result['index'] = (session.apply(np.mean)+session.apply(np.std))/2
    else:
        for i in range(window,len(df_result)):
            session = df_result.iloc[i-window: i,8].values
            df_result.iat[i, 9] = ((np.sum(session*weight_df)/weight_mean)+np.std(session))/2

    df_result.to_csv(d_dir+'/{}'.format(data)) 
    

if __name__ == '__main__':
    s_dir = 'C:/Users/21995/Desktop/量化投资/可转债数据/full_data'
    d_dir = 'C:/Users/21995/Desktop/量化投资/CB_Data_Test'
    cols = ['SecurityID', 'time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'factor', 'index']
    files = os.listdir(s_dir)
    finishd = os.listdir(d_dir)

    window=int(225/session_length)*day_avg
    weight_df = pd.DataFrame(data={'weight':range(1, window+1)})
    weight_df = weight_df.sort_values(by='weight', ascending=True).values.T
    weight_mean = weight_df.sum()
    
    for ind, file in enumerate(files):
        # if file in finishd:
        #     continue
        tic = time.perf_counter()
        analyze_index(file, window=window, s_dir=s_dir, d_dir=d_dir, cols=cols, weight_df=weight_df, weight_mean=weight_mean)
        toc = time.perf_counter()
        print("\r", end="")
        print(f"Processing Data: {int(ind+1)*100//len(files)}%, time taken last file: {toc - tic:0.4f}s, last file: {file}")
        sys.stdout.flush()