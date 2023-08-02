import numpy as np
import pandas as pd
import os
import sys
import time
from cringe import extend_data

follow_interval = 5
adv_moment_num = 15
session_length = 225
day_avg = 20
weighted_avg = False



def analyze_index(data, window, s_dir, d_dir, cols, weight_df, weight_mean):
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
    df = pd.read_csv(s_dir+'/{}'.format(data), index_col=0)
    df_result = pd.DataFrame(columns=cols)
    df['time'] = pd.to_datetime(df['time'])

    df = df.dropna()

    i = 0
    while i < len(df):
        
        df_temp = df.iloc[i+16:i+241]

        for n in range(0, int(225/session_length)):
            i_start = n*session_length
            i_end = (n+1)*session_length
            
            df_session = df_temp.iloc[i_start:i_end-follow_interval]

            # Sort the dataframe by trading volume
            df_session = df_session.sort_values(by='volume', ascending=False)

            # Get the 10 minutes with the highest trading volume of the day
            M = df_session.head(adv_moment_num).sort_values(by='time')
            df_session = df_session.append(df_temp.iloc[i_end-follow_interval:])

            if M['volume'].sum()==0 or M.size == 0:
                df_result = pd.concat([df_result, df.loc[[i+i_start]]], ignore_index=True)
                df_result.iat[-1, 8] = 0
                df_result.iat[-1, 9] = 0
                continue

            # Identify "advantageous moments", which are at least 5 minutes apart
            A = M.copy()
            A = A.drop(A.index)
            for a in range(0, len(M)):
                if len(A) == 0:
                    A = pd.concat([A, M.iloc[[a]]], ignore_index=True)
                elif (M.iloc[a]['time'] - A.iloc[-1]['time']).total_seconds() > follow_interval*60:
                    A = pd.concat([A, M.iloc[[a]]], ignore_index=True)
                    
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


def analyze_index_release(data, window, s_dir, d_dir, cols, weight_df, weight_mean):
    """
        This will calculate the Amount follow index and store the result at a new column.\n
        Added new criteria which only consider time slice that has a positive return.\n
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

    # Create a new 'return' column
    df['return'] = df['close'].diff()

    df = df.dropna()

    i = 0
    while i < len(df):
        
        df_temp = df.iloc[i+16:i+241]

        for n in range(0, int(225/session_length)):
            i_start = n*session_length
            i_end = (n+1)*session_length
            
            df_session = df_temp.iloc[i_start:i_end-follow_interval]

            # Sort the dataframe by trading volume and return
            df_session = df_session[df_session['return'] > 0].sort_values(by=['volume'], ascending=False)

            if len(df_session)==0:
                print(i+i_start)
                df_result = pd.concat([df_result, df.loc[[i+16+i_start]]], ignore_index=True)
                df_result.iat[-1, 8] = 0
                df_result.iat[-1, 9] = 0
                continue

            # Get the 10 minutes with the highest trading volume and positive return of the day
            M = df_session.head(adv_moment_num).sort_values(by='time')
            df_session = df_session.append(df_temp.iloc[i_end-follow_interval:])

            if M['volume'].sum()==0 or M.size == 0:
                df_result = pd.concat([df_result, df.loc[[i+16+i_start]]], ignore_index=True)
                df_result.iat[-1, 8] = 0
                df_result.iat[-1, 9] = 0
                continue

            # Initialize an empty list to store selected rows
            selected_rows = []

            # Iterate over the rows of M
            for a in range(0, len(M)):
                # If this is the first row, or the time difference with the last selected row is greater than follow_interval*60
                if not selected_rows or (M.iloc[a]['time'] - selected_rows[-1]['time']).total_seconds() > follow_interval*60:
                    # Append the row to the list
                    selected_rows.append(M.iloc[a])

            # Create a DataFrame from the list of selected rows
            A = pd.DataFrame(selected_rows).reset_index(drop=True)

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



def analyze_index_optimimzed(data, window, s_dir, d_dir, cols, weight_df, weight_mean):
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
    df = pd.read_csv(s_dir+'/{}'.format(data), index_col=0)
    df_result = pd.DataFrame(columns=cols)
    df['time'] = pd.to_datetime(df['time'])

    df = df.dropna()

    i = 0
    while i < len(df):
        
        df_temp = df.iloc[i+16:i+241]

        for n in range(0, int(225/session_length)):
            i_start = n*session_length
            i_end = (n+1)*session_length
            
            df_session = df_temp.iloc[i_start:i_end-follow_interval]

            # Sort the dataframe by trading volume
            df_session = df_session.sort_values(by='volume', ascending=False)

            # Get the 10 minutes with the highest trading volume of the day
            M = df_session.head(adv_moment_num).sort_values(by='time')
            df_session = df_session.append(df_temp.iloc[i_end-follow_interval:])

            if M['volume'].sum()==0 or M.size == 0:
                df_result = pd.concat([df_result, df.loc[[i+i_start]]], ignore_index=True)
                df_result.iat[-1, 8] = 0
                df_result.iat[-1, 9] = 0
                continue

            # Convert the time column to datetime format (if it's not already)
            M['time'] = pd.to_datetime(M['time'])

            # Initialize an empty list to store selected rows
            selected_rows = []

            # Iterate over the rows of M
            for a in range(0, len(M)):
                # If this is the first row, or the time difference with the last selected row is greater than follow_interval*60
                if not selected_rows or (M.iloc[a]['time'] - selected_rows[-1]['time']).total_seconds() > follow_interval*60:
                    # Append the row to the list
                    selected_rows.append(M.iloc[a])

            # Create a DataFrame from the list of selected rows
            A = pd.DataFrame(selected_rows).reset_index(drop=True)

                    
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
    d_dir = 'C:/Users/21995/Desktop/量化投资/CB_Data_F15'
    cols = ['SecurityID', 'time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'factor', 'index']
    files = os.listdir(s_dir)
    if not os.path.isdir(d_dir):
        os.makedirs(d_dir)
    finishd = os.listdir(d_dir)

    window=int(225/session_length)*day_avg
    weight_df = pd.DataFrame(data={'weight':range(1, window+1)})
    weight_df = weight_df.sort_values(by='weight', ascending=True).values.T
    weight_mean = weight_df.sum()

    
    
    for ind, file in enumerate(files):
        # if file in finishd:
        #     continue
        tic = time.perf_counter()
        analyze_index_optimimzed(file, window=window, s_dir=s_dir, d_dir=d_dir, cols=cols, weight_df=weight_df, weight_mean=weight_mean)
        toc = time.perf_counter()
        print("\r", end="")
        print(f"Processing Data: {int(ind+1)*100//len(files)}%, time taken last file: {toc - tic:0.4f}s, last file: {file}")
        sys.stdout.flush()
    
    extend_data(d_dir, d_dir)