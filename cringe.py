import pandas as pd
import numpy as np
import os
import sys
import warnings
warnings.filterwarnings("ignore")


dirs = os.listdir("C:/Users/21995/Desktop/量化投资/CB_Data_Test")
sample_data = "C:/Users/21995/Desktop/量化投资/CB_Data_Test/110043.csv"
df = pd.read_csv(sample_data, index_col=0)
dates = pd.to_datetime(df['time'])
df_template = pd.DataFrame(dates, columns=['time'])

# Create an empty DataFrame to store the new data



for data in dirs:
    df_temp = pd.read_csv("C:/Users/21995/Desktop/量化投资/CB_Data_Test/"+data, index_col=0)
    df_temp['time'] = pd.to_datetime(df_temp['time'])
    df_result = pd.merge(df_template, df_temp, on='time', how='left')
    cols = df_temp.drop(columns=['index', 'factor']).columns
    df_result[cols] = df_result[cols].bfill()
    #print(df_result.head())
    df_result.to_csv("C:/Users/21995/Desktop/量化投资/Test/"+data)


# for data in dirs:
#     df_temp = pd.read_csv("C:/Users/21995/Desktop/量化投资/可转债数据/2022-2023_modified/"+data, index_col=0)
#     cols = df_temp.columns
#     df_temp['time'] = pd.to_datetime(df_temp['time'])
#     df_temp.set_index('time', inplace=True)
#     dates = pd.Series(df_temp.index.date).unique()
#     if len(df_temp)/len(dates) == 241:
#         continue
#     else:
#         print(f"Data {data} has missing time frame")
#     new_data = pd.DataFrame()

#     # Create a time range for each day based on the minutes from the first day
#     for date in dates:
#         morning_start = pd.Timestamp(date.year, date.month, date.day, 9, 30)
#         morning_end = pd.Timestamp(date.year, date.month, date.day, 11, 30)
#         morning_range = pd.date_range(start=morning_start, end=morning_end, freq='T')

#         afternoon_start = pd.Timestamp(date.year, date.month, date.day, 13, 1)
#         afternoon_end = pd.Timestamp(date.year, date.month, date.day, 15, 0)
#         afternoon_range = pd.date_range(start=afternoon_start, end=afternoon_end, freq='T')

#         day_range = morning_range.append(afternoon_range)


#         current_day_data = df_temp.loc[df_temp.index.date == date]
#         current_day_data = current_day_data.reindex(day_range, method='nearest')
#         new_data = new_data.append(current_day_data)
    
#     # Reset index to make 'time' a column again
#     new_data.reset_index(inplace=True)

#     # Rename the 'Unnamed: 0' column back to 'time'
#     new_data.rename(columns={'index': 'time'}, inplace=True)

#     # Set a range of integers as the index
#     new_data.index = range(len(new_data))
#     new_data = new_data[cols]

#     new_data.to_csv("C:/Users/21995/Desktop/量化投资/可转债数据/full_data/"+data)



