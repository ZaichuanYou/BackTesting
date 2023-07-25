import pandas as pd
import os
import numpy as np


data_dir = "C:/Users/21995/Desktop/量化投资/可转债数据/2022-2023"
dest_dir = "C:/Users/21995/Desktop/量化投资/可转债数据/2022-2023_modified"


# for data in data_2023:
#     df = pd.read_csv(dir_2023+"/"+data, index_col=None)
#     if os.path.isfile(dir_2022+"/"+data):
#         df_temp = pd.read_csv(dir_2022+"/"+data)
#         df = pd.concat([df_temp,df])
#     df.to_csv(dest_dir+"/"+data, index=False)
# data_dir = "C:/Users/21995/Desktop/量化投资/可转债数据/2023.sz/2023.07.14"
# files = os.listdir(data_dir)
# dirs = os.listdir("C:/Users/21995/Desktop/量化投资/可转债数据/2023.sz")
# # for dir in dirs:
# #     if not os.listdir(f"C:/Users/21995/Desktop/量化投资/可转债数据/2023.sz/{dir}"):
# #         os.rmdir(f"C:/Users/21995/Desktop/量化投资/可转债数据/2023.sz/{dir}")


# for file in files:
#     firstTime = True
#     for dir in dirs:
#         file_dir = f"C:/Users/21995/Desktop/量化投资/可转债数据/2023.sz/{dir}/{file}"
#         if not os.path.isfile(file_dir):
#             continue
#         if firstTime:
#             df = pd.read_csv(file_dir)
#             df = df.drop(columns=['IOPV', 'fp_Volume', 'fp_Amount', 'Market'])
#             firstTime = False
#             continue
#         else:
#             df_temp = pd.read_csv(file_dir)
#             df_temp = df_temp.drop(columns=['IOPV', 'fp_Volume', 'fp_Amount', 'Market'])
#         df = pd.concat([df, df_temp])
#     df.to_csv(f"C:/Users/21995/Desktop/量化投资/可转债数据/2023/{file[:-4]}.csv")
columns = ['open', 'high', 'low', 'close']

finished = os.listdir("C:/Users/21995/Desktop/量化投资/可转债数据/2022-2023_modified")
# time_list = pd.Series(pd.date_range("2000-01-01 09:30:00", periods=400, freq="T").time())
# day_list = list(pd.date_range(start='2022-01-01 01:00:00',end='2022-12-31 23:00:00', freq='D'))
# time_list = time_list.loc[(time_list["DateTime"].dt.time<=pd.to_datetime('15:00').time()) & (time_list["DateTime"].dt.time>=pd.to_datetime('9:30').time())]
# time_list = time_list.loc[(time_list["DateTime"].dt.time>pd.to_datetime('13:00').time()) | (time_list["DateTime"].dt.time<=pd.to_datetime('11:30').time())]
# print(time_list)

dirs = os.listdir(data_dir)
for dir in dirs:
    df = pd.read_csv(data_dir+f"/{dir}", index_col=None)
    df["DateTime"] = pd.to_datetime(df["DateTime"], format="%Y-%m-%d %H:%M:%S")
    df = df.loc[(df["DateTime"].dt.time<=pd.to_datetime('15:00').time()) & (df["DateTime"].dt.time>=pd.to_datetime('9:30').time())]
    df = df.loc[(df["DateTime"].dt.time>pd.to_datetime('13:00').time()) | (df["DateTime"].dt.time<=pd.to_datetime('11:30').time())]
    df = df.rename(columns={"DateTime":"time", "OpenPx": "open", "HighPx":"high", "LowPx":"low", "LastPx":"close", "Volume":"volume", "Amount":"amount"})
    df = df.drop(df.columns[0], axis=1)
    # for day in day_list:

    #     s_date = day
    #     e_date = (day + pd.Timedelta(days=1))

    #     df_temp = df[(df['time'] >= s_date) & (df['time'] < e_date)]

    #     if not len(df_temp)%241 == 0:
    #         print(f"{dir} At day {day.dt} has missing data")
    #         missing = time_list.difference(df_temp['time'])
    #         print(missing)
            
    df = df.drop(columns=["PreClosePx"])
    df = df.reset_index(drop=True)
    index = df.loc[df['open']==0].index
    if index.size>0:
        if index.size>1200:
            print(f"Too many data was deprecated, this bond will be droped: {dir}")
            continue
        # print("Found following rows:")
        # print(dir, index)
        df[columns] = df[columns].replace(to_replace=0, method='ffill')
        df[columns] = df[columns].replace(to_replace=0, method='bfill')
    df.to_csv(dest_dir+"/"+dir)