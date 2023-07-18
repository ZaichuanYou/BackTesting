import pandas as pd
import os
import numpy as np


data_dir = "C:/Users/21995/Desktop/量化投资/可转债数据/2022"
files = os.listdir(data_dir)
dirs = os.listdir("C:/Users/21995/Desktop/量化投资/可转债数据/2022")
# for dir in dirs:
#     if not os.listdir(f"C:/Users/21995/Desktop/量化投资/可转债数据/2022.sz/{dir}"):
#         os.rmdir(f"C:/Users/21995/Desktop/量化投资/可转债数据/2022.sz/{dir}")


# for file in files:
#     firstTime = True
#     for dir in dirs:
#         file_dir = f"C:/Users/21995/Desktop/量化投资/可转债数据/2022.sz/{dir}/{file}"
#         if not os.path.exists(file_dir):
#             continue
#         elif firstTime:
#             df = pd.read_csv(file_dir)
#             df = df.drop(columns=['IOPV', 'fp_Volume', 'fp_Amount', 'Market'])
#             firstTime = False
#             continue
#         else:
#             df_temp = pd.read_csv(file_dir)
#             df_temp = df_temp.drop(columns=['IOPV', 'fp_Volume', 'fp_Amount', 'Market'])
#         df = pd.concat([df, df_temp])
#     df.to_csv(f"C:/Users/21995/Desktop/量化投资/可转债数据/2022/{file[:-4]}.csv")


for dir in dirs:
    df = pd.read_csv(data_dir+f"/{dir}", index_col=None)
    df = df.drop(df.columns[0], axis=1)
    df["DateTime"] = pd.to_datetime(df["DateTime"], format="%Y-%m-%d %H:%M:%S")
    df = df.loc[(df["DateTime"].dt.time<=pd.to_datetime('15:00').time()) & (df["DateTime"].dt.time>=pd.to_datetime('9:30').time())]
    df = df.loc[(df["DateTime"].dt.time>=pd.to_datetime('13:00').time()) | (df["DateTime"].dt.time<=pd.to_datetime('11:30').time())]
    df = df.rename(columns={"DateTime":"time", "OpenPx": "open", "HighPx":"high", "LowPx":"low", "LastPx":"close", "Volume":"volume", "Amount":"amount"})
    df = df.drop(columns=["PreClosePx"])
    df.to_csv(f"C:/Users/21995/Desktop/量化投资/可转债数据/2022_modified/{dir}")