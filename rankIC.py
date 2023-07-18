import pandas as pd
import numpy as np
import os
import sys

if __name__=='__main__':
    data_dir = "C:/Users/21995/Desktop/量化投资/CB_Data_Test"
    files = os.listdir(data_dir)

    df = pd.DataFrame(columns=["name","return","index"])
    for file in files:
        df_temp = pd.read_csv(data_dir+"/"+file, index_col=0)
        df