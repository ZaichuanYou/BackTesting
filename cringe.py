import pandas as pd
import numpy as np
import os
import sys
import warnings
warnings.filterwarnings("ignore")

def fill_missing_data(data_dir):
    """
        This function will detect the missing minutes data in each day's data and fill it with nearest minute data.
        All change will be in-place.

        params:
            data_dir: the directry of the source data
    """
    # List all files in the given source directory.
    datas = os.listdir(data_dir)

    # Loop through each file in the source directory.
    for data in datas:
        # Read each CSV file in the directory list as a DataFrame. The first column in the CSV file is set as the index of the DataFrame.
        df_temp = pd.read_csv(os.path.join(data_dir, data), index_col=0)

        # Store column names of the DataFrame for later use.
        cols = df_temp.columns

        # Convert the 'time' column of the DataFrame to datetime format and set it as the index.
        df_temp['time'] = pd.to_datetime(df_temp['time'])
        df_temp.set_index('time', inplace=True)

        # Get the unique dates in the DataFrame.
        dates = pd.Series(df_temp.index.date).unique()

        # If the number of rows divided by the number of unique dates equals 241, skip the current file.
        if len(df_temp)/len(dates) == 241:
            continue
        else:
            # Print a message indicating that the current file has missing time frames.
            print(f"Data {data} has missing time frame")

        # Initialize an empty DataFrame to store the new data.
        new_data = pd.DataFrame()

        # Loop through each unique date.
        for date in dates:
            # Define the start and end times for the morning and afternoon sessions.
            morning_start = pd.Timestamp(date.year, date.month, date.day, 9, 30)
            morning_end = pd.Timestamp(date.year, date.month, date.day, 11, 30)
            afternoon_start = pd.Timestamp(date.year, date.month, date.day, 13, 1)
            afternoon_end = pd.Timestamp(date.year, date.month, date.day, 15, 0)

            # Create time ranges for the morning and afternoon sessions.
            morning_range = pd.date_range(start=morning_start, end=morning_end, freq='T')
            afternoon_range = pd.date_range(start=afternoon_start, end=afternoon_end, freq='T')

            # Append the morning and afternoon time ranges to create a full day time range.
            day_range = morning_range.append(afternoon_range)

            # Get the data for the current day.
            current_day_data = df_temp.loc[df_temp.index.date == date]

            # Fill in missing time frames in the current day's data by reindexing it with the full day time range. 
            # The 'nearest' method is used to fill missing values.
            current_day_data = current_day_data.reindex(day_range, method='nearest')

            # Append the current day's data to the new data DataFrame.
            new_data = new_data.append(current_day_data)

        # Reset the index of the new data DataFrame to make 'time' a column again.
        new_data.reset_index(inplace=True)

        # Rename the 'index' column to 'time'.
        new_data.rename(columns={'index': 'time'}, inplace=True)

        # Set a range of integers as the index of the new data DataFrame.
        new_data.index = range(len(new_data))

        # Reorder the columns of the new data DataFrame based on the original column order.
        new_data = new_data[cols]

        # Save the new data DataFrame as a CSV file in the source directory. The name of the CSV file is the same as the name of the original CSV file.
        new_data.to_csv(os.path.join(data_dir, data))


def extend_data(data_dir, dest_dir):
    """
        This function will extend every data to the length of the first data read

        params:
            data_dir: the source directory of data
            dest_dir: the destination directory of data
    """
    # List all directories in the given source directory.
    dirs = os.listdir(data_dir)

    # Join the source directory path with the first directory in the list to form the path of the sample data.
    sample_data = os.path.join(data_dir, dirs[0])

    # Read the first CSV file from the list of directories as a pandas DataFrame. The first column in the CSV file is set as the index of the DataFrame.
    df = pd.read_csv(sample_data, index_col=0)

    # Convert the 'time' column of the DataFrame to datetime format.
    dates = pd.to_datetime(df['time'])

    # Create a new DataFrame using the converted 'time' column.
    df_template = pd.DataFrame(dates, columns=['time'])

    # Loop through each directory in the source directory.
    for data in dirs:
        # Read each CSV file in the directory list as a DataFrame. The first column in the CSV file is set as the index of the DataFrame.
        df_temp = pd.read_csv(os.path.join(data_dir, data), index_col=0)
        
        # Convert the 'time' column of the temp DataFrame to datetime format.
        df_temp['time'] = pd.to_datetime(df_temp['time'])

        # Merge the template DataFrame with the temp DataFrame based on the 'time' column. If the 'time' values in the temp DataFrame do not exist in the template DataFrame, they will be added with NaN values in the other columns.
        df_result = pd.merge(df_template, df_temp, on='time', how='left')

        # Drop the 'index' and 'factor' columns from the temp DataFrame and get the remaining columns.
        cols = df_temp.drop(columns=['index', 'factor']).columns

        # Fill any NaN values in the result DataFrame with the next valid observation in the DataFrame.
        df_result[cols] = df_result[cols].bfill()

        # Save the result DataFrame as a CSV file in the destination directory. The name of the CSV file is the same as the name of the CSV file in the source directory.
        df_result.to_csv(os.path.join(dest_dir, data))