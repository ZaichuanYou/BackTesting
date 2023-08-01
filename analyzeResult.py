
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import os

def plot_result(data_path):
    """
        This function will plot the ten-group result of the index and market value

        params:
            data_path: the path to data
    """
    data = pd.read_csv(os.path.join(data_path, "Result.csv"))
    # Setting the color palette
    reds = sns.color_palette("Reds", 10)
    reds.reverse() # Reverse the color palette so the darkest red corresponds to 'top 0 to 10% Return'
    colors = ['blue'] + reds

    # Creating the plot
    plt.figure(figsize=(10, 6))

    # We reverse the order of the columns so that 'Market trend' is plotted first and 'top 90 to 100% Return' last.
    reversed_columns = list(reversed(data.columns))

    for i, column in enumerate(reversed_columns):
        plt.plot(data[column], color=colors[len(data.columns)-1-i], label=column)

    # Reversing the legend to match the request of data order
    handles, labels = plt.gca().get_legend_handles_labels()
    handles.reverse()
    labels.reverse()

    plt.legend(handles, labels)
    plt.xlabel('Time')
    plt.ylabel('Value')
    plt.title('Market trend and Return Percentages Over Time')
    plt.grid(True)
    plt.savefig(os.path.join(data_path, 'Market trend and Return Percentages Over Time.png'))
    plt.show()


def show_end(data_path):
    """
        This function will plot the ending value of ten-group result of the index and using market value as a basis line

        params:
            data_path: the path to data
    """
    data = pd.read_csv(os.path.join(data_path, "Result.csv"))

    # Extracting the ending values of each column
    ending_values = data.iloc[-1]

    # Setting the color palette
    reds = sns.color_palette("Reds", 10)
    reds.reverse() # Reverse the color palette so the darkest red corresponds to 'top 0 to 10% Return'
    colors = ['blue'] + reds

    # Creating the plot
    plt.figure(figsize=(12, 8))

    # Plotting the horizontal basis line (Market trend)
    plt.axhline(y=ending_values['Market trend'], color='blue', linestyle='--')

    # Plotting the ending values of each column (excluding Market trend)
    for i, column in enumerate(ending_values.index[1:]):  # start from 1 to exclude 'Market trend'
        plt.scatter(i, ending_values[column], color=colors[i+1], label=column, marker='o', s=100)  # larger point size

    plt.xticks(np.arange(len(ending_values.index[1:])), labels=ending_values.index[1:], rotation=45)  # make x-axis readable
    plt.xlabel('Category')
    plt.ylabel('Ending Value')
    plt.title('Ending Value of Each Category')
    plt.grid(True)

    # Fitting a regression line
    x_values = np.arange(len(ending_values.index[1:]))
    y_values = ending_values[1:]
    coefficients = np.polyfit(x_values, y_values, 1)
    poly = np.poly1d(coefficients)
    plt.plot(x_values, poly(x_values), 'k--', label="Regression Line")

    plt.legend()
    plt.tight_layout()  # adjust the layout to make everything fit
    plt.savefig(os.path.join(data_path, 'Ending Value of Each Category.png'))
    plt.show()



if __name__ == "__main__":
    plot_result("Results/ResultFollow_Release")
    show_end("Results/ResultFollow_Release")