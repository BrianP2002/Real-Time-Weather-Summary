"""
File: plot.py
Partners: Lin Ha, Haoting Tan
"""

import pandas as pd
import matplotlib.pyplot as plt
import json
import os

def get_latest_year_data(month_data):
    if not month_data:
        return None, None
    latest_year = max(month_data.keys())
    return latest_year, month_data[latest_year]['avg']

def main():
    months_of_interest = ['January', 'February', 'March']
    month_averages = {}

    for partition in range(4):
        path = f"/files/partition-{partition}.json"
        if os.path.exists(path):
            with open(path, 'r') as file:
                data = json.load(file)
                for month in months_of_interest:
                    if month in data:
                        latest_year, latest_data = get_latest_year_data(data[month])
                        if latest_data:
                            month_averages[f"{month}-{latest_year}"] = latest_data

    month_series = pd.Series(month_averages)
    fig, ax = plt.subplots()
    month_series.plot.bar(ax=ax)
    ax.set_ylabel('Avg. Max Temperature')
    plt.tight_layout()
    plt.savefig("/files/month.svg")

if __name__ == "__main__":
    main()

