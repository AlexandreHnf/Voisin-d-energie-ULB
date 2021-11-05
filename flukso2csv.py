"""
Script to fetch Fluksometer data using the tmpo protocol and export it as a CSV file.

Input
-----
This script requires to have a file named `sensors.csv` in the same directory.
This `sensors.csv` file contains the home name, sensor ID and sensor token for each row.

Example input:

> cat sensors.csv
home_name,sensor_id,token
Flukso HQ,fed676021dacaaf6a12a8dda7685be34,b371402dc767cc83e41bc294b63f9586

Output
------
File `output.csv` in the same directory as the module.
/!\ The previous file will be overwritten!
The file contains the power time series, each sensor a column.
"""

import argparse
import os
from datetime import timezone

import pandas as pd
from pandas import read_csv
import tmpo
from matplotlib import pyplot


SENSOR_FILE = "sensors2.csv"
OUTPUT_FILE = "output/output.csv"



def get_prog_dir():
    import __main__
    main_path = os.path.abspath(__main__.__file__)
    main_path = os.path.dirname(main_path) + os.sep
    return main_path


def read_sensor_info(path):
    path += SENSOR_FILE
    sensors = pd.read_csv(path, header=0, index_col=0)
    return sensors


def energy2power(energy_df):
    power_df = energy_df.diff() * 1000
    power_df.fillna(0, inplace=True)
    power_df = power_df.resample("1S").mean()
    return power_df


def showTimeSeries(power_df):
    series = read_csv('output3.csv', header=0, index_col=0, parse_dates=True, squeeze=True)
    print(series.head())


def createFluksoDataframe(session, sensors, since):
    data_dfs = [session.series(id, head=since) for id in sensors.sensor_id]
    energy_df = pd.concat(data_dfs, axis=1)
    del data_dfs
    print("nb index : ", len(energy_df.index))
    energy_df.index = pd.DatetimeIndex(energy_df.index, name="time")
    # energy_df.index = energy_df.index.astype('datetime64[ns]')
    energy_df.columns = sensors.index
    power_df = energy2power(energy_df)
    del energy_df

    if power_df.index.tzinfo is None or power_df.index.tzinfo.utcoffset(power_df.index) is None: # NAIVE
        power_df.index = power_df.index.tz_localize("CET").tz_convert("CET")
    else:
        power_df.index = power_df.index.tz_convert("CET")

    power_df.fillna(0, inplace=True)

    print("nb elements : ", len(power_df.index))
    print(power_df.head(30))
    # showTimeSeries(power_df)

    return power_df


def flukso2csv(path="", since=""):
    if not path:
        path = get_prog_dir()

    if not since:
        since = 0
    else:
        since = pd.Timestamp.now(tz="UTC") - pd.Timedelta(since)
        print("Since : ", since, type(since))

    sensors = read_sensor_info(path)
    session = tmpo.Session(path)
    for sid, tk in sensors.values:
        session.add(sid, tk)

    session.sync()

    power_df = createFluksoDataframe(session, sensors, since)

    power_df.to_csv(path + OUTPUT_FILE)


if __name__ == "__main__":
    argparser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    argparser.add_argument("--since", type=str, default="",
            help="Period to query until now, e.g. '30days', '1hours', '20min' etc. Defaults to all data.")
    args = argparser.parse_args()
    flukso2csv(since=args.since)

