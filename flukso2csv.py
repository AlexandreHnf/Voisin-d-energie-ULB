"""
Authors :
    - Guillaume Levasseur
    - Alexandre Heneffe

Script to fetch Fluksometer data using the tmpo protocol and export it as a CSV file.

Input
-----
This script requires to have a file named `sensors.csv` in the "sensors/" directory.
This `sensors.csv` file contains the home name, sensor ID and sensor token for each row.

Example input:

> cat sensors.csv
home_ID,home_name,sensor_id,token,state
1,Flukso HQ,fed676021dacaaf6a12a8dda7685be34,b371402dc767cc83e41bc294b63f9586,+

Output
------
File `output.csv` in the same directory as the module.
/!\ The previous file will be overwritten!
The file contains the power time series, each sensor a column.
"""

import argparse
import os

import pandas as pd
from pandas import read_csv
import tmpo
import matplotlib.pyplot as plt


SENSOR_FILE = "sensors/sensors.csv"
OUTPUT_FILE = "output/output.csv"



def get_prog_dir():
    import __main__
    main_path = os.path.abspath(__main__.__file__)
    main_path = os.path.dirname(main_path) + os.sep
    return main_path


def read_sensor_info(path):
    """
    read csv file of sensors data
    """
    path += SENSOR_FILE
    sensors = pd.read_csv(path, header=0, index_col=1)
    return sensors


def energy2power(energy_df):
    """
    from cumulative energy to power (Watt)
    """
    power_df = energy_df.diff() * 1000
    power_df.fillna(0, inplace=True)
    power_df = power_df.resample("8S").mean()
    return power_df


def showTimeSeries(df, since, home_ID):
    """
    show time series : x = time, y = power (Watt)
    """
    if len(df.index) == 0:
        plt.figure()
    else:
        df.plot(colormap='jet',
                marker='.',
                markersize=5,
                title='Electricity consumption over time - home {0} - since {1}'.format(home_ID, since))

        plt.xlabel('Time')
        plt.ylabel('Power (Watt)')
        # plt.show()



def createSeries(session, sensors, since, hID):
    """
    create a list of time series from the sensors data
    """
    data_dfs = []
    home_sensors = sensors.loc[sensors["home_ID"] == hID]
    print(home_sensors)
    for id in home_sensors.sensor_id:
        dff = session.series(id, head=since)
        print(type(dff))
        data_dfs.append(session.series(id, head=since))

    return data_dfs, home_sensors


def createFluksoDataframe(session, sensors, since, home_ID):
    """
    create a dataframe where the colums are the phases of the flukso and the rows are the
    data : 1 row = 1 timestamp = 1 power value
    """
    data_dfs, home_sensors = createSeries(session, sensors, since, home_ID)
    energy_df = pd.concat(data_dfs, axis=1)
    del data_dfs
    print("nb index : ", len(energy_df.index))
    energy_df.index = pd.DatetimeIndex(energy_df.index, name="time")
    energy_df.columns = home_sensors.index
    power_df = energy2power(energy_df)
    del energy_df

    # set timestamps to local timezone
    if power_df.index.tzinfo is None or power_df.index.tzinfo.utcoffset(power_df.index) is None: # NAIVE
        power_df.index = power_df.index.tz_localize("CET").tz_convert("CET")
    else:
        power_df.index = power_df.index.tz_convert("CET")

    power_df.fillna(0, inplace=True)

    print("nb elements : ", len(power_df.index))
    print(power_df.head(10))

    return power_df


def getTiming(since):
    """
    get the timestamp of the "since"
    ex : the timestamp 20 min ago
    """
    if not since:
        since_timing = 0
    else:
        since_timing = pd.Timestamp.now(tz="UTC") - pd.Timedelta(since)
        print("Since : ", since_timing, type(since_timing))

    return since_timing


def flukso2csv(path="", since=""):
    """

    """
    if not path:
        path = get_prog_dir()

    since_timing = getTiming(since)

    sensors = read_sensor_info(path)
    session = tmpo.Session(path)
    for hid, sid, tk, st in sensors.values:
        session.add(sid, tk)

    session.sync()

    print(set(sensors["home_ID"]))
    for i in range(len(set(sensors["home_ID"]))):
        power_df = createFluksoDataframe(session, sensors, since_timing, i+1)

        showTimeSeries(power_df, since, i+1)

        # power_df.to_csv(path + OUTPUT_FILE)

    plt.show()


if __name__ == "__main__":
    argparser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    argparser.add_argument("--since", type=str, default="",
            help="Period to query until now, e.g. '30days', '1hours', '20min' etc. Defaults to all data.")
    args = argparser.parse_args()
    flukso2csv(since=args.since)

