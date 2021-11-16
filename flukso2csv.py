"""
Authors :
    - Guillaume Levasseur
    - Alexandre Heneffe

Script to fetch Fluksometer data using the tmpo protocol and
    - export it as a CSV file.
    - visualize it (time series)

Input
-----
This script requires to have a file named `sensors.csv` in the "sensors/" directory.
This `sensors.csv` file contains the home_ID, home name, sensor ID, sensor token and
state for each row.

Example input:

> cat sensors.csv
home_ID,phase,home_name,sensor_id,token,state
1,phase1+,Flukso1,fed676021dacaaf6a12a8dda7685be34,b371402dc767cc83e41bc294b63f9586,+

Output
------
File `output.csv` in output/
/!\ The previous file will be overwritten!
The file contains the power time series, each sensor/phase a column.
"""


import argparse
import os

import pandas as pd
from pandas import read_csv
import tmpo
from datetime import datetime
import matplotlib.pyplot as plt


SENSOR_FILE = "sensors/sensors.csv"
OUTPUT_FILE = "output/output.csv"
FREQUENCY = "8S"


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
    power_df = power_df.resample(FREQUENCY).mean()
    return power_df


def showTimeSeries(df, since, home_ID):
    """
    show time series : x = time, y = power (Watt)
    """
    # if len(df.index) == 0:
    #     plt.figure()
    # else:
    df.plot(colormap='jet',
            marker='.',
            markersize=5,
            title='Electricity consumption over time - home {0} - since {1}'.format(home_ID, since))

    plt.xlabel('Time')
    plt.ylabel('Power (kiloWatts) - KW')
    # plt.show()


def getZeroSeries(since, since_timing):
    frequency = 8  # seconds
    period = pd.Timedelta(since).total_seconds() / frequency
    zeros = pd.date_range(since_timing, periods=period, freq=str(frequency) + "S")
    # print("datetime range : ", zeros)
    zeros_series = pd.Series(int(period) * [0], zeros)
    # print("zeros series :", zeros_series)

    return zeros_series



def createSeries(session, sensors, since, since_timing, hID):
    """
    create a list of time series from the sensors data
    """
    data_dfs = []
    home_sensors = sensors.loc[sensors["home_ID"] == hID]

    # print(home_sensors)
    for id in home_sensors.sensor_id:
        print("{} :".format(id))
        print("- first timestamp : {}".format(session.first_timestamp(id)))
        print("- last timestamp : {}".format(session.last_timestamp(id)))

        dff = session.series(id, head=since_timing)
        if len(dff.index) == 0:
            dff = getZeroSeries(since, since_timing)
        data_dfs.append(dff)

    return data_dfs, home_sensors


def createFluksoDataframe(session, sensors, since, since_timing, home_ID):
    """
    create a dataframe where the colums are the phases of the flukso and the rows are the
    data : 1 row = 1 timestamp = 1 power value
    """
    data_dfs, home_sensors = createSeries(session, sensors, since, since_timing, home_ID)
    energy_df = pd.concat(data_dfs, axis=1)
    del data_dfs
    print("nb index : ", len(energy_df.index))
    # print(energy_df.index)
    energy_df.index = pd.DatetimeIndex(energy_df.index, name="time")
    energy_df.columns = home_sensors.index
    power_df = energy2power(energy_df)
    del energy_df

    # set timestamps to local timezone
    if power_df.index.tzinfo is None or power_df.index.tzinfo.utcoffset(power_df.index) is None:  # NAIVE
        power_df.index = power_df.index.tz_localize("CET").tz_convert("CET")
    else:
        power_df.index = power_df.index.tz_convert("CET")

    power_df.fillna(0, inplace=True)

    print("nb elements : ", len(power_df.index))

    print("=======> 10 first elements : ")
    print(power_df.head(10))

    return power_df


def getTiming(since):
    """
    get the timestamp of the "since"
    ex : the timestamp 20 min ago
    """
    print("since {}".format(since))
    if not since:
        since_timing = 0
    else:
        since_timing = pd.Timestamp.now(tz="UTC") - pd.Timedelta(since)
        print("timing in sec : ", pd.Timedelta(since).total_seconds())
        print("Since : ", since_timing, type(since_timing))

    print("since timing : ", since_timing)
    return since_timing


def flukso2visualization(path="", since=""):
    """
    get Flukso data (via API) then visualize the data
    """
    if not path:
        path = get_prog_dir()

    since_timing = getTiming(since)

    sensors = read_sensor_info(path)
    session = tmpo.Session(path)
    for hid, hn, sid, tk, st in sensors.values:
        session.add(sid, tk)

    session.sync()

    nb_homes = len(set(sensors["home_ID"]))
    print("Homes : ", nb_homes)

    plt.style.use('grayscale')

    for i in range(nb_homes):
    # for i in range(1):

        print("========================= HOME {} =====================".format(i+1))
        power_df = createFluksoDataframe(session, sensors, since, since_timing, i+1)

        showTimeSeries(power_df, since, i+1)

        # power_df.to_csv(path + OUTPUT_FILE)

    plt.show()


def main():
    argparser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    argparser.add_argument("--since",
                           type=str,
                           default="",
                           help="Period to query until now, e.g. '30days', '1hours', '20min' etc. Defaults to all data.")
    args = argparser.parse_args()
    flukso2visualization(since=args.since)


if __name__ == "__main__":
    main()

