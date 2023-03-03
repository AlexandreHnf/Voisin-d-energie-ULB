__title__ = "utils"
__version__ = "2.0.0"
__author__ = "Alexandre Heneffe, Guillaume Levasseur, and Brice Petit"
__license__ = "MIT"


# standard library
import os
import sys
import os.path
import math
from datetime import timedelta

# 3rd party packages
import pandas as pd
import numpy as np
import logging
import logging.handlers

# local sources
from constants import (
    CASSANDRA_KEYSPACE,
    FREQ,
    LOG_LEVEL,
    LOG_FILE,
    LOG_HANDLER,
    TBL_POWER,
    TBL_SENSORS_CONFIG,
)

from sensors_config import Configuration
import py_to_cassandra as ptc


def setup_log_level():
    """
    set logging level based on a constant
    levels :
    - CRITICAL
    - ERROR
    - WARNING
    - INFO
    - DEBUG
    """
    if LOG_LEVEL == "CRITICAL":
        return logging.CRITICAL
    elif LOG_LEVEL == "ERROR":
        return logging.ERROR
    elif LOG_LEVEL == "WARNING":
        return logging.WARNING
    elif LOG_LEVEL == "INFO":
        return logging.INFO
    elif LOG_LEVEL == "DEBUG":
        return logging.DEBUG


def get_log_handler():
    """
    If prod : rotating logfile handler
    If dev : only stdout
    """
    if LOG_HANDLER == "logfile":
        handler = logging.handlers.TimedRotatingFileHandler(
            LOG_FILE,
            when='midnight',
            backupCount=7,
        )
    else:  # stdout
        handler = logging.StreamHandler(stream=sys.stdout)
    return handler


# Create and configure logger
logging.getLogger("tmpo").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

logging.basicConfig(
    level=setup_log_level(),
    format="{asctime} {levelname:<8} {filename:<16} {message}",
    style='{',
    handlers=[get_log_handler()]
)


def get_prog_dir():
    import __main__
    main_path = os.path.abspath(__main__.__file__)
    main_path = os.path.dirname(main_path) + os.sep
    return main_path


def read_sensor_info(path, sensor_file):
    """
    read csv file of sensors data
    """
    path += sensor_file
    sensors = pd.read_csv(
        path,
        header=0,
        index_col=1
    )
    return sensors


def set_init_seconds(ts):
    """
    SS = 00 if M even, 04 if odd
    """
    return ts.replace(second=4 if ts.minute % 2 != 0 else 0)


def get_last_registered_config():
    """
    Get the last registered config based on insertion time
    """
    latest_configs = ptc.groupby_query(
        CASSANDRA_KEYSPACE,
        TBL_SENSORS_CONFIG,
        column='insertion_time',
        groupby_operator='max',
        groupby_cols=['home_id', 'sensor_id'],
        allow_filtering=False,
        tz="UTC"
    )
    if len(latest_configs) == 0:  # if no config in db yet.
        return None
    last_config_id = latest_configs.max().max().tz_localize('UTC')
    config_df = ptc.select_query(
        CASSANDRA_KEYSPACE,
        TBL_SENSORS_CONFIG,
        ["*"],
        "insertion_time = '{}'".format(last_config_id),
    )
    config = Configuration(last_config_id, config_df.set_index("sensor_id"))
    return config


def get_all_registered_configs():
    """
    Get all configs present in the system
    returns a list of config ids

    POTENTIAL ISSUE : if the whole config table does not fit in memory
        -> possible solution : select distinct insertion_time, home_id, sensor_id
            to reduce the nb of queried lines
    """
    all_configs_df = ptc.select_query(
        CASSANDRA_KEYSPACE,
        TBL_SENSORS_CONFIG,
        ["*"],
        where_clause="",
        limit=None,
        allow_filtering=False
    )

    configs = []
    if len(all_configs_df) > 0:
        for config_id, config in all_configs_df.groupby("insertion_time"):

            configs.append(Configuration(config_id, config.set_index("sensor_id")))

    return configs


def get_home_power_data_from_cassandra(home_id, date, ts_clause=""):
    """
    Get power data from Power table in Cassandra
    > for 1 specific home
    > specific day
    """

    where_clause = "home_id = '{}' and day = '{}' {}".format(home_id, date, ts_clause)
    cols = [
        "home_id",
        "day",
        "ts",
        "p_cons",
        "p_prod",
        "p_tot"
    ]

    home_df = ptc.select_query(
        CASSANDRA_KEYSPACE,
        TBL_POWER,
        cols,
        where_clause,
    )

    return home_df


def get_dates_between(start_date, end_date):
    """
    get the list of dates between 2 given dates
    """
    d = pd.date_range(
        start_date.date(),
        end_date.date() - timedelta(days=1),
        freq='d'
    )

    dates = []
    for ts in d:
        dates.append(str(ts.date()))
    dates.append(str(end_date.date()))

    return dates


def to_epochs(time):
    return int(math.floor(time.value / 1e9))


def is_earlier(ts1, ts2):
    """
    check if timestamp 'ts1' is earlier/older than timestamp 'ts2'
    """
    return (ts1 - ts2) / np.timedelta64(1, 's') < 0


def time_range(since, until):
    return pd.date_range(
        since,
        until,
        freq=str(FREQ[0]) + FREQ[1],
        closed='left',
    )


def energy2power(energy_df):
    """
    From cumulative energy to power (Watt).
    The global idea is the following:
        - We want to fill nan with something. We want to apply it before the diff
        because the diff will be 0 by a fill (no change).
        - We first apply a ffill and then a bfill in order to not have a series that
        start with nan.
        - Finally, we apply a bfill AFTER the diff because the diff will create a nan
        in the first raw => that produce peak to 0. Then we check if it remains a nan
        and if it is the case => 0. (case where there is no data in server)
    """
    # Compute the difference between each value.
    power_df = (
        energy_df
        .ffill()
        .bfill()
        .diff()
        .bfill()
    )
    # Check if the DataFrame is not empty.
    if not power_df.empty:
        # Compute the delta time between each point and express this delta time in seconds.
        delta = (
            power_df
            .index
            .to_series()
            .diff()
            .bfill()
            .dt
            .total_seconds()
        )
        # Apply the formula to convert kWh in W.
        power_df = (
            power_df
            .mul(3600)
            .div(delta.values, axis=0)
        )
    return power_df


def get_time_spent(time_begin, time_end):
    """
    Get the time spent in seconds between 2 timings (1 timing = time.time())
    """
    return timedelta(seconds=time_end - time_begin)


def get_local_timestamps_index(df):
    """
    set timestamps to local timezone
    """

    # NAIVE
    if df.index.tzinfo is None or df.index.tzinfo.utcoffset(df.index) is None:
        # first convert to aware timestamp, then local
        return df.index.tz_localize("CET", ambiguous='NaT').tz_convert("CET")
    # if already aware timestamp
    else:
        return df.index.tz_convert("CET")


def main():
    config = get_last_registered_config()
    print(config.get_sensors_config())


if __name__ == '__main__':
    main()
