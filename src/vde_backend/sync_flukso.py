__title__ = "sync_flukso"
__version__ = "2.0.0"
__author__ = "Alexandre Heneffe, Guillaume Levasseur, and Brice Petit"
__license__ = "MIT"


"""
Script to fetch Fluksometer data using the tmpo protocol and
- format it into tables of time series
- save the raw data in Cassandra database
"""


# standard library
from datetime import timedelta
import time
import argparse
import sys

from threading import Thread

# 3rd party packages
import pandas as pd
import numpy as np
import tmpo

from utils import (
    logging,
    get_last_registered_config,
    get_prog_dir,
    get_time_spent,
    is_earlier,
    set_init_seconds,
    read_sensor_info,
    energy2power,
    get_local_timestamps_index,
    time_range
)


# Hide warnings :
import urllib3
import warnings


# local sources
from constants import (
    PROD,
    CASSANDRA_KEYSPACE,
    FROM_FIRST_TS,
    GAP_THRESHOLD,
    INSERTS_PER_BATCH,
    LIMIT_TIMING_RAW,
    TBL_RAW,
    FREQ,
    TBL_RAW_MISSING,
    TMPO_FILE,
    TBL_POWER
)


import py_to_cassandra as ptc
from compute_power import save_home_power_data_to_cassandra, get_consumption_production_df

# security warning & Future warning
warnings.simplefilter('ignore', urllib3.exceptions.SecurityWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

# ====================================================================================
# Cassandra table creation
# ====================================================================================


def create_raw_flukso_table(table_name):
    """
    create a cassandra table for the raw flukso data
    """

    columns = [
        "sensor_id TEXT",
        "day TEXT", 				# CET timezone
        "ts TIMESTAMP", 			# UTC timezone (automatically converted)
        "insertion_time TIMESTAMP",
        "config_id TIMESTAMP",
        "power FLOAT"
    ]

    ptc.create_table(
        CASSANDRA_KEYSPACE,
        table_name,
        columns,
        ["sensor_id"],
        ["day", "ts"],
        {"day": "ASC", "ts": "ASC"}
    )


def create_power_table(table_name):
    """
    create a cassandra table for the power data
    """

    power_cols = [
        "home_id TEXT",
        "day TEXT", 			# CET timezone
        "ts TIMESTAMP", 		# UTC timezone (automatically converted)
        "P_cons FLOAT",
        "P_prod FLOAT",
        "P_tot FLOAT",
        "insertion_time TIMESTAMP",
        "config_id TIMESTAMP"
    ]

    ptc.create_table(
        CASSANDRA_KEYSPACE,
        table_name,
        power_cols,
        ["home_id"],
        ["day", "ts"],
        {"day": "ASC", "ts": "ASC"}
    )


def create_raw_missing_table(table_name):
    """
    Raw missing table contains timestamps range where there is missing data
    from a specific query given a specific configuration of the sensors
    """

    cols = [
        "sensor_id TEXT",
        "config_id TIMESTAMP",
        "start_ts TIMESTAMP",
        "end_ts TIMESTAMP"
    ]

    ptc.create_table(
        CASSANDRA_KEYSPACE,
        table_name,
        cols,
        ["sensor_id", "config_id"],
        ["start_ts"],
        {"start_ts": "ASC"}
    )


# ====================================================================================


def get_last_registered_timestamp(table_name, sensor_id):
    """
    get the last registered timestamp of the raw table
    - None if no timestamp in the table

    We assume that if there is data in raw table, each sensor can have different last timestmaps
    registered.
    Assume the 'raw' table is created

    technique : first get the last registered date for the sensor, then
    query the last timestamp of this day for this sensor.

    more robust but much slower
    """
    # get last date available for this home
    where_clause = "sensor_id = '{}' ORDER BY day DESC".format(sensor_id)
    dates_df = ptc.select_query(
        CASSANDRA_KEYSPACE,
        table_name,
        ["day"],
        where_clause,
        limit=1,
    )

    if len(dates_df) > 0:
        last_date = dates_df.iat[0, 0]
        ts_df = ptc.select_query(
            CASSANDRA_KEYSPACE,
            table_name,
            ["ts"],
            "sensor_id = '{}' AND day = '{}'".format(sensor_id, last_date),
        )
        if len(ts_df) > 0:
            return ts_df.max().max()

    return None


def get_initial_timestamp(tmpo_session, sid, now):
    """
    get the first ever registered timestamp for a sensor using tmpo Session
    if no such timestamp (None), return an arbitrary timing (ex: since 4min)

    return a timestamp in local timezone (CET)
    """
    initial_ts = now if FROM_FIRST_TS is None else (now - pd.Timedelta(FROM_FIRST_TS))
    if PROD:
        initial_ts_tmpo = tmpo_session.first_timestamp(sid)

        if initial_ts_tmpo is not None:
            initial_ts = initial_ts_tmpo.tz_convert("CET")

    return initial_ts


def get_sensor_timings(tmpo_session, missing_data, sid, now):
    """
    For each sensor, we get start timing, forming the interval of time we have to
    query to tmpo
    - The start timing is either based on the missing data table, or the initial timestamp
    of the sensor if no missing data registered, or simply the default timing (the last
    registered timestamp in raw data table)

    return a starting timestamp with CET timezone
        or None if no starting timestamp
    """
    if sid in missing_data['sensor_id']:  # if there is missing data for this sensor
        # CET timezone (minus a certain offset to avoid losing first ts)
        sensor_start_ts = (
            missing_data
            .groupby('sensor_id')
            .get_group(sid)["start_ts"]
            - timedelta(seconds=FREQ[0])
        )  # sensor start timing = missing data first timestamp
    else:  # if no missing data for this sensor
        default_timing = get_last_registered_timestamp(TBL_RAW, sid)  # None or tz-naive CET
        if default_timing is None:  # if no raw data registered for this sensor yet
            # we take its first tmpo timestamp
            sensor_start_ts = get_initial_timestamp(tmpo_session, sid, now)
        else:
            sensor_start_ts = default_timing

    return sensor_start_ts


def compute_timings(tmpo_session, timings, home_id, sensors_ids, missing_data, now):
    """
    Function to compute timings. By timings we mean that moments where we don't have data.

    For instance, sensor_start_ts  will give the first moment where don't have data.

    :param tmpo_session:    Tmpo session.
    :param timings:         Timings -> period where there are missing data.
    :param home_id:         Home id.
    :param sensors_ids:     List of all sensors for the home_id.
    :param missing_data:    Dataframe containing missing data.
    :param now:             Current timestamp.

    :return:                Return a dictionary with periods where there are missing data.
    """
    # for each sensor of this home
    for sid in sensors_ids:
        # Get the first moment where we have missing data
        sensor_start_ts = get_sensor_timings(tmpo_session, missing_data, sid, now)

        # Sometimes, get_sensor_timing will return a list. So, if it is not a list,
        # we create a list.
        if type(sensor_start_ts) is not pd.Series:
            sensor_start_ts = [sensor_start_ts]
        # For each potential ts.
        for ts in sensor_start_ts:
            logging.debug(f"sensors start ts : {ts}")
            # if 'ts' is older (in the past) than the current start_ts
            if is_earlier(ts, timings[home_id]["start_ts"]):
                timings[home_id]["start_ts"] = ts

            # ensures a CET timezone
            if str(ts.tz) == "None":
                ts = ts.tz_localize("CET")
            # CET
            timings[home_id]["sensors"][sid] = set_init_seconds(ts)

    # no data to recover from this home
    if timings[home_id]["start_ts"] is now:
        timings[home_id]["start_ts"] = None
    return timings


def get_timings(tmpo_session, config, missing_data, now):
    """
    For each home, get the start timing for the query based on the missing data table
    (containing for each sensor the first timestamp with missing data from the previous query)
    if no timestamp available yet for this sensor, we get the first ever timestamp available
    for the Flukso sensor with tmpo API

    default_timing = last registered timestamp for a home : CET tz
    'now' : CET by default
    """
    timings = {}
    try:
        ids = config.get_ids()
        for home_id, sensors_ids in ids.items():
            # start_ts = earliest timestamp among all sensors of this home
            timings[home_id] = {
                "start_ts": now,
                "end_ts": now - pd.Timedelta(minutes=10),
                "sensors": {}
            }

            timings = compute_timings(
                tmpo_session, timings, home_id, sensors_ids, missing_data, now
            )

        # truncate existing rows in Raw missing table
        ptc.delete_rows(CASSANDRA_KEYSPACE, TBL_RAW_MISSING)
        logging.debug("Missing raw table deleted")

    except Exception:
        logging.critical("Exception occured in 'get_timings' : ", exc_info=True)

    return timings


def set_custom_timings(config, timings, custom_timings):
    """
    Set custom start timing and custom end timing for each home, and each sensor
    - Same custom timings for each home.
    """
    try:
        for home_id, sensors_ids in config.get_ids().items():
            timings[home_id] = {
                "start_ts": custom_timings["start_ts"],
                "end_ts": custom_timings["end_ts"],
                "sensors": {}
            }
            for sid in sensors_ids:
                # CET
                timings[home_id]["sensors"][sid] = set_init_seconds(custom_timings["start_ts"])

    except Exception:
        logging.critical("Exception occured in 'set_custom_timings' : ", exc_info=True)


def process_timings(tmpo_session, config, missing_data, now, custom_timings):
    """
    Get the timings for each home
    Timings are either custom or determined by the current database state.
    """
    timings = {}
    if (len(custom_timings) > 0):
        set_custom_timings(config, timings, custom_timings)
    else:
        timings = get_timings(
            tmpo_session,
            config,
            missing_data,
            now
        )

    return timings


# ====================================================================================


def test_session(sensors_config):
    """
    test each sensor and see if tmpo accepts or refuses the sensor when
    syncing
    """
    path = TMPO_FILE
    if not path:
        path = get_prog_dir()

    for sid, row in sensors_config.get_sensors_config().iterrows():
        try:
            logging.debug("{}, {}".format(sid, row["sensor_token"]))
            tmpo_session = tmpo.Session(path)
            tmpo_session.add(sid, row["sensor_token"])
            tmpo_session.sync()
            logging.debug("=> OK")
        except Exception:
            logging.warning("=> NOT OK")
            continue


def get_tmpo_session(config):
    """
    Get tmpo (via api) session with all the sensors in it
    """
    path = TMPO_FILE
    if not path:
        path = get_prog_dir()
    logging.info("tmpo path : " + path)

    tmpo_session = tmpo.Session(path)
    for sid, row in config.get_sensors_config().iterrows():
        tmpo_session.add(sid, row["sensor_token"])

    logging.info("> tmpo synchronization...")
    try:
        tmpo_session.sync()
    except Exception:
        logging.warning("Exception occured in tmpo sync: ", exc_info=True)
        logging.warning("> tmpo sql file needs to be reset, or some sensors are invalid.")
    logging.info("> tmpo synchronization : OK")

    return tmpo_session


def get_flukso_data(sensor_file, path=""):
    """
    get Flukso tmpo session (via API) + sensors info (IDs, ...)
    from a csv file containing the sensors configurations
    """
    if not path:
        path = get_prog_dir()

    sensors = read_sensor_info(path, sensor_file)
    tmpo_session = tmpo.Session(path)
    for hid, hn, sid, tk, n, c, p in sensors.values:
        tmpo_session.add(sid, tk)

    tmpo_session.sync()

    return sensors, tmpo_session


# ====================================================================================


def save_home_missing_data(config, to_timing, hid, incomplete_raw_df):
    """
    Save the first timestamp with no data (nan values) for each sensors of the home
    """
    saved_sensors = {}
    try:
        config_id = config.get_config_id()

        col_names = ["sensor_id", "config_id", "start_ts", "end_ts"]

        if len(incomplete_raw_df) > 0:
            sensors_ids = incomplete_raw_df.columns
            for sid in sensors_ids:
                # if no missing data saved for this sensor yet
                if saved_sensors.get(sid, None) is None:
                    # if the column contains null
                    if incomplete_raw_df[sid].isnull().values.any():
                        for i, timestamp in enumerate(incomplete_raw_df.index):
                            # if valid timestamp
                            # X days from now max
                            if (to_timing - timestamp).days < LIMIT_TIMING_RAW:
                                if np.isnan(incomplete_raw_df[sid][i]):
                                    values = [sid, config_id, timestamp, to_timing]
                                    ptc.insert(
                                        CASSANDRA_KEYSPACE, TBL_RAW_MISSING,
                                        col_names, values
                                    )
                                    # mark that this sensor has missing data
                                    saved_sensors[sid] = True
                                    # as soon as we find the first ts with null value,
                                    # we go to next sensor
                                    break
    except Exception:
        logging.critical(
            "Exception occured in 'save_home_missing_data' : {} ".format(hid), exc_info=True
        )


def save_home_raw_data(hid, raw_df, config, timings):
    """
    Save raw flukso flukso data to Cassandra table
    Save per sensor : 1 row = 1 sensor + 1 timestamp + 1 power value
        home_df : timestamp, sensor_id1, sensor_id2, sensor_id3 ... sensor_idN
    """
    try:
        insertion_time = pd.Timestamp.now(tz="CET")
        config_id = config.get_config_id()

        # add date column
        raw_df['date'] = raw_df.apply(lambda row: str(row.name.date()), axis=1)
        by_day_df = raw_df.groupby("date")  # group by date

        col_names = ["sensor_id", "day", "ts", "insertion_time", "config_id", "power"]
        for date, date_rows in by_day_df:  # loop through each group (each date group)

            for sid in date_rows:  # loop through each column, 1 column = 1 sensor
                if sid == "date" or timings[hid]["sensors"][sid] is None:
                    continue
                insert_queries = ""
                for i, timestamp in enumerate(date_rows[sid].index):
                    # if the timestamp > the sensor's defined start timing
                    if is_earlier(timings[hid]["sensors"][sid], timestamp):
                        power = date_rows[sid][i]
                        values = [sid, date, timestamp, insertion_time, config_id, power]
                        insert_queries += ptc.get_insert_query(
                            CASSANDRA_KEYSPACE, TBL_RAW, col_names, values
                        )

                        if (i + 1) % INSERTS_PER_BATCH == 0:
                            ptc.batch_insert(insert_queries)
                            insert_queries = ""

                ptc.batch_insert(insert_queries)
    except Exception:
        logging.critical("Exception occured in 'save_home_raw_data' : ", exc_info=True)


# ====================================================================================

def display_home_info(home_id, start_ts, end_ts):
    """
    Display some info during the execution of a query for logging. Can be activated by
    turning the logging mode to 'INFO'
    > home id | start timestamp > end timestamp (nb days > nb minutes)
        -> date 1 start timestamp > date 1 end timestamp (nb minutes)
            - nb raw data, nb NaN data, total nb NaN data
        -> date 2 start timestamp > date 2 end timestamp (nb minutes)
            - ...
        -> ...
    """

    nb_days = (end_ts - start_ts).days
    duration_min = 0
    if start_ts is not None and end_ts is not None:
        duration_min = round((end_ts - start_ts).total_seconds() / 60.0, 2)
    if duration_min > 0:
        logging.info("> {} | {} > {} ({} days > {} min.)".format(
            home_id,
            start_ts,
            end_ts,
            nb_days,
            duration_min
        ))
    else:
        logging.info("> {} | no data to recover".format(home_id))


def get_intermediate_timings(start_ts, end_ts):
    """
    Given 2 timestamps, generate the intermediate timings
    - interval duration = 1 day
    """
    intermediate_timings = list(pd.date_range(
        start_ts,
        end_ts,
        freq="1D"
    ))

    if len(intermediate_timings) == 1 and end_ts != start_ts:
        intermediate_timings.append(end_ts)

    return intermediate_timings


def save_data_threads(
    hid, raw_df, incomplete_raw_df, cons_prod_df,
    config, timings, now, custom
):
    """
    Threads to save data to different Cassandra tables
    -> raw data in raw table
    -> raw missing data in raw_missing table
    -> power data in power table
    """

    threads = []
    # save raw flukso data in cassandra
    if len(raw_df) > 0:
        t1 = Thread(
            target=save_home_raw_data,
            args=(hid, raw_df, config, timings)
        )
        threads.append(t1)
        t1.start()

    # in custom mode, no need to save missing data (in the past)
    # and check if there are data
    if not custom and len(incomplete_raw_df) > 0:
        # save missing raw data in cassandra
        t2 = Thread(
            target=save_home_missing_data,
            args=(config, now, hid, incomplete_raw_df)
        )
        threads.append(t2)
        t2.start()

    # save power flukso data in cassandra
    if len(cons_prod_df) > 0:
        t3 = Thread(
            target=save_home_power_data_to_cassandra,
            args=(hid, cons_prod_df, config)
        )
        threads.append(t3)
        t3.start()

    # wait for the threads to complete
    for t in threads:
        t.join()


def find_incomplete_raw_df(energy_df):
    """
    From the cumulative energy dataframe,
    1) fill the gaps in the timestamps with NaN values
    2) get a dataframe containing all the lines with NaN values (= incomplete rows)
    """
    incomplete_raw_df = energy_df[energy_df.isna().any(axis=1)]  # with CET timezones
    incomplete_raw_df.index = pd.DatetimeIndex(incomplete_raw_df.index, name="time")
    # convert all timestamps to local timezone (CET)
    incomplete_raw_df.index = get_local_timestamps_index(incomplete_raw_df)
    return incomplete_raw_df


def generate_raw_df(df):
    """
    The goal of this function is to generate a raw dataframe according to the df
    that can be a dataframe with consumption values or production values. It will
    return the generated dataframe.

    The main idea of this function is that we don't want to keep a hole of data
    that is above a certain threshold but holes that are under the threshold, we
    want to keep them. We want to fill these small holes.

    :param df:  Dataframe. The dataframe can be a dataframe with consumption values
                or production values.

    :return:    Return the new created dataframe.
    """
    raw_df = pd.DataFrame()
    # Check if there is a hole of values of a certain threshold of time.
    if ((df.dropna().index.to_series().diff() > GAP_THRESHOLD).cumsum().sum()) > 0:
        tmp_res = []
        # If it is the case, we create a group where we drop all data.
        group = df.groupby((df.dropna().index.to_series().diff() > GAP_THRESHOLD).cumsum())
        for chunk_nb, chunk in group:
            # First, we check the difference between the first time value of the group 0
            # and the first time value of energy_df. The reason is that the dropna will
            # drop all na but we want to keep na that are under the threshold and remove
            # all na that above the threshold. That's also why we consider df and not chunk.
            if (
                chunk_nb == 0
                and chunk.index[0] != df.index[0]
                and (chunk.index[0] - df.index[0]) <= pd.Timedelta(GAP_THRESHOLD)
            ):
                tmp_res.append(energy2power(df[df.index[0]:chunk.index[-1]]))
            # Same reason as before but with last values.
            elif (
                chunk_nb == group.ngroups - 1
                and chunk.index[-1] != df.index[-1]
                and (df.index[-1] - chunk.index[-1]) <= pd.Timedelta(GAP_THRESHOLD)
            ):
                tmp_res.append(
                    energy2power(df[chunk.index[0]:df.index[-1]])
                )
            # All intermediates states.
            else:
                tmp_res.append(energy2power(df[chunk.index[0]:chunk.index[-1]]))
        # Check if the list is not empty
        if len(tmp_res) > 0:
            raw_df = pd.concat(tmp_res)
    # If it is not the case, we do the way to have the power.
    else:
        raw_df = energy2power(df)
    return raw_df


def create_flukso_raw_df(energy_df, home_sensors):
    """
    create a dataframe where the colums are the phases of the Flukso and the rows are the
    data :
    1 row = 1 timestamp = 1 power value
    """
    # Separate mains and pv.
    raw_cons = generate_raw_df(energy_df[home_sensors.loc[home_sensors['net'] != 0].index])
    raw_prod = generate_raw_df(energy_df[home_sensors.loc[home_sensors['pro'] != 0].index])
    # It is missing a column VE in the database and we need it for ECHCOM
    # raw_ve = generate_raw_df(energy_df[home_sensors.loc[home_sensors['ve'] != 0]])
    # raw_df = pd.concat([raw_cons, raw_prod, raw_ve], axis=1)
    raw_df = pd.concat([raw_cons, raw_prod], axis=1)
    if len(raw_df) > 0:
        # convert all timestamps to local timezone (CET)
        local_timestamps = get_local_timestamps_index(raw_df)
        raw_df.index = local_timestamps
        if len(local_timestamps) > 1:
            # drop first row because NaN after conversion
            raw_df.drop(local_timestamps[0], inplace=True)

            raw_df = raw_df.round(1)  # round with 1 decimals
    return raw_df


def get_serie(session, sensor_id, since_timing, to_timing):
    """
    # since_timing and to_timing = UTC timezone for tmpo query
    """
    if to_timing == 0:
        df = session.series(
            sensor_id,
            head=since_timing
        )
    else:
        df = session.series(
            sensor_id,
            head=since_timing,
            tail=to_timing
        )
    if len(df.index) == 0:
        df = pd.Series(np.nan, index=time_range(since_timing, to_timing))
    return df


def create_energy_df(tmpo_session, home_sensors, start_ts, to_ts):
    """
    Function to create a dataframe with energies for each sensor.

    :param tmpo_session:    Tmpo Session.
    :param home_sensors:    Dataframe with the config.
    :param start_ts:        Start ts.
    :param to_ts:           End ts.

    :return:                Return an energy dataframe.
    """
    sensors = []
    columns = []
    for sid, _ in home_sensors.iterrows():
        sensors.append(get_serie(tmpo_session, sid, start_ts, to_ts))
        columns.append(sid)
    energy_df = pd.concat(sensors, axis=1)
    energy_df.columns = columns
    return energy_df


def process_homes(tmpo_session, config, timings, now, custom):
    """
    For each home, we first create the home object containing
    all the tmpo queries and series computation
    Then, we save computed data in Cassandra tables.
    """
    # for each home
    for hid, home_sensors in config.get_sensors_config().groupby("home_id"):
        # if home has a start timestamp and a end timestamp
        if timings[hid]["start_ts"] is not None and timings[hid]["end_ts"] is not None:
            # set init seconds (for tmpo query), might set timings
            # earlier than planned (not a problem)
            start_timing = set_init_seconds(timings[hid]["start_ts"])
            end_timing = set_init_seconds(timings[hid]["end_ts"])
            intermediate_timings = get_intermediate_timings(start_timing, end_timing)
            display_home_info(hid, start_timing, end_timing)
            # query day by day
            for i in range(len(intermediate_timings) - 1):
                # generate energy df
                energy_df = create_energy_df(
                    tmpo_session, home_sensors,
                    intermediate_timings[i], intermediate_timings[i + 1]
                )
                # If all values are nan, we need to add the name of the column
                # and we don't retrieve data
                if energy_df.isna().all().all():
                    raw_df = pd.DataFrame()
                    cons_prod_df = pd.DataFrame()
                else:
                    raw_df = create_flukso_raw_df(energy_df, home_sensors)
                    cons_prod_df = get_consumption_production_df(raw_df, home_sensors)

                incomplete_raw_df = find_incomplete_raw_df(energy_df)

                logging.info("     - len raw : {}, len NaN : {}, tot NaN: {}".format(
                    len(raw_df.index),
                    len(incomplete_raw_df),
                    energy_df.isna().sum().sum()
                ))

                save_data_threads(
                    hid, raw_df, incomplete_raw_df, cons_prod_df,
                    config, timings, now, custom
                )
        else:
            logging.info("{} : No data to save".format(hid))


# ====================================================================================


def show_config_info(config):
    """
    Display Configuration stats/information
    """

    logging.info("- Number of Homes :           {}".format(
        str(config.get_nb_homes())
    ))
    logging.info("- Number of Fluksos :         {}".format(
        str(len(set(config.get_sensors_config().flukso_id)))
    ))
    logging.info("- Number of Fluksos sensors : {}".format(
        str(len(config.get_sensors_config()))
    ))


def show_processing_times(begin, setup_time, t):
    """
    Display processing time for each step of 1 query
    t = timer (dictionary with running timings)
    """

    logging.info("--------------------- Timings --------------------")
    logging.info("> Setup time :                     {}.".format(
        get_time_spent(begin, setup_time)
    ))
    logging.info("> Timings computation :            {}.".format(
        get_time_spent(t["start"], t["timing"])
    ))
    logging.info("> Generate homes + saving in db :  {}.".format(
        get_time_spent(t["timing"], t["homes"])
    ))

    logging.info("> Total Processing time :          {}.".format(
        get_time_spent(begin, time.time())
    ))


def create_tables():
    """
    create the necessary tables for the flukso data synchronization
    """
    create_raw_flukso_table(TBL_RAW)
    create_raw_missing_table(TBL_RAW_MISSING)
    create_power_table(TBL_POWER)


def sync(custom_timings):
    logging.info("====================== Sync ======================")

    # custom mode (custom start and end timings)
    custom = "start_ts" in custom_timings  # custom mode
    logging.info("- Custom mode :               " + str(custom))
    begin = time.time()

    # =============================================================

    now = pd.Timestamp.now(tz="CET").replace(microsecond=0)  # remove microseconds for simplicity

    # > Configuration
    config = get_last_registered_config()
    if config:
        missing_data = ptc.select_query(
            CASSANDRA_KEYSPACE,
            TBL_RAW_MISSING,
            ["*"],
            where_clause="",
            limit=None,
            allow_filtering=False
        )

        logging.info("- Running time (Now - CET) :  " + str(now))
        setup_time = time.time()

        # =========================================================

        # Timer
        config_id = config.get_config_id()
        logging.info("- Config :                    " + str(config_id))
        timer = {"start": time.time()}

        # Config information
        show_config_info(config)

        logging.info("---------------------- Tmpo -----------------------")

        # TMPO synchronization
        tmpo_session = get_tmpo_session(config)

        # STEP 1 : get start and end timings for all homes for the query
        timings = process_timings(
            tmpo_session,
            config,
            missing_data,
            now,
            custom_timings
        )
        timer["timing"] = time.time()

        # =========================================================

        logging.info("---------------------- Homes ---------------------")
        logging.info("Generating homes data, getting Flukso data and save in Cassandra...")

        # STEP 2 : process all homes data, and save in database
        process_homes(tmpo_session, config, timings, now, custom)

        timer["homes"] = time.time()

        # =========================================================

        show_processing_times(begin, setup_time, timer)
    else:
        logging.debug("No registered config in db.")


def process_custom_timings(start, end):
    """
    Given the custom mode, check if the two provided arguments
    are valid. Namely, if the timestamp format is ok, and
    start ts < end ts
    """
    custom_timings = {}
    if start:
        try:
            custom_timings["start_ts"] = pd.Timestamp(start, tz="CET")
            custom_timings["end_ts"] = pd.Timestamp(end, tz="CET")
        except Exception:
            logging.critical("Wrong argument format - custom timings : ", exc_info=True)
            sys.exit(1)

        if is_earlier(custom_timings["end_ts"], custom_timings["start_ts"]):
            logging.critical(
                "Wrong arguments (custom timings) : first timing must be earlier than second timing"
            )
            sys.exit(1)

    return custom_timings


def get_arguments():
    """
    process arguments
    argument : custom mode : choose a interval between 2 specific timings
    """
    argparser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    argparser.add_argument(
        "--start",
        type=str,
        help="Start timing. Format : YYYY-mm-ddTHH:MM:SS"
    )

    argparser.add_argument(
        "--end",
        type=str,
        default=str(pd.Timestamp.now()),
        help="End timing. Format : YYYY-mm-ddTHH:MM:SS"
    )

    return argparser.parse_args()


def main():
    args = get_arguments()

    # Custom timings argument
    custom_timings = process_custom_timings(args.start, args.end)

    # first, create tables if needed:
    create_tables()

    # then, sync new data in Cassandra
    sync(custom_timings)


if __name__ == "__main__":
    main()
