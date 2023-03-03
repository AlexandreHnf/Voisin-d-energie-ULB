__title__ = "compute_power"
__version__ = "2.0.0"
__author__ = "Alexandre Heneffe, Guillaume Levasseur, and Brice Petit"
__license__ = "MIT"


# standard library

# 3rd party packages
import argparse
import logging
import pandas as pd

# local source
from constants import (
    CASSANDRA_KEYSPACE,
    INSERTS_PER_BATCH,
    TBL_POWER,
    TBL_RAW
)
import py_to_cassandra as ptc
from utils import (
    get_dates_between,
    get_last_registered_config
)


# ====================================================================================
# called by syncRawFluksoData.py just after writing raw data to cassandra
# ====================================================================================


def save_home_power_data_to_cassandra(hid, cons_prod_df, config):
    """
    save power flukso data to cassandra : P_cons, P_prod, P_tot
    - home : Home object
        => contains cons_prod_df : timestamp, P_cons, P_prod, P_tot
    """

    try:
        insertion_time = pd.Timestamp.now(tz="CET")
        config_id = config.get_config_id()

        # add date column
        cons_prod_df['date'] = cons_prod_df.apply(lambda row: str(row.name.date()), axis=1)
        by_day_df = cons_prod_df.groupby("date")  # group by date

        col_names = [
            "home_id",
            "day",
            "ts",
            "p_cons",
            "p_prod",
            "p_tot",
            "insertion_time",
            "config_id"
        ]
        for date, date_rows in by_day_df:  # loop through each group (each date group)

            insert_queries = ""
            nb_inserts = 0
            for timestamp, row in date_rows.iterrows():
                # [:-1] to avoid date column
                values = [hid, date, timestamp] + list(row)[:-1] + [insertion_time, config_id]
                insert_queries += ptc.get_insert_query(
                    CASSANDRA_KEYSPACE,
                    TBL_POWER,
                    col_names,
                    values
                )

                if (nb_inserts + 1) % INSERTS_PER_BATCH == 0:
                    ptc.batch_insert(insert_queries)
                    insert_queries = ""

                nb_inserts += 1
            ptc.batch_insert(insert_queries)
    except Exception:
        logging.critical(
            "Exception occured in 'save_home_power_data_to_cassandra' : {}".format(hid),
            exc_info=True
        )


# =====================================================================================
# called whenever we want to modify the power table with time interval
# =====================================================================================


def get_home_raw_data(home, date):
    """
    Function to query raw data in the database according to a day.

    :param home:    Dataframe with the home configuration.
    :param date:    The date to recover data.

    :return:        Return a concatenated dataframe with all sensors.
                    columns -> ["sensor_id, day, ts, power"].
    """
    home_raw_data = []
    for sid in home.index:
        raw_data_df = ptc.select_query(
            CASSANDRA_KEYSPACE,
            TBL_RAW,
            ["sensor_id, day, ts, power"],
            f"sensor_id = '{sid}' and day = '{date}'"
        )
        home_raw_data.append(raw_data_df)
    return pd.concat(home_raw_data)


def get_consumption_production_df(raw_df, sensors_config):
    """
    P_cons = P_tot - P_prod
    P_net = P_prod + P_cons
    cons_prod_df : timestamp, P_cons, P_prod, P_tot
    """
    cons_prod_df = pd.DataFrame(
        0,
        raw_df.index,
        ["P_cons", "P_prod", "P_tot"]
    )

    for sid in raw_df.columns:
        p = sensors_config.loc[sid]["pro"]
        n = sensors_config.loc[sid]["net"]

        cons_prod_df["P_prod"] += raw_df[sid].multiply(p, fill_value=0)
        cons_prod_df["P_tot"] += raw_df[sid].multiply(n, fill_value=0 if n == 0 else None)

    cons_prod_df["P_cons"] = cons_prod_df["P_tot"] - cons_prod_df["P_prod"]

    cons_prod_df = cons_prod_df.round(1)  # round all column values with 2 decimals

    return cons_prod_df


def get_home_consumption_production_df(home_raw_data, home_config):
    """
    compute power data from raw data (coming from cassandra 'raw' table) :
    P_cons = P_tot - P_prod
    P_net = P_prod + P_cons

    :param home_raw_data:   Dataframe with all sensors.
                            columns -> ["sensor_id, day, ts, power"].
    :param home_config:     The configuration for a specific home.

    :return:                Return a dataframe with computed power data.
    """
    grouped_sensors = home_raw_data.groupby('sensor_id')
    first_group = grouped_sensors.get_group(home_raw_data.iloc[0, 0])
    cons_prod_df = (
        pd.DataFrame(columns=['home_id', 'day', 'ts', 'P_cons', 'P_prod', 'P_tot'])
        .assign(
            day=first_group['day'],
            ts=first_group['ts'],
            home_id=home_config.iloc[0, 0],
            P_cons=0,
            P_prod=0,
            P_tot=0
        )
    )

    for sid, sensor_df in grouped_sensors:
        p = home_config.loc[sid]["pro"]
        n = home_config.loc[sid]["net"]

        cons_prod_df["P_prod"] += sensor_df["power"].multiply(p, fill_value=0)
        cons_prod_df["P_tot"] += sensor_df["power"].multiply(n, fill_value=0 if n == 0 else None)

    cons_prod_df["P_cons"] = cons_prod_df["P_tot"] - cons_prod_df["P_prod"]

    cons_prod_df = cons_prod_df.round(1)  # round all column values with 1 decimals

    return cons_prod_df


def save_recomputed_powers_to_cassandra(new_config_id, cons_prod_df):
    """
    Save the powers (P_cons, P_prod, P_tot) of the raw data
    of some period of time in Cassandra for 1 specific home
    - cons_prod_df : home_id, day, ts, p_cons, p_prod, p_tot

    We assume that the data is of 1 specific date.
    """
    insertion_time = pd.Timestamp.now(tz="CET")
    col_names = [
        "home_id",
        "day",
        "ts",
        "p_cons",
        "p_prod",
        "p_tot",
        "config_id",
        "insertion_time"
    ]
    insert_queries = ""
    nb_inserts = 0

    for _, row in cons_prod_df.iterrows():
        values = list(row)
        values.append(new_config_id)
        values.append(insertion_time)
        insert_queries += ptc.get_insert_query(
            CASSANDRA_KEYSPACE,
            TBL_POWER,
            col_names,
            values
        )
        if (nb_inserts + 1) % INSERTS_PER_BATCH == 0:
            ptc.batch_insert(insert_queries)
            insert_queries = ""

        nb_inserts += 1

    ptc.batch_insert(insert_queries)


def get_data_dates_from_home(sensors_df):
    """
    From a home, query the first date in the raw data.
    then, return all dates between the first date and now.
    """
    now = pd.Timestamp.now()
    first_date = now
    for sensor_id in list(sensors_df.index):  # sensor ids
        first_date_df = ptc.select_query(
            CASSANDRA_KEYSPACE,
            TBL_RAW,
            ["day"],
            "sensor_id = '{}'".format(sensor_id),
            limit=1
        )
        if len(first_date_df) > 0:
            if pd.Timestamp(first_date_df.iat[0, 0]) < first_date:
                first_date = pd.Timestamp(first_date_df.iat[0, 0])

    all_dates = get_dates_between(first_date, now)
    return all_dates


def exist_home_power_data(home_id, date):
    """
    From a home, query the first date in the raw data.
    return True if there is at least 1 row of data for this home,
    given the specified date.
    """
    first_date_df = ptc.select_query(
        CASSANDRA_KEYSPACE,
        TBL_POWER,
        ["p_cons"],
        "home_id = '{}' and day = '{}'".format(home_id, date),
        limit=1
    )
    return len(first_date_df) > 0


def recompute_power_data(last_config, dates=None):
    """
    Given a configuration, recompute all power data for all homes
    based on the existing raw data stored in Cassandra.

    :param last_config: Configuration file.
    :param dates:       List of dates.
    """
    # Get a dataframe of the new configuration file and we groupby home_id.
    for hid, home_config in last_config.get_sensors_config().groupby("home_id"):
        # first select all dates registered for this home
        if dates is None:
            dates = get_data_dates_from_home(home_config)

        if len(dates) > 0:
            # then, for each day, recompute data and store it in the database
            # (overwrite existing data)
            for date in dates:
                if exist_home_power_data(hid, date):
                    home_raw_data = get_home_raw_data(home_config, date)
                    home_powers = get_home_consumption_production_df(home_raw_data, home_config)
                    # save (overwrite) to cassandra table
                    if len(home_powers) > 0:
                        save_recomputed_powers_to_cassandra(
                            last_config.get_config_id(), home_powers
                        )
                else:
                    logging.debug(f"No data for the date {date}")
        else:
            logging.debug("No date to process")


# ====================================================================================


def process_arguments():
    """
    Function to process arguments.

    :return:    Return ArgumentParser.
    """

    argparser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    argparser.add_argument(
        "-d",
        "--daily",
        action='store_true',
        help="""
        By adding this argument, the script will recompute yesterday's power data for each house in
        the database.
        """
    )

    return argparser


def main():
    argparser = process_arguments()
    args = argparser.parse_args()
    last_config = get_last_registered_config()
    # If the configuration exists
    if last_config:
        # Check if we want to do a daily recomputation or not
        if args.daily:
            recompute_power_data(last_config, [(pd.Timestamp.now() - pd.Timedelta(days=1)).date()])
        else:
            recompute_power_data(last_config)
    else:
        logging.debug("No registered config in db.")


if __name__ == "__main__":
    main()
