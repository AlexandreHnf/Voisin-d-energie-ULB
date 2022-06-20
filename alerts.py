""" 

"""

from concurrent.futures import process
from turtle import home
from constants import *
import pyToCassandra as ptc
from utils import *
from sensorConfig import Configuration

import logging
import pandas as pd
from datetime import timedelta
import json
import os
import argparse


def getHomePowerDataFromCassandra(cassandra_session, home_id, date, table_name):
	""" 
	Get power data from Power table in Cassandra
	> for 1 specific home
	> for 1 specific day
	"""

	where_clause = "home_id = {} and day = '{}'".format("'"+home_id+"'", date)
	cols = ["home_id", "day", "ts", "p_cons", "p_prod", "p_tot"]
	home_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, where_clause, "ALLOW FILTERING", "")

	return home_df


def checkMissing(cassandra_session, home_id, date, table_name):
    """ 
    Check if there are a lot of missing data in one day of power data for a home
    """
    home_df = getHomePowerDataFromCassandra(cassandra_session, home_id, date, table_name)

    count_zero = 0
    if len(home_df) > 0:

        # create column with the sum of p_cons, p_prod and p_tot columns
        # if the total is 0, then it means there is no raw data for the sensors of this home
        home_df['s'] = home_df['p_cons'] + home_df['p_prod'] + home_df['p_tot']
        count_zero = (home_df['s'] == 0).sum()  # get the number of 0 in the sum column
        
    return count_zero, len(home_df)


def checkMissing2(cassandra_session, sensor_id, table_name):
    """ 
    Check if there are a lot of missing data for each sensors of a home
    If the number of missing rows exceeds a certain threshold, then we send an alert by mail
    ex : 24 hours of missing data
    Assumption : we take the last configuration into consideration
    Obsolete : too specific with sensor ids, doesn't apply to a whole home. 
    """
    alert = False 
    where_clause = "sensor_id = '{}'".format(sensor_id)
    cols = ["sensor_id", "config_id", "start_ts", "end_ts"]
    missing_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, where_clause, "ALLOW FILTERING", "") 

    if len(missing_df) > 0:
        by_config = missing_df.groupby("config_id")
        tot_missing_duration = 0
        for config_id, missing in by_config:
            # get the duration between the start and end timestamp
            start_ts = missing.iloc[0]["start_ts"]
            end_ts = missing.iloc[0]["end_ts"]
            duration = round((end_ts - start_ts).total_seconds() / 3600.0, 2)  # in hours
            print("{} > {} -> {} = {} hours".format(sensor_id, start_ts, end_ts, duration))
            tot_missing_duration += duration 

        # if last_ts - start_ts > threshold : alert
        if tot_missing_duration >= 20:  # if above 20 hours
            alert = True

    return alert


def checkSigns(cassandra_session, home_id, date, table_name):
    """ 
    Check if the signs are coherent in power data based on 3 criterion : 
    Signs are incorrect if :
    - photovoltaic active during the night
    - negative consumption values
    - positive production values
    """
    home_df = getHomePowerDataFromCassandra(cassandra_session, home_id, date, table_name)

    # TODO : third condition : using config
    status = {"ok": True, "cons_neg": False, "prod_pos": False}
    if len(home_df) > 0:
        status["cons_neg"] = (home_df["p_cons"] < 0).any()
        status["prod_pos"] = (home_df["p_prod"] > 0).any()
        if status["cons_neg"] or status["prod_pos"]:
            status["ok"] = False

    return status


def getHomesWithMissingData(cassandra_session, config, yesterday):
    """ 
    For each home, check if power data has a lot of missing data, 
    if the percentage of missing data is non negligeable, we send an alert by email
    """
    MISSING_ALERT_THRESHOLD = 1
    to_alert = {}
    ids = config.getIds()
    for home_id, sensor_ids in ids.items():
        # print(home_id)
        nb_zeros, tot_len = checkMissing(cassandra_session, home_id, yesterday, TBL_POWER)

        percentage = 0
        if nb_zeros > 0:
            percentage = (100 * nb_zeros) / tot_len
        # print("nb 0 : {}, tot len : {}, {}%".format(nb_zeros, tot_len, percentage))
        if percentage >= MISSING_ALERT_THRESHOLD:  # if at least 80% of the rows are 0s, then alert
            to_alert[home_id] = percentage 
    
    print(to_alert)
    return to_alert


def getHomesWithIncorrectSigns(cassandra_session, config, yesterday):
    """ 
    For each home, we check if power data are correct w.r.t the signs
    If some signs are incorrect, we send an alert by email
    """
    to_alert = {}
    ids = config.getIds()
    for home_id, sensor_ids in ids.items():
        # print(home_id)
        status = checkSigns(cassandra_session, home_id, yesterday, TBL_POWER)
        # print(status)
        if not status["ok"]:
            to_alert[home_id] = status

    print(to_alert)
    return to_alert


def getYesterday(now):
    """ 
    Given the timestamp of today, get the previous day's date.
    -> YYYY-MM-DD
    """

    yesterday = now.date()-timedelta(days=1)

    return yesterday


def processArguments():
	"""
	process arguments 
	argument : sftp config filename
		-> contains host, port, username, password and destination path
	"""
	argparser = argparse.ArgumentParser(
		description=__doc__,
		formatter_class=argparse.RawDescriptionHelpFormatter,
	)
	argparser.add_argument("--m", type=str, default="missing",
						   help="sftp config file")

	return argparser


def main():

    argparser = processArguments()
    args = argparser.parse_args()
    mode = args.m

    cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

    last_config = getLastRegisteredConfig(cassandra_session)
    now = pd.Timestamp.now()
    # yesterday = getYesterday(now)  # TODO : handle multiple past dates
    yesterday = "2022-06-14"
    print("yesterday : ", yesterday)

    if mode == "missing":
        to_alert = getHomesWithMissingData(cassandra_session, last_config, yesterday)
    elif mode == "sign":
        to_alert = getHomesWithIncorrectSigns(cassandra_session, last_config, yesterday)


if __name__ == "__main__":
    main()