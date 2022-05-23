""" 
Script to send Flukso electrical data to sftp 
Author : Alexandre Heneffe.
"""

from distutils.command.config import config
from constants import *
import pyToCassandra as ptc
from utils import *
from sensorConfig import Configuration
import logging

import copy
import pandas as pd


def getLastRegisteredConfig(cassandra_session, table_name):
	""" 
	Get the last registered config based on insertion time
	"""
	all_configs_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name,
									["*"], "", "", "").groupby("insertion_time")
	
	config = None
	if len(all_configs_df) > 0:
		config_ids = list(all_configs_df.groups.keys())  # keys sorted by default
		print(config_ids)

		last_config_id = config_ids[-1]
		config = Configuration(last_config_id, all_configs_df.get_group(last_config_id).set_index("sensor_id"))

	return config		


def getPowerDataFromCassandra(cassandra_session, config, date, moment, table_name):
	""" 
	Get power data from Power table in Cassandra
	> for all homes
	> specific timings
	"""

	homes_powerdata = {}
	ids = config.getIds()
	for home_id in ids.keys():
		ts_clause = "ts {} '{} 12:00:00.000000+0000'".format(moment, date)  # all data before of after noon
		where_clause = "home_id = {} and day = '{}' and {}".format("'"+home_id+"'", date, ts_clause)
		print(where_clause)
		cols = ["home_id", "day", "ts", "p_cons", "p_prod", "p_tot"]
		home_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, where_clause, "ALLOW FILTERING", "")

		homes_powerdata[home_id] = home_df

	return homes_powerdata


def main():
	print("Hello world")
	
	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	config = getLastRegisteredConfig(cassandra_session, TBL_SENSORS_CONFIG)
	print("config id : ", config.getConfigID())

	date = "2022-05-22"
	moment = "<"
	home_powerdata = getPowerDataFromCassandra(cassandra_session, config, date, moment, TBL_POWER)
	print(home_powerdata["CDB001"])


if __name__ == "__main__":
	main()

