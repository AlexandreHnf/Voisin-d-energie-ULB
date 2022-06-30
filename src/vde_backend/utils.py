__title__ = "utils"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"


# standard library
import os
import math
from datetime import date, timedelta, datetime
from tracemalloc import start

# 3rd party packages
import pandas as pd
import numpy as np
import logging

# local sources
from constants import (
	CASSANDRA_KEYSPACE, 
	FREQ, 
	LAST_TS_DAYS, 
	LOG_LEVEL,
	TBL_SENSORS_CONFIG
)
from sensorConfig import Configuration
import pyToCassandra as ptc


# ========================================================================================



def getProgDir():
	import __main__
	main_path = os.path.abspath(__main__.__file__)
	main_path = os.path.dirname(main_path) + os.sep
	return main_path


def read_sensor_info(path, sensor_file):
	"""
	read csv file of sensors data
	"""
	path += sensor_file
	sensors = pd.read_csv(path, header=0, index_col=1)
	return sensors
	

def getCurrentSensorsConfigCassandra(cassandra_session, table_name):
	""" 
	Get the last registered sensors configuration
	sensors configurations columns : home ids, sensors ids, tokens, indices for each phase (net, pro, con)
	method : get all rows of the table, groupby config id (sorted by dates) and pick the last one
	
	POTENTIAL ISSUE : if the whole config table does not fit in memory
		-> possible solution : select distinct insertion_time, home_id, sensor_id to reduce the nb of queried lines
	"""

	all_configs_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name, ["*"], 
				"", "allow filtering", "")
	
	by_date_df = all_configs_df.groupby("insertion_time")
	dates = list(by_date_df.groups.keys())
	current_config_id = dates[-1]   # last config registered

	return current_config_id, by_date_df	


def getLastRegisteredConfig(cassandra_session):
	"""
	Get the last registered config based on insertion time
	"""
	first_row = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, TBL_SENSORS_CONFIG,
									["insertion_time"], "", "", "LIMIT 1")

	last_config_id = first_row.iat[0,0]
	config_df = ptc.selectQuery(
		cassandra_session,
		CASSANDRA_KEYSPACE,
		TBL_SENSORS_CONFIG,
		["*"],
		"insertion_time = '{}.000000+0000'".format(last_config_id),
		"ALLOW FILTERING",
		""
	)
	config = Configuration(last_config_id, config_df.set_index("sensor_id"))
	return config


def getAllRegisteredConfigs(cassandra_session):
	""" 
	Get all configs present in the system
	returns a list of config ids

	POTENTIAL ISSUE : if the whole config table does not fit in memory
		-> possible solution : select distinct insertion_time, home_id, sensor_id to reduce the nb of queried lines
	"""
	all_configs_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, TBL_SENSORS_CONFIG,
									["*"], "", "", "")

	configs = []
	if len(all_configs_df) > 0:
		for config_id, config in all_configs_df.groupby("insertion_time"):
			# print("config ", config_id)
			# print(config)

			configs.append(Configuration(config_id, config.set_index("sensor_id")))

	return configs


def getDatesBetween(start_date, end_date):
	""" 
	get the list of dates between 2 given dates
	"""
	d = pd.date_range(start_date.date(), end_date.date()-timedelta(days=1), freq='d')
	dates = []
	for ts in d:
		dates.append(str(ts.date()))
	dates.append(str(end_date.date()))

	return dates


def toEpochs(time):
	return int(math.floor(time.value / 1e9))


def isEarlier(ts1, ts2):
	""" 
	check if timestamp 'ts1' is earlier/older than timestamp 'ts2'
	"""
	return (ts1 - ts2) / np.timedelta64(1, 's') < 0


def getSpecificSerie(value, since_timing, to_timing):

	period = (to_timing - since_timing).total_seconds() / FREQ[0]
	# logging.info("s : {}, t : {}, to : {}, period : {}".format(self.since_timing, self.to_timing, to, period))
	values = pd.date_range(since_timing, periods=period, freq=str(FREQ[0]) + FREQ[1])
	# logging.info("datetime range : " + values)
	values_series = pd.Series(int(period) * [value], values)
	# logging.info(since_timing, values_series.index[0])

	return values_series


def energy2power(energy_df):
	"""
	from cumulative energy to power (Watt)
	"""
	# logging.info(energy_df.head(10))
	power_df = energy_df.diff() * 1000
	power_df.fillna(0, inplace=True)
	
	power_df = power_df.resample(str(FREQ[0]) + FREQ[1]).mean()

	return power_df


def getTimeSpent(time_begin, time_end):
	""" 
	Get the time spent in seconds between 2 timings (1 timing = time.time())
	"""
	return timedelta(seconds=time_end - time_begin)


def setupLogLevel():
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


def main():
	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)
	config = getLastRegisteredConfig(cassandra_session)
	print(config.getSensorsConfig())


if __name__ == '__main__':
	main()
