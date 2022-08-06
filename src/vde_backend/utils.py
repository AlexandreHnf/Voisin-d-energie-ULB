__title__ = "utils"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"


# standard library
import os
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
	TBL_SENSORS_CONFIG
)

from sensorConfig import Configuration
import pyToCassandra as ptc


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


# Create and configure logger
logging.getLogger("tmpo").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

logging.basicConfig(
	level = setupLogLevel(),
	format = "{asctime} {filename} {levelname:<8} {message}",
    style='{',
	handlers=[
		logging.handlers.TimedRotatingFileHandler(
			LOG_FILE,
			when='midnight',
			backupCount=7,
		),
	]
)


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
	sensors = pd.read_csv(
		path, 
		header=0, 
		index_col=1
	)
	return sensors


def setInitSeconds(ts):
	"""
	SS = 00 if M even, 04 if odd
	"""
	minute = ts.minute
	sec = "00"
	if minute % 2 != 0: # odd
		sec = "04"
	ts = ts.replace(second=int(sec))
	return ts


def getLastRegisteredConfig(cassandra_session):
	"""
	Get the last registered config based on insertion time
	"""
	first_row = ptc.selectQuery(
		cassandra_session, 
		CASSANDRA_KEYSPACE,
		TBL_SENSORS_CONFIG,
		["insertion_time"], 
		where_clause="", 
		limit=1,
		allow_filtering=False,
		distinct=False,
		tz="UTC"
	)
	last_config_id = first_row.iat[0,0]
	
	config_df = ptc.selectQuery(
		cassandra_session,
		CASSANDRA_KEYSPACE,
		TBL_SENSORS_CONFIG,
		["*"],
		"insertion_time = '{}'".format(last_config_id),
	)
	# print(config_df['insertion_time'].head(5))
	config = Configuration(last_config_id, config_df.set_index("sensor_id"))
	return config


def getAllRegisteredConfigs(cassandra_session):
	""" 
	Get all configs present in the system
	returns a list of config ids

	POTENTIAL ISSUE : if the whole config table does not fit in memory
		-> possible solution : select distinct insertion_time, home_id, sensor_id to reduce the nb of queried lines
	"""
	all_configs_df = ptc.selectQuery(
		cassandra_session, 
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


def getDatesBetween(start_date, end_date):
	""" 
	get the list of dates between 2 given dates
	"""
	d = pd.date_range(
		start_date.date(), 
		end_date.date()-timedelta(days=1), 
		freq='d'
	)

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
	""" 
	Get a pandas series between 2 defined timestamps 
	and filled with predefined values
	"""

	period = (to_timing - since_timing).total_seconds() / FREQ[0]
	values = pd.date_range(
		since_timing, 
		periods=period, 
		freq=str(FREQ[0]) + FREQ[1]
	)

	values_series = pd.Series(int(period) * [value], values)

	return values_series


def energy2power(energy_df):
	"""
	from cumulative energy to power (Watt)
	"""
	logging.debug(energy_df.head(10))
	power_df = energy_df.diff() * 1000
	power_df.fillna(0, inplace=True)
	
	power_df = power_df.resample(str(FREQ[0]) + FREQ[1]).mean()

	return power_df


def getTimeSpent(time_begin, time_end):
	""" 
	Get the time spent in seconds between 2 timings (1 timing = time.time())
	"""
	return timedelta(seconds=time_end - time_begin)


def main():
	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)
	config = getLastRegisteredConfig(cassandra_session)
	print(config.getSensorsConfig())


if __name__ == '__main__':
	main()
