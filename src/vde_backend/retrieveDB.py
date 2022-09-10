__title__ = "retrieveDB"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"


""" 
Script to save Flukso electrical data from the Cassandra database to csv files
saved csv files : 
	1 file = power data of 1 specific home, 1 specific day
	power data = consumption, production, total.
	see 'syncRawFluksoData.py' for more details about the data.

	in automatic mode: the script automatically retrieve data from the last saved date
	for each home. If no saved file yet, then we take the history of all dates
	available in the database for each home. 
"""


# standard library
import os
import os.path
import argparse

# 3rd party packages
import pandas as pd

# local sources
from constants import (
	CASSANDRA_KEYSPACE, 
	TBL_POWER
)
import pyToCassandra as ptc

from utils import (
	getDatesBetween, 
	getLastRegisteredConfig,
	getHomePowerDataFromCassandra
)


# ==============================================================================


def saveDataToCsv(data_df, csv_filename, output_filename):
	""" 
	Save to csv
	"""

	if not os.path.exists(output_filename):
		os.mkdir(output_filename)
	filepath = os.path.join(output_filename, csv_filename)

	data_df.to_csv(filepath)

	print("Successfully Saved data in csv")	


def getLastDate(output_file, home_id):
	""" 
	Get the last filename sent to the sftp server in order
	to know which date to start the new query from.
	"""

	latest = 0
	latest_file = None
	latest_date = None

	for filename in os.listdir(output_file):
		file_time = os.path.getmtime(os.path.join(output_file, filename))
		if filename.startswith(home_id) and file_time > latest:
			latest = file_time
			latest_file = filename

	if latest_file is not None:
		latest_date = pd.Timestamp(latest_file.split("_")[1][:-4])

	return latest_date


def getAllHistoryDates(cassandra_session, home_id, table_name, now):
	""" 
	For a home, get the first timestamp available in the db, and 
	from that first date, return the list of dates until now.
	"""

	# get first date available for this home 
	where_clause = "home_id = '{}'".format(home_id)
	cols = ["day"]
	date_df = ptc.selectQuery(
		cassandra_session, 
		CASSANDRA_KEYSPACE, 
		table_name, 
		cols, 
		where_clause, 
		limit=1,
		allow_filtering=True,
		distinct=False
	)

	all_dates = []
	if len(date_df) > 0:
		first_date = pd.Timestamp(date_df.iat[0,0])

		all_dates = getDatesBetween(first_date, now)

	return all_dates


def getDates(cassandra_session, home_id, specific_days, now, output_filename):
	""" 
	Get all dates based on the chosen argument:
	- if 1 specific date :  [specific_date]
	- if date range date1 -> [start_day... end_day]
	- if no specific dates : 
		- if no data saved yet : all database history
		- if data already saved : dates between latest date and now
	"""

	all_dates = []
	if specific_days:
		if len(specific_days) == 1:
			all_dates.append(specific_days[0])
		elif len(specific_days) == 2:
			all_dates = getDatesBetween(
				pd.Timestamp(specific_days[0]), 
				pd.Timestamp(specific_days[1])
			)
	else:
		latest_date = getLastDate(output_filename, home_id)
		if latest_date is None:  # history
			all_dates = getAllHistoryDates(cassandra_session, home_id, TBL_POWER, now)
		else:					 # realtime
			all_dates = getDatesBetween(latest_date, now)

	return all_dates


def processAllHomes(cassandra_session, now, homes, specific_days, output_filename):
	""" 
	save 1 csv file per home, per day
	- if no data sent for this home yet, we send the whole history
	- otherwise, we send data from the last sent date to now
	"""

	for home_id in homes:
		all_dates = getDates(cassandra_session, home_id, specific_days, now, output_filename)

		for date in all_dates:
			csv_filename = "{}_{}.csv".format(home_id, date)
			print(csv_filename)
			home_data = getHomePowerDataFromCassandra(
				cassandra_session, 
				home_id, 
				date
			)
			
			saveDataToCsv(home_data.set_index("home_id"), csv_filename, output_filename)

		print("-----------------------")


def getHomes(config, specific_home):
	""" 
	if specific home chosen in arguments : only 1 home
	else : all homes from the config
	"""
	homes = []
	if specific_home != "":
		homes.append(specific_home)
	else:
		homes = config.getIds().keys()

	return homes


def processArguments():
	"""
	process arguments 

	The purpose of this script is to dump csv from the database in many ways :
		- real time (get database history if no data saved yet, or get data between last saved date and now)
		- specific day of data
		- now also a specific home id and a specific date range
	
	arguments : 
		- sftp config filename (mandatory)
			-> contains host, port, username, password and destination path
		- home (optional): specific home id
		- day (optional): specific day
		- date_range (optional) : range between 2 specific dates
	"""
	argparser = argparse.ArgumentParser(
		description=__doc__,
		formatter_class=argparse.RawDescriptionHelpFormatter,
	)
	argparser.add_argument(
		"output_filename", 
		type=str, 
		help="output filename"
	)

	argparser.add_argument(
		"--home",
		type=str,
		default="",
		required=False,
		help="home id"
	)

	argparser.add_argument(
		"--day",
		type=str,
		default="",
		required=False,
		help="day in YYYY_MM_DD format"
	)

	argparser.add_argument(
		"--start",
		type=str,
		help="start day. Format : YYYY-MM-DD"
	)

	argparser.add_argument(
		"--end",
		type=str,
		help="end day. Format : YYYY-MM-DD"
	)

	return argparser


def main():

	argparser = processArguments()
	args = argparser.parse_args()
	output_filename = args.output_filename
	specific_home = args.home
	specific_day = args.day
	start_day = args.start
	end_day = args.end

	# TODO : check the arguments validity + which arguments can work together
	specific_days = []
	if start_day and end_day:
		specific_days = [start_day, end_day]
	if args.day:
		specific_days.append(args.day)

	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	config = getLastRegisteredConfig(cassandra_session)

	now = pd.Timestamp.now()

	print("config id : " + str(config.getConfigID()))
	print("specific home : " + ("/" if not specific_home else specific_home))
	print("specific range : " + ("/" if not start_day else "{} -> {}".format(start_day, end_day)))
	print("specific day : " + ("/" if not specific_day else specific_day))

	homes = getHomes(config, specific_home)

	processAllHomes(
		cassandra_session, 
		now, 
		homes,
		specific_days,
		output_filename
	)


if __name__ == "__main__":
	main()

