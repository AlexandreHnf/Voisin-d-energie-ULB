""" 
Script to send Flukso electrical data to sftp 
Author : Alexandre Heneffe.

Constraints : 
	- the data is not always complete when querying at a certain time for 2 reasons: 
		1) because of missing data that may be recovered after an undetermined amount of time by the system. 
		2) if the backend system used to populate the database with data is currently executing a query while 
		this script is running, then the 5 last minutes of data may not end up in this script's query (because
		the backend system query data every 5 minutes).

	- The script has to be executed a bit after a 'half day time' : for example : after noon, or after midnight
	because the database table works by chunk composed of 1 day of data. It also allows to have consistent 
	chunks of data that follow well. To avoid having too much delay, execute the script the closest to a half time
	as possible.

	- We save 1 csv file per home, otherwise, fusing all homes in one file can cause memory issues (since the
	number of homes can grow in time). 

	- the files already sent to the sftp server in the /upload/ folder must remain in that folder
	because the system depends on those files to determine which date to query. Or at least, the most 
	important files are the last ones for each home. 
		- solution to become independent on the server : locally save the last sent files and check locally
"""


from constants import *
import pyToCassandra as ptc
from utils import *
from sensorConfig import Configuration

import logging
import pandas as pd
from datetime import timedelta
import json
import os

import paramiko


SFTP_HOST = 						"repo.memoco.eu"
SFTP_PORT = 						3584
DESTINATION_PATH = 					"/upload/"
LOCAL_PATH = 						"output/fluksoData/sftp_data/"
SFTP_CREDENTIALS_FILE = 			"sftp_credentials.json"
DELETE_LOCAL_FILES = 				True
AM = 								"<="
PM = 								">"
VERBOSE = 							True


def getdateToQuery(now):
	""" 
	Based on the timestamp of the moment the query is triggered, 
	determine which day to query
	"""
	midnight = pd.Timestamp("00:00:00")
	nb_hours_today = round((now-midnight).total_seconds() / (60.0*60.0), 1)

	date = str(now.date()) # if we are in the PM part, we take data from the first half of today
	moment = AM  # first half : AM 
	moment_now = PM  # the moment of today

	if nb_hours_today <= 12.0:  # AM
		# we are in the AM part, so we take data from the second half of the previous day
		date = str(now.date() - timedelta(days=1))  # previous day
		moment = PM  # second half : PM 
		moment_now = AM
	
	return date, moment, moment_now


def getCsvFilename(home_id, date, moment):
	part = "AM" if moment == AM else "PM"
	return "{}_{}_{}.csv".format(home_id, date, part)
	

def saveDataToCsv(data_df, csv_filename):
	""" 
	Save to csv
	"""

	outdir = LOCAL_PATH
	if not os.path.exists(outdir):
		os.mkdir(outdir)
	filepath = os.path.join(outdir, csv_filename)

	data_df.to_csv(filepath)

	logging.info("Successfully Saved flukso data in csv")



def getLastRegisteredConfig(cassandra_session, table_name):
	""" 
	Get the last registered config based on insertion time
	"""
	all_configs_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name,
									["*"], "", "", "").groupby("insertion_time")
	
	config = None
	if len(all_configs_df) > 0:
		config_ids = list(all_configs_df.groups.keys())  # keys sorted by default
		# print(config_ids)

		last_config_id = config_ids[-1]
		config = Configuration(last_config_id, all_configs_df.get_group(last_config_id).set_index("sensor_id"))

	return config		


def getHomePowerDataFromCassandra(cassandra_session, home_id, date, moment, table_name):
	""" 
	Get power data from Power table in Cassandra
	> for 1 specific home
	> specific timings
	"""

	ts_clause = "ts {} '{} 12:00:00.000000+0000'".format(moment, date)  # all data before of after noon
	where_clause = "home_id = {} and day = '{}' and {}".format("'"+home_id+"'", date, ts_clause)
	cols = ["home_id", "day", "ts", "p_cons", "p_prod", "p_tot"]
	home_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, where_clause, "ALLOW FILTERING", "")

	return home_df


def getHomesPowerDataFromCassandra(cassandra_session, config, date, moment, table_name):
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
		cols = ["home_id", "day", "ts", "p_cons", "p_prod", "p_tot"]
		home_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, where_clause, "ALLOW FILTERING", "")

		homes_powerdata[home_id] = home_df

	return homes_powerdata
	

def getSFTPsession():
	""" 
	connect to the sftp server
	"""

	transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))

	with open(SFTP_CREDENTIALS_FILE) as json_file:
		cred = json.load(json_file)
		transport.connect(username = cred["username"], password = cred["password"])
		sftp = paramiko.SFTPClient.from_transport(transport)

	return sftp


def listFilesSFTP(sftp_session):
	""" 
	return the list of 
	"""

	sftp_filenames = []

	for filename in sftp_session.listdir('/upload/'):
		sftp_filenames.append(filename)
		print(filename)

	return sftp_filenames


def getLastDate(sftp_session, home_id):
	""" 
	Get the last filename sent to the sftp server in order
	to know which date to start the new query from.
	"""

	latest = 0
	latest_file = None
	latest_date = None

	for fileattr in sftp_session.listdir_attr('/upload/'):
		if fileattr.filename.startswith(home_id) and fileattr.st_mtime > latest:
			latest = fileattr.st_mtime
			latest_file = fileattr.filename

	if latest_file is not None:
		# print(latest_file)
		latest_date = pd.Timestamp(latest_file.split("_")[1])

	return latest_date


def sendFileToSFTP(sftp_session, filename):
	""" 
	Send csv file to the sftp server
	"""

	dest_path = DESTINATION_PATH + filename
	local_path = LOCAL_PATH + filename

	sftp_session.put(local_path, dest_path)

	if DELETE_LOCAL_FILES:
		os.remove(local_path)



def getMoments(dates, default_moment):
	""" 
	Given a list of dates, define the moments of each day to query
	one moment = AM or PM
	"""

	moments = {}
	for i in range(len(dates)):
		date = dates[i]
		if i == len(dates) - 1:  # if last date
			if default_moment == AM:
				moments[date] = []
			elif default_moment == PM:
				moments[date] = [AM]
		else:
			moments[date] = [AM, PM]

	return moments


def getAllHistoryDates(cassandra_session, home_id, table_name, now):
	""" 
	For a home, get the first timestamp available in the db, and 
	from that first date, return the list of dates until now.
	"""

	# get first date available for this home
	where_clause = "home_id = {}".format("'"+home_id+"'")
	cols = ["home_id", "day"]
	result_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, where_clause, "ALLOW FILTERING", "", "DISTINCT")

	all_dates = []
	if len(result_df) > 0:
		first_date = pd.Timestamp(list(result_df.groupby('day').groups.keys())[0])
		del result_df
		# print("first date : ", first_date)

		all_dates = getDatesBetween(first_date, now)

	return all_dates


def processAllHomes(cassandra_session, sftp_session, config, default_date, moment, moment_now, now):
	""" 
	send 1 csv file per home, per day moment (AM or PM) to the sftp server
	- if no data sent for this home yet, we send the whole history
	- otherwise, we send data from the last sent date to now
	"""

	ids = config.getIds()
	for home_id in ids.keys():
		latest_date = getLastDate(sftp_session, home_id)
		all_dates = [default_date]
		moments = {default_date: [moment]}
		if latest_date is None:  # history
			all_dates = getAllHistoryDates(cassandra_session, home_id, TBL_POWER, now)
			moments = getMoments(all_dates, moment_now)
		else:					 # realtime
			all_dates = getDatesBetween(latest_date, now)
			moments = getMoments(all_dates, moment_now)

		for date in moments:
			for moment in moments[date]:
				csv_filename = getCsvFilename(home_id, date, moment)
				if VERBOSE:
					print(csv_filename)
				home_data = getHomePowerDataFromCassandra(cassandra_session, home_id, date, moment, TBL_POWER)
				
				saveDataToCsv(home_data.set_index("home_id"), csv_filename)  # first save csv locally
				sendFileToSFTP(sftp_session, csv_filename)								 # then, send to sftp server

		if VERBOSE: 
			print("---------------------")



def main():
	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)
	sftp_session = getSFTPsession()

	config = getLastRegisteredConfig(cassandra_session, TBL_SENSORS_CONFIG) # TODO : tester avec 2-3 configs

	now = pd.Timestamp.now()
	default_date, moment, moment_now = getdateToQuery(now)

	if VERBOSE : 
		print("config id : ", config.getConfigID())
		print("date : ", default_date)
		print("moment : ", moment)
		print("moment now : ", moment_now)

	# temporary (for testing purpose)
	# default_date = "2022-05-30"
	# moment = AM
	# moment_now = PM
	# now = pd.Timestamp("2022-05-31 10:00:00")
 
	# sftp_filenames = listFilesSFTP()


	processAllHomes(cassandra_session, sftp_session, config, default_date, moment, moment_now, now)


if __name__ == "__main__":
	main()
