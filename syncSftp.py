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
import sys

import paramiko


SFTP_HOST = 						"repo.memoco.eu"
SFTP_PORT = 						3584
DESTINATION_PATH = 					"/upload/"
LOCAL_PATH = 						"output/fluksoData/sftp_data/"
SFTP_CREDENTIALS_FILE = 			"sftp_credentials.json"
DELETE_LOCAL_FILES = 				False
AM = 								"<"
PM = 								">"


def getdateToQuery(now):
	""" 
	Based on the timestamp of the moment the query is triggered, 
	determine which day to query
	"""
	midnight = pd.Timestamp("00:00:00")
	nb_hours_today = round((now-midnight).total_seconds() / (60.0*60.0), 1)

	date = str(now.date()) # if we are in the PM part, we take data from the first half of today
	moment = "<"  # first half : AM TODO : <= ?
	default_moment = PM  # the moment of today

	if nb_hours_today <= 12.0:  # AM
		# we are in the AM part, so we take data from the second half of the previous day
		date = str(now.date() - timedelta(days=1))  # previous day
		moment = ">"  # second half : PM 
		default_moment = AM
	
	return date, moment, default_moment


def getCsvFilename(home_id, date, moment):
	part = "AM" if moment == "<" else "PM"
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
		print(config_ids)

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
	

def listFilesSFTP():
	""" 
	return the list of 
	"""
	transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))

	sftp_filenames = []
	with open(SFTP_CREDENTIALS_FILE) as json_file:
		cred = json.load(json_file)
		transport.connect(username = cred["username"], password = cred["password"])
		sftp = paramiko.SFTPClient.from_transport(transport)

		for filename in sftp.listdir('/upload/'):
			sftp_filenames.append(filename)
			print(filename)

	sftp.close()
	transport.close()

	return sftp_filenames

def getLastDate():
	""" 
	Get the last filename sent to the sftp server in order
	to know which date to start the new query from.
	"""

	latest = 0
	latest_file = None
	latest_date = None

	transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))

	with open(SFTP_CREDENTIALS_FILE) as json_file:
		cred = json.load(json_file)
		transport.connect(username = cred["username"], password = cred["password"])
		sftp = paramiko.SFTPClient.from_transport(transport)

		for fileattr in sftp.listdir_attr('/upload/'):
			if fileattr.st_mtime > latest:
				latest = fileattr.st_mtime
				latest_file = fileattr.filename

	sftp.close()
	transport.close()

	if latest_file is not None:
		# print(latest_file)
		latest_date = pd.Timestamp(latest_file.split("_")[1])

	return latest_date


def sendFileToSFTP(filename):
	""" 
	Send csv file to the sftp server
	"""

	transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))

	dest_path = DESTINATION_PATH + filename
	local_path = LOCAL_PATH + filename

	with open(SFTP_CREDENTIALS_FILE) as json_file:
		cred = json.load(json_file)
		transport.connect(username = cred["username"], password = cred["password"])
		sftp = paramiko.SFTPClient.from_transport(transport)
		sftp.put(local_path, dest_path)

	sftp.close()
	transport.close()

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
	cols = ["day", "ts"]
	result_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, where_clause, "ALLOW FILTERING", "")

	all_dates = []
	if len(result_df) > 0:
		first_date = result_df.groupby('day').first().iloc[0]['ts']
		del result_df
		print("first date : ", first_date)

		all_dates = getDatesBetween(first_date, now)

	return all_dates


def processAllHomes(cassandra_session, config, default_date, moment, default_moment, now, latest_date, mode):
	""" 
	send 1 csv file per home, per day moment (AM or PM) to the sftp server
	"""

	# home_powerdata = getHomesPowerDataFromCassandra(cassandra_session, config, date, moment, TBL_POWER)

	ids = config.getIds()
	for home_id in ids.keys():
		all_dates = [default_date]  # TODO : replace default date by a list of missing dates
		moments = {default_date: [moment]}
		if mode == "history":
			all_dates = getAllHistoryDates(cassandra_session, home_id, TBL_POWER, now)
			moments = getMoments(all_dates, default_moment)
		elif mode == "realtime":
			all_dates = getDatesBetween(latest_date, now)
			moments = getMoments(all_dates, default_moment)

		for date in moments:
			for moment in moments[date]:
				csv_filename = getCsvFilename(home_id, date, moment)
				print(csv_filename)
				home_data = getHomePowerDataFromCassandra(cassandra_session, home_id, date, moment, TBL_POWER)
				# saveDataToCsv(home_data.set_index("home_id"), csv_filename)

				# sendFileToSFTP(csv_filename)



def main():	
	mode = sys.argv[1]  # 'history' or 'realtime'

	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	config = getLastRegisteredConfig(cassandra_session, TBL_SENSORS_CONFIG) # TODO : tester avec 2-3 configs
	print("config id : ", config.getConfigID())

	now = pd.Timestamp.now()
	# default_date, moment, default_moment = getdateToQuery(now)
	# print("date : ", default_date)
	# print("moment : ", moment)
	default_date = "2022-05-30"
	moment = "<"
	default_moment = PM

	latest_date = getLastDate()

	processAllHomes(cassandra_session, config, default_date, moment, default_moment, now, latest_date, mode)
	
	# for testing purpose 
	# dates = getAllHistoryDates(cassandra_session, "CDB001", TBL_POWER, now)
	# print(dates)
	# print(getMoments(dates, AM))

	# sftp_filenames = listFilesSFTP()


if __name__ == "__main__":
	main()

