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


def getdateToQuery(now):
	""" 
	Based on the timestamp of the moment the query is triggered, 
	determine which day to query
	"""
	midnight = pd.Timestamp("00:00:00")
	nb_hours_today = round((now-midnight).total_seconds() / (60.0*60.0), 1)

	date = str(now.date()) # if we are in the PM part, we take data from the first half of today
	moment = "<"  # first halt

	if nb_hours_today <= 12.0:  # AM
		# we are in the AM part, so we take data from the second half of the previous day
		date = str(now.date() - timedelta(days=1))  # previous day
		moment = ">"
	
	return date, moment


def getCsvFilename(home_id, date, moment):
	part = 1 if moment == "<" else 2
	return "{}_{}_part_{}.csv".format(home_id, date, part)
	

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
		cols = ["home_id", "day", "ts", "p_cons", "p_prod", "p_tot"]
		home_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, where_clause, "ALLOW FILTERING", "")

		homes_powerdata[home_id] = home_df

	return homes_powerdata


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

	# os.remove(local_path)


def processAllHomes(cassandra_session, config, date, moment):
	""" 
	send 1 csv file per home to the sftp server
	"""

	home_powerdata = getPowerDataFromCassandra(cassandra_session, config, date, moment, TBL_POWER)

	for home_id, power_data in home_powerdata.items():
		csv_filename = getCsvFilename(home_id, date, moment)
		print(csv_filename)
		saveDataToCsv(power_data.set_index("home_id"), csv_filename)

		sendFileToSFTP(csv_filename)


def main():	
	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	config = getLastRegisteredConfig(cassandra_session, TBL_SENSORS_CONFIG) # TODO : tester avec 2-3 configs
	print("config id : ", config.getConfigID())

	now = pd.Timestamp.now()
	# date, moment = getdateToQuery(now)
	# print("date : ", date)
	# print("moment : ", moment)
	date = "2022-05-22"
	moment = "<"

	processAllHomes(cassandra_session, config, date, moment)


if __name__ == "__main__":
	main()

