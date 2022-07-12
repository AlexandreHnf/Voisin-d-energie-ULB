__title__ = "preprocessFluksoSensors"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"


# standard library
import argparse
import sys

# 3rd party packages
import pandas as pd

# local source
from constants import (
	TBL_SENSORS_CONFIG, 
	CASSANDRA_KEYSPACE, 
	CONFIG_SENSORS_TAB, 
	CONFIG_ACCESS_TAB, 
	CONFIG_CAPTIONS_TAB
)

import pyToCassandra as ptc
from sensorConfig import Configuration
from computePower import recomputePowerData
from utils import getLastRegisteredConfig


# ==========================================================================


def getConfigDf(config_file_path, sheet_name):
	""" 
	given a config file (excel), and a sheet name within this file, 
	return a dataframe with the data
	"""
	try:
		data_df = pd.read_excel(config_file_path, sheet_name=sheet_name)
		return data_df
	except Exception as e:
		print("Error when trying to read excel file : {}. Please provide a valid Configuration file.".format(config_file_path))
		print("Exception : {}".format(str(e)))
		sys.exit(1)


def getFluksosDic(installation_ids_df):
	""" 
	Return a dictionary of the form : 
	key = flukso id, value = installation id 
	ex : {'FL08000475': 'CDB001'}
	"""
	fluksos = {}

	for i in range(len(installation_ids_df)):
		FlmId = installation_ids_df["FlmId"][i]
		installation_id = installation_ids_df["InstallationId"][i]
		# print(FlmId, installation_id)

		fluksos[FlmId] = installation_id

	# print(fluksos)
	return fluksos


def getInstallationsIds(flukso_ids, fluksos):
	""" 
	return a list of all the installations ids in the excel column, but
	only those whose fluksos are available. (an id can appear several times)
	"""
	installation_id_col = []
	for fi in flukso_ids:
		if fi in fluksos:
			installation_id_col.append(fluksos[fi])
		else:
			installation_id_col.append("unknown")

	return installation_id_col


def getCompactSensorDF(config_file_path):
	"""
	read the excel sheet containing the flukso ids, the sensors ids, the tokens
	and compact them into a simpler usable csv file
	columns : home_id, phase, flukso_id, sensor_id, token, net, con, pro
	"""
	print("Config file : ", config_file_path)
	sensors_df = getConfigDf(config_file_path, CONFIG_SENSORS_TAB)
	compact_df = pd.DataFrame(
		columns=[
			"home_id",
			"phase",
			"flukso_id",
			"sensor_id",
			"token",
			"net",
			"con",
			"pro"
		]
	)

	compact_df["home_id"] = sensors_df["InstallationId"]
	compact_df["phase"] = sensors_df["Function"]
	compact_df["flukso_id"] = sensors_df["FlmId"]
	compact_df["sensor_id"] = sensors_df["SensorId"]
	compact_df["token"] = sensors_df["Token"]
	compact_df["net"] = sensors_df["Network"]
	compact_df["con"] = sensors_df["Cons"]
	compact_df["pro"] = sensors_df["Prod"]

	compact_df.fillna(0, inplace=True)

	compact_df.sort_values(by=["home_id"])

	return compact_df


def recomputeData(cassandra_session):
	""" 
	Recompute the power data according to the latest configuration.
	"""
	new_config = getLastRegisteredConfig(cassandra_session)
	changed_homes = new_config.getSensorsConfig()["home_id"].unique()
	recomputePowerData(cassandra_session, new_config, changed_homes)


def writeSensorsConfigCassandra(cassandra_session, new_config_df, now):
	""" 
	write sensors config to cassandra table 
	"""
	col_names = [
		"insertion_time", 
		"home_id", 
		"phase", 
		"flukso_id", 
		"sensor_id", 
		"sensor_token", 
		"net", 
		"con", 
		"pro"
	]
	insertion_time = now.isoformat()

	for _, row in new_config_df.iterrows():
		values = [insertion_time] + list(row)
		ptc.insert(
			cassandra_session, 
			CASSANDRA_KEYSPACE, 
			TBL_SENSORS_CONFIG, 
			col_names, 
			values
		)

	print("Successfully inserted sensors config in table '{}'".format(TBL_SENSORS_CONFIG))


# ==========================================================================


def getInstallationCaptions(config_file_path):
	""" 
	get a dictionary with
	key : installation id (login id = home id) => generally a group
	value : caption, description
	"""
	captions_df = getConfigDf(config_file_path, CONFIG_CAPTIONS_TAB)
	captions = {}
	for i in captions_df.index:
		captions[captions_df.iloc[i]["InstallationId"]] = captions_df.iloc[i]["Caption"]
	
	return captions


def writeAccessDataCassandra(cassandra_session, config_file_path, table_name):
	""" 
	write access data to cassandra (login ids)
	"""
	login_df = getConfigDf(config_file_path, CONFIG_ACCESS_TAB)
	by_login = login_df.groupby("Login")

	col_names = ["login", "installations"]

	for login_id, installation_ids in by_login:
		values = [login_id, list(installation_ids["InstallationId"])]
		ptc.insert(
			cassandra_session, 
			CASSANDRA_KEYSPACE, 
			table_name, 
			col_names, 
			values
		)

	print("Successfully inserted access data in table '{}'".format(table_name))


def writeGroupCaptionsToCassandra(cassandra_session, config_file_path, table_name):
	""" 
	write groups (installation ids) with their captions
	"""
	captions = getInstallationCaptions(config_file_path)

	col_names = ["installation_id", "caption"]

	for installation_id, caption in captions.items():
		caption = caption.replace("'", "''")
		values = [installation_id, caption]

		ptc.insert(
			cassandra_session, 
			CASSANDRA_KEYSPACE, 
			table_name, 
			col_names, 
			values
		)
	
	print("Successfully inserted group captions in table '{}'".format(table_name))


# ==========================================================================
# Cassandra table creation
# ==========================================================================


def createTableSensorConfig(cassandra_session, table_name):
	""" 
	create a sensors config table
	"""
	cols = [
		"insertion_time TIMESTAMP", 
		"home_id TEXT", 
		"phase TEXT", 
		"flukso_id TEXT", 
		"sensor_id TEXT", 
		"sensor_token TEXT", 
		"net FLOAT", 
		"con FLOAT", 
		"pro FLOAT"
	]

	ptc.createTable(
		cassandra_session, 
		CASSANDRA_KEYSPACE, 
		table_name, 
		cols, 
		["home_id, sensor_id"], 
		["insertion_time"], 
		{"insertion_time": "DESC"}
	)


def createTableGroupsConfig(cassandra_session, table_name):
	""" 
	create a table with groups config
	group_id, list of home ids
	"""
	cols = [
		"insertion_time TIMESTAMP", 
		"group_id TEXT", 
		"homes LIST<TEXT>"
	]

	ptc.createTable(
		cassandra_session, 
		CASSANDRA_KEYSPACE, 
		table_name, 
		cols, 
		["insertion_time, group_id"], 
		[], 
		{}
	)


def createTableAccess(cassandra_session, table_name):
	"""
	create a table for the login access
	access, installations
	"""
	cols = [
		"login TEXT", 
		"installations LIST<TEXT>"
	]

	ptc.createTable(
		cassandra_session, 
		CASSANDRA_KEYSPACE, 
		table_name, 
		cols, 
		["login"], 
		[], 
		{}
	)


def createTableGroup(cassandra_session, table_name):
	""" 
	create a table for the groups captions
	installation_id, caption
	"""
	cols = [
		"installation_id TEXT",
		"caption TEXT"
	]

	ptc.createTable(
		cassandra_session, 
		CASSANDRA_KEYSPACE, 
		table_name, 
		cols, 
		["installation_id"], 
		[], 
		{}
	)


# ==========================================================================


def processConfig(cassandra_session, config_file_path, new_config_df, now):
	""" 
	Given a compact dataframe with a configuration, write
	the right data to Cassandra 
	- config in config table
	- login info in access table
	- group captions in group table
	"""

	# first, create tables if necessary (if they do not already exist)
	createTableSensorConfig(cassandra_session, "sensors_config")
	createTableAccess(cassandra_session, "access")
	createTableGroup(cassandra_session, "group")

	# > fill config tables using excel configuration file
	print("> Writing new config in cassandra...")
	writeSensorsConfigCassandra(cassandra_session, new_config_df, now)

	# write login and group ids to 'access' cassandra table
	writeAccessDataCassandra(cassandra_session, config_file_path, "access")

	# write group captions to 'group' cassandra table 
	writeGroupCaptionsToCassandra(cassandra_session, config_file_path, "group")


# ==========================================================================

def processArguments():

	argparser = argparse.ArgumentParser(
		description=__doc__,
		formatter_class=argparse.RawDescriptionHelpFormatter,
	)

	argparser.add_argument(
		"config", 
		type=str,
		help="Path to the config excel file"
	)

	return argparser


def main():
	# > arguments
	argparser = processArguments()
	args = argparser.parse_args()
	config_path = args.config

	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	# > get the useful flukso sensors data in a compact dataframe
	new_config_df = getCompactSensorDF(config_path)
	print("nb sensors : ", len(new_config_df))

	# Define the current time once for consistency of the insert time between tables.
	now = pd.Timestamp.now(tz="CET")
	
	processConfig(cassandra_session, config_path, new_config_df, now)

	# then, compare new config with previous configs and recompute data if necessary
	# print("> Recompute previous data... ")
	# recomputeData(cassandra_session)



if __name__ == "__main__":
	main()

