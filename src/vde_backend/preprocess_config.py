__title__ = "preprocess_config"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe, and Guillaume Levasseur"
__license__ = "MIT"


# standard library
import argparse
import sys

# 3rd party packages
import pandas as pd

# local source
from constants import (
	TBL_POWER,
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


def get_sheet_data(config_file_path, sheet_name):
	""" 
	given a config file (excel), and a sheet name within this file, 
	return a dataframe with the data
	"""
	try:
		data_df = pd.read_excel(config_file_path, sheet_name=sheet_name)
		return data_df
	except Exception as e:
		print("Error when trying to read excel file : {}. ", end="")
		print("Please provide a valid Configuration file.".format(config_file_path))
		print("Exception : {}".format(str(e)))
		sys.exit(1)


def get_fluksos_dic(installation_ids_df):
	""" 
	Return a dictionary of the form : 
	key = flukso id, value = installation id 
	ex : {'FL08000475': 'CDB001'}
	"""
	fluksos = {}

	for i in range(len(installation_ids_df)):
		FlmId = installation_ids_df["FlmId"][i]
		installation_id = installation_ids_df["InstallationId"][i]

		fluksos[FlmId] = installation_id

	return fluksos


def get_installations_ids(flukso_ids, fluksos):
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


def get_config_df(config_file_path, index_name=""):
	"""
	read the excel sheet containing the flukso ids, the sensors ids, the tokens
	and compact them into a simpler usable dataframe
	columns : home_id, phase, flukso_id, sensor_id, token, net, con, pro
	"""
	sensors_df = get_sheet_data(config_file_path, CONFIG_SENSORS_TAB)
	config_df = pd.DataFrame(
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

	config_df["home_id"] = 		sensors_df["InstallationId"]
	config_df["phase"] = 		sensors_df["Function"]
	config_df["flukso_id"] = 	sensors_df["FlmId"]
	config_df["sensor_id"] = 	sensors_df["SensorId"]
	config_df["token"] = 		sensors_df["Token"]
	config_df["net"] = 			sensors_df["Network"]
	config_df["con"] = 			sensors_df["Cons"]
	config_df["pro"] = 			sensors_df["Prod"]

	config_df.fillna(0, inplace=True)

	config_df.sort_values(by=["home_id"])

	if index_name:
		config_df = config_df.set_index(index_name)

	return config_df


def recompute_data(cassandra_session):
	""" 
	Recompute the power data according to the latest configuration.
	"""
	new_config = getLastRegisteredConfig(cassandra_session)
	changed_homes = new_config.getSensorsConfig()["home_id"].unique()
	recomputePowerData(cassandra_session, new_config, changed_homes)


def write_sensors_config_cassandra(cassandra_session, new_config, now):
	""" 
	write sensors config to cassandra table 
	"""
	col_names = [
		"insertion_time", 
		"sensor_id", 
		"home_id", 
		"phase", 
		"flukso_id", 
		"sensor_token", 
		"net", 
		"con", 
		"pro"
	]
	insertion_time = now.isoformat()

	for sensor_id, row in new_config.getSensorsConfig().iterrows():
		values = [insertion_time] + [sensor_id] + list(row)
		ptc.insert(
			cassandra_session, 
			CASSANDRA_KEYSPACE, 
			TBL_SENSORS_CONFIG, 
			col_names, 
			values
		)

	print("Successfully inserted sensors config in table '{}'".format(TBL_SENSORS_CONFIG))


# ==========================================================================


def get_installation_captions(config_file_path):
	""" 
	get a dictionary with
	key : installation id (login id = home id) => generally a group
	value : caption, description
	"""
	captions_df = get_sheet_data(config_file_path, CONFIG_CAPTIONS_TAB)
	captions = {}
	for i in captions_df.index:
		captions[captions_df.iloc[i]["InstallationId"]] = captions_df.iloc[i]["Caption"]
	
	return captions


def write_access_data_cassandra(cassandra_session, config_file_path, table_name):
	""" 
	write access data to cassandra (login ids)
	"""
	login_df = get_sheet_data(config_file_path, CONFIG_ACCESS_TAB)
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


def write_group_captions_cassandra(cassandra_session, config_file_path, table_name):
	""" 
	write groups (installation ids) with their captions
	"""
	captions = get_installation_captions(config_file_path)

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


def create_table_sensor_config(cassandra_session, table_name):
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
		["home_id", "sensor_id"], 
		["insertion_time"], 
		{"insertion_time": "DESC"}
	)


def create_table_groups_config(cassandra_session, table_name):
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
		["insertion_time", "group_id"], 
		[], 
		{}
	)


def create_table_access(cassandra_session, table_name):
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


def create_table_group(cassandra_session, table_name):
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


def create_tables(cassandra_session):
	""" 
	create tables if necessary (if they do not already exist)
	"""
	create_table_sensor_config(cassandra_session, "sensors_config")
	create_table_access(cassandra_session, "access")
	create_table_group(cassandra_session, "group")


def save_config(cassandra_session, config_file_path, new_config, now):
	""" 
	Given a configuration, write
	the right data to Cassandra 
	- config in config table
	- login info in access table
	- group captions in group table
	"""

	# > fill config tables using excel configuration file
	print("> Writing new config in cassandra...")
	write_sensors_config_cassandra(cassandra_session, new_config, now)

	# write login and group ids to 'access' cassandra table
	write_access_data_cassandra(cassandra_session, config_file_path, "access")

	# write group captions to 'group' cassandra table 
	write_group_captions_cassandra(cassandra_session, config_file_path, "group")


# ==========================================================================


def compare_configs(c1_path, c1, c2_path, c2):
	""" 
	Go through the 2 config home ids and sensors ids
	and detect new changes
	TODO : count changes 
	"""

	print("--------------------------------------------------")
	print("Old config : " + (c1_path if c1_path else "Last registered config"))
	print(c1)
	print("New config : " + c2_path)
	print(c2)

	if c1:
		for hid, sids in c1.getHomeSensors().items():
			print("{} : ".format(hid), end="")
			if hid in c2.getHomeSensors():
				new_sids = c2.getHomeSensors()[hid]
				if set(sids) == set(new_sids):
					print("Same sensor ids")
				else:
					print("New sensors ids : ")
					# sensors ids from new config not present in the other config
					print([sid for sid in new_sids if sid not in sids])
			else:
				print("New home")
	print("--------------------------------------------------")


def process_configs(cassandra_session, old_config_path, new_config_path, now):
	""" 
	
	"""
	save = False
	new_config = Configuration(
		now,
		get_config_df(new_config_path, "sensor_id")
	)

	if not old_config_path:
		last_config = getLastRegisteredConfig(cassandra_session)
		if not last_config:
			# no comparisons
			save = True
		else:
			compare_configs(old_config_path, last_config, new_config_path, new_config)
			save = True 
	else:
		# just compare 2 configurations from 2 files
		other_config = Configuration(
			now, 
			get_config_df(new_config_path, "sensor_id")
		)
		compare_configs(old_config_path, other_config, new_config_path, new_config)

	if save:
		save_config(cassandra_session, new_config_path, new_config, now)


def process_arguments():

	argparser = argparse.ArgumentParser(
		description=__doc__,
		formatter_class=argparse.RawDescriptionHelpFormatter,
	)

	# mandatory argument
	argparser.add_argument(
		"config", 
		type=str,
		help="Path to the new config excel file. Format : ConfigurationNN_YYYYmmdd.xlsx"
	)

	# optional argument
	argparser.add_argument(
		"--diff",
		type=str,
		help="Path to another config excel file. Format : ConfigurationNN_YYYYmmdd.xlsx"
	)

	return argparser


def main():
	# > arguments
	argparser = process_arguments()
	args = argparser.parse_args()
	new_config_path = args.config
	old_config_path = args.diff

	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	# Define the current time once for consistency of the insert time between tables.
	now = pd.Timestamp.now(tz="CET")

	create_tables(cassandra_session)

	process_configs(cassandra_session, old_config_path, new_config_path, now)

	"""
	# then, compare new config with previous configs and recompute data if necessary
	if (ptc.existTable(cassandra_session, CASSANDRA_KEYSPACE, TBL_POWER)):
		print("> Recompute previous data... ")
		recompute_data(cassandra_session)
	"""



if __name__ == "__main__":
	main()

