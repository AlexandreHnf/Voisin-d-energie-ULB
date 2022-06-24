__title__ = "preprocessFluksoSensors"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"


import pandas as pd
import numpy as np
from constants import *
import pyToCassandra as ptc
from sensorConfig import Configuration
import computePower as cp
import json
import sys
from utils import * 
import argparse


# ==========================================================================


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


def getCompactSensorDF():
	"""
	read the excel sheet containing the flukso ids, the sensors ids, the tokens
	and compact them into a simpler usable csv file
	columns : home_id, phase, flukso_id, sensor_id, token, net, con, pro
	"""
	print("Config file : ", FLUKSO_TECHNICAL_FILE)
	sensors_df = pd.read_excel(FLUKSO_TECHNICAL_FILE, sheet_name="Export_InstallationSensors")
	compact_df = pd.DataFrame(columns=["home_id",
									   "phase",
									   "flukso_id",
									   "sensor_id",
									   "token",
									   "net",
									   "con",
									   "pro"])

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


def sameSigns(home1_sensors_df, home2_sensors_df):
	""" 
	Given 2 configurations 1 and 2, check if for each sensor of 1, the signs are the same as in 2
	signs : associated to production, network and consumption : -1, 0 or 1
	"""
	res = True
	for _, sid in enumerate(home1_sensors_df.index):
		# print(sid)
		p = home1_sensors_df.loc[sid]["pro"]
		n = home1_sensors_df.loc[sid]["net"]
		c = home1_sensors_df.loc[sid]["con"]
		# print("p : {}, n : {}, c : {}".format(p, n, c))

		found = home2_sensors_df[home2_sensors_df['sensor_id'].str.contains(sid)]

		if len(found) > 0:
			pp = home2_sensors_df.loc[home2_sensors_df["sensor_id"] == sid]["pro"].iloc[0]
			nn = home2_sensors_df.loc[home2_sensors_df["sensor_id"] == sid]["net"].iloc[0]
			cc = home2_sensors_df.loc[home2_sensors_df["sensor_id"] == sid]["con"].iloc[0]
			# print("pp : {}, nn : {}, cc : {}".format(pp, nn, cc))

			if not (p == pp and n == nn and c == cc):
				res = False
				break 
	
	return res 


def sameSensorsIds(hid, home1_sensors_df, home2_sensors_df):
	""" 
	Check if the 2 homes have the same number of sensors and the same sensors ids.
	"""
	# print("---- {} ----".format(hid))
	sid_home1 = set(home1_sensors_df.index)
	sid_home2 = set(home2_sensors_df["sensor_id"].tolist())
	# print("sid prev config : ", sid_home1)
	# print("sid new config : ", sid_home2)
	# print("same sensors ids ? ", sid_home1 == sid_home2)

	return sid_home1 == sid_home2


def getHomesToRecompute(new_config_df, previous_config):
	""" 
	Get the previous configuration and check for each home if 
	- it has the same sensors ids as the new config
		- if yes, then we check a sign has changed in one of the sensors : 
			- if yes, then we have to recompute all power data for this home based on the raw data
	- else : we do not need to recompute the data
	"""

	homes_modif = []
	if previous_config is not None:
		for hid, home1_sensors_df in previous_config.getSensorsConfig().groupby("home_id"):
			home2_sensors_df = new_config_df.loc[new_config_df['home_id'] == hid]

			# same sensors ids
			if sameSensorsIds(hid, home1_sensors_df, home2_sensors_df):
				# at least one sign is different
				if not sameSigns(home1_sensors_df, home2_sensors_df):
					homes_modif.append(hid)

	return homes_modif


def recomputeData(cassandra_session, new_config_df, now):
	""" 
	compare new config with all previous config, and determine which data to recompute
	for each home
	"""

	all_configs = getAllRegisteredConfigs(cassandra_session)
	new_config = Configuration(now, new_config_df.set_index("sensor_id"))

	for i in range(len(all_configs)-1, -1, -1):
		prev_config_id = all_configs[i].getConfigID()
		print("config : ", prev_config_id)
		changed_homes = getHomesToRecompute(new_config_df, all_configs[i])
		print("homes to recompute : ", changed_homes)

		# recompute those homes
		cp.recomputePowerData(cassandra_session, prev_config_id, new_config, changed_homes, now)


def writeSensorsConfigCassandra(cassandra_session, new_config_df, now):
	""" 
	write sensors config to cassandra table 
	"""
	col_names = ["insertion_time", "home_id", "phase", "flukso_id", "sensor_id", "sensor_token", "net", "con", "pro"]
	insertion_time = str(now)[:19] + "Z"

	for _, row in new_config_df.iterrows():
		values = [insertion_time] + list(row)
		ptc.insert(cassandra_session, CASSANDRA_KEYSPACE, TBL_SENSORS_CONFIG, col_names, values)

	print("Successfully inserted sensors config in table : {}".format(TBL_SENSORS_CONFIG))


# ==========================================================================


def writeGroupsFromFluksoIDs():
	"""
	get the list of installations IDs based on the flukso sensors ID of a group
	-> save them in a txt file
	format : 1 line = 1 group = home_id1, home_id2, ... 
	"""
	installation_ids_df = pd.read_excel(FLUKSO_TECHNICAL_FILE, sheet_name="Flukso")
	available_fluksos = set(pd.read_excel(FLUKSO_TECHNICAL_FILE, sheet_name="Sensors")["FlmId"])
	fluksos = getFluksosDic(installation_ids_df)

	groups_df = pd.read_excel(FLUKSO_TECHNICAL_FILE, sheet_name="Groups")
	nb_groups = len(set(groups_df["GroupId"]))

	with open(GROUPS_FILE, "w") as gf:
		for i in range(nb_groups):
			group = ""
			grp_df = groups_df.loc[groups_df["GroupId"] == i+1]  # get group i
			for flukso in grp_df["FlmId"]:
				if flukso in available_fluksos:
					install_id = fluksos[flukso]
					# print(flukso, install_id)
					if install_id not in group:
						group += install_id + ","

			gf.write(group[:-1] + "\n")  # -1 to remove the last ","


def writeGroupsConfigCassandra(cassandra_session, table_name, now):
	""" 
	write groups config data in cassandra table 
	1 row = all the installations ids of a group (home ids)
	"""
	groups_df = pd.read_excel(FLUKSO_TECHNICAL_FILE, sheet_name="Groups")
	nb_groups = len(set(groups_df["GroupId"]))

	cols = ["insertion_time", "group_id", "homes"]
	insertion_time = str(now)[:19] + "Z"

	for i in range(nb_groups):
		home_ids = []
		grp_df = groups_df.loc[groups_df["GroupId"] == i+1]  # get group i
		for install_id in grp_df["InstalationId"]:
			if install_id not in home_ids:
				home_ids.append(install_id)

		values = [insertion_time, str(i+1), home_ids]
		ptc.insert(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, values)

	print("Successfully inserted groups stats in table : {}".format(table_name))


def writeGroupsFromInstallationsIds():
	"""
	get the list of installations IDs from the excel file and save them into a simple
	csv file
	1 row = all the installations ids of a group
	"""
	groups_df = pd.read_excel(FLUKSO_TECHNICAL_FILE, sheet_name="Groups")
	nb_groups = len(set(groups_df["GroupId"]))

	with open(GROUPS_FILE, "w") as gf:
		for i in range(nb_groups):
			group = ""
			grp_df = groups_df.loc[groups_df["GroupId"] == i+1]  # get group i
			for install_id in grp_df["InstalationId"]:
				if install_id not in group:
					group += install_id + ","

			# print(group[:-1])
			gf.write(group[:-1] + "\n")  # -1 to remove the last ","


def saveToCsv(df, filename):
	df.to_csv(filename, index=None, header=True)


# ==========================================================================


def correctPhaseSigns(sensors_df=None):
	"""
	functions to modify the signs of different phases (defined in a txt file of the form
	installation_ID:phase1,phase2...)
	and write the corrected content in another csv file
	"""
	to_modif = {}
	with open(PHASE_TO_MODIF_FILE) as f:
		for lign in f:
			l = lign.split(":")
			to_modif[l[0]] = l[1].strip().split(",")

	print(to_modif)

	# -----------------------------------------------------------------------

	# reading the csv file
	# if sensors_df is None:
	#     sensors_df = pd.read_csv(COMPACT_SENSOR_FILE)

	for i in range(len(sensors_df)):
		hid = sensors_df.loc[i, "home_id"]
		if hid in to_modif:
			phase = sensors_df.loc[i, "phase"]
			if ("-" in phase and phase[:-1] in to_modif[hid]) or (phase in to_modif[hid]):
				# change phase name
				if phase[-1] == "-":
					sensors_df.loc[i, "phase"] = phase[:-1]
				elif phase[-1] != "-":
					sensors_df.loc[i, "phase"] = phase + "-"
				# change net sign
				sensors_df.loc[i, "net"] = -1 * sensors_df.loc[i, "net"]
				# change con sign
				sensors_df.loc[i, "con"] = -1 * sensors_df.loc[i, "con"]
				# change pro sign
				sensors_df.loc[i, "pro"] = -1 * sensors_df.loc[i, "pro"]

	# writing into the file
	# sensors_df.to_csv(UPDATED_FLUKSO_TECHNICAL_FILE, index=False)

	return sensors_df


def getInstallationCaptions():
	""" 
	get a dictionary with
	key : installation id (login id = home id) => generally a group
	value : caption, description
	"""
	captions_df = pd.read_excel(FLUKSO_TECHNICAL_FILE, sheet_name="InstallationCaptions")
	captions = {}
	for i in captions_df.index:
		captions[captions_df.iloc[i]["InstallationId"]] = captions_df.iloc[i]["Caption"]
	
	return captions


def writeAccessDataCassandra(cassandra_session, table_name):
	""" 
	write access data to cassandra (login ids)
	"""
	login_df = pd.read_excel(FLUKSO_TECHNICAL_FILE, sheet_name="Export_Access")
	by_login = login_df.groupby("Login")

	col_names = ["login", "installations"]

	for login_id, installation_ids in by_login:
		values = [login_id, list(installation_ids["InstallationId"])]
		ptc.insert(cassandra_session, CASSANDRA_KEYSPACE, table_name, col_names, values)

	print("Successfully inserted access data in table : {}".format(table_name))


def writeGroupCaptionsToCassandra(cassandra_session, table_name):
	""" 
	write groups (installation ids) with their captions
	"""
	captions = getInstallationCaptions()

	col_names = ["installation_id", "caption"]

	for installation_id, caption in captions.items():
		caption = caption.replace("'", "''")
		values = [installation_id, caption]

		ptc.insert(cassandra_session, CASSANDRA_KEYSPACE, table_name, col_names, values)
	
	print("Successfully inserted group captions in table : {}".format(table_name))


# ==========================================================================
# Cassandra table creation
# ==========================================================================


def createTableSensorConfig(cassandra_session, table_name):
	""" 
	create a sensors config table
	"""
	cols = ["insertion_time TIMESTAMP", 
			"home_id TEXT", 
			"phase TEXT", 
			"flukso_id TEXT", 
			"sensor_id TEXT", 
			"sensor_token TEXT", 
			"net FLOAT", 
			"con FLOAT", 
			"pro FLOAT"]
	ptc.createTable(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, 
		["insertion_time, home_id, sensor_id"], [], {})


def createTableGroupsConfig(cassandra_session, table_name):
	""" 
	create a table with groups config
	group_id, list of home ids
	"""
	cols = ["insertion_time TIMESTAMP", 
			"group_id TEXT", 
			"homes LIST<TEXT>"]
	ptc.createTable(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, ["insertion_time, group_id"], [], {})


def createTableAccess(cassandra_session, table_name):
	"""
	create a table for the login access
	access, installations
	"""
	cols = ["login TEXT", 
			"installations LIST<TEXT>",
			"caption TEXT"]
	ptc.createTable(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, ["login"], [], {})


def createTableGroup(cassandra_session, table_name):
	""" 
	create a table for the groups captions
	installation_id, caption
	"""
	cols = ["installation_id TEXT",
			"caption TEXT"]
	ptc.createTable(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, ["installation_id"], [], {})


def createRawFluksoTable(cassandra_session, table_name):
	""" 
	compact df : home_id,phase,flukso_id,sensor_id,token,net,con,pro
	create a cassandra table for the raw flukso data : 
		columns : flukso_sensor_id, day, timestamp, insertion_time, config_id, power_value 
	"""

	columns = ["sensor_id TEXT", 
			   "day TEXT", 
			   "ts TIMESTAMP", 
			   "insertion_time TIMESTAMP", 
			   "config_id TIMESTAMP",
			   "power FLOAT"]
	ptc.createTable(cassandra_session, CASSANDRA_KEYSPACE, table_name, columns, ["sensor_id, day"], ["ts"], {"ts":"ASC"})


def createPowerTable(cassandra_session, table_name):

	power_cols = ["home_id TEXT", 
				  "day TEXT", 
				  "ts TIMESTAMP", 
				  "P_cons FLOAT", 
				  "P_prod FLOAT", 
				  "P_tot FLOAT", 
				  "insertion_time TIMESTAMP",
				  "config_id TIMESTAMP",]
	ptc.createTable(cassandra_session, CASSANDRA_KEYSPACE, table_name, power_cols, ["home_id, day"], ["ts"], {"ts":"ASC"})


def createRawMissingTable(cassandra_session, table_name):
	""" 
	Raw missing table contains timestamps range where there is missing data
	from a specific query given a specific configuration of the sensors 
	"""

	cols = ["sensor_id TEXT", 
			"config_id TIMESTAMP",
			"start_ts TIMESTAMP",
			"end_ts TIMESTAMP"]
	ptc.createTable(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, 
					["sensor_id, config_id"], ["start_ts"], {"start_ts":"ASC"})


# ==========================================================================


def saveHomeIds(new_config_df):
	""" 
	Take the compact df of the form "home_id,phase,flukso_id,sensor_id,token,net,con,pro"
	and save the ids for each home :
	[{hid: home_id1, phases: [flukso_id1_phase1, ..., flukso_id1_phaseN]}, 
	 {hid: home_id2, phases: [...]}, ...}
	"""
	ids = {}
	for hid, phase, fid, sid, t, n, c, p in new_config_df.values:
		if hid not in ids:
			ids[hid] = ["{}_{}".format(fid, phase)]
		else:
			ids[hid].append("{}_{}".format(fid, phase))

	print(ids)

	with open(IDS_FILE, "w") as f:
		json.dump(ids, f, indent = 4, sort_keys=True)


def saveGroupsIds():
	""" 
	Save groups ids in a json file 
	{"group1": [home_id1, home_id2, ...], ...}
	"""
	groups_ids = {} 
	with open(GROUPS_FILE) as f:
		lines = f.readlines()
		for i, line in enumerate(lines):
			groups_ids["group" + str(i+1)] = line.strip().split(",")

	with open(GIDS_FILE, "w") as f:
		json.dump(groups_ids, f, indent = 4, sort_keys=True)


# ==========================================================================

def processArguments():

	argparser = argparse.ArgumentParser(
		description=__doc__,
		formatter_class=argparse.RawDescriptionHelpFormatter,
	)

	argparser.add_argument("--table_name", type=str, default="",
						   help="access, sensors_config, raw, power, raw_missing, group")

	argparser.add_argument("--task", type=str, default="new_config",
						   help="create_table : create new table in cassandra : requires --table_name; "
						   		"new_config : write new config to cassandra; "
                                "login_config : write login info to cassandra; "
								"group_captions : write group captions to cassandra; ")

	return argparser


def main():
	# > arguments
	argparser = processArguments()
	args = argparser.parse_args()
	task = args.task
	table_name = args.table_name

	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	# > get the useful flukso sensors data in a compact csv
	new_config_df = getCompactSensorDF()
	print("nb sensors : ", len(new_config_df))

	now = pd.Timestamp.now()

	if task == "create_table":
		# > create cassandra tables
		if table_name == "access":
			createTableAccess(cassandra_session, "access") 						# access
		elif table_name == "sensors_config":
			createTableSensorConfig(cassandra_session, "sensors_config")		# sensors config
		elif table_name == "raw":
			createRawFluksoTable(cassandra_session, "raw")						# raw
		elif table_name == "power":
			createPowerTable(cassandra_session, "power")						# power
		elif table_name == "raw_missing":
			createRawMissingTable(cassandra_session, "raw_missing")				# raw_missing
		elif table_name == "group":
			createTableGroup(cassandra_session, "group")
	
	elif task == "new_config":
		# first compare new config with previous configs and recompute data if necessary
		print("> Checking for data to recompute... ")
		recomputeData(cassandra_session, new_config_df, now)

		# > fill config tables using excel configuration file
		print("> Writing new config in cassandra...")
		writeSensorsConfigCassandra(cassandra_session, new_config_df, now)

	elif task == "login_config":
		# write login and group ids to 'access' cassandra table
		writeAccessDataCassandra(cassandra_session, "access")
	
	elif task == "group_captions":
		# write group captions to 'group' cassandra table 
		writeGroupCaptionsToCassandra(cassandra_session, "group")


if __name__ == "__main__":
	main()
