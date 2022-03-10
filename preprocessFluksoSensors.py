from matplotlib.pyplot import table
import pandas as pd
import numpy as np
from constants import *
import pyToCassandra as ptc
import json
from utils import * 

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
    # 'Sensors' sheet
    sensors_df = pd.read_excel(FLUKSO_TECHNICAL_FILE, sheet_name="Sensors")
    compact_df = pd.DataFrame(columns=["home_ID",
                                       "phase",
                                       "flukso_id",
                                       "sensor_id",
                                       "token",
                                       "net",
                                       "con",
                                       "pro"])

    compact_df["phase"] = sensors_df["Function"]
    compact_df["flukso_id"] = sensors_df["FlmId"]
    compact_df["sensor_id"] = sensors_df["SensorId"]
    compact_df["token"] = sensors_df["Token"]
    compact_df["net"] = sensors_df["Network"]
    compact_df["con"] = sensors_df["Cons"]
    compact_df["pro"] = sensors_df["Prod"]

    compact_df.fillna(0, inplace=True)

    # 'Flukso' sheet
    installation_ids_df = pd.read_excel(FLUKSO_TECHNICAL_FILE, sheet_name="Flukso")
    fluksos = getFluksosDic(installation_ids_df)  # {flukso_id : install_id}
    installation_ids_col = getInstallationsIds(sensors_df["FlmId"], fluksos)
    # print(installation_ids_col)
    compact_df["home_ID"] = installation_ids_col

    # remove unused fluksos (those without active installations)
    compact_df = compact_df.drop(compact_df[compact_df.home_ID == "unknown"].index)
    compact_df.reset_index(inplace=True, drop=True)

    compact_df.sort_values(by=["home_ID"])

    return compact_df


def writeSensorsConfigCassandra(cassandra_session, compact_df, table_name):
    """ 
    write sensors config to cassandra table 
    """
    col_names = ["insertion_time", "home_id", "phase", "flukso_id", "sensor_id", "sensor_token", "net", "con", "pro"]
    insertion_time = str(pd.Timestamp.now())[:19] + "Z"

    for _, row in compact_df.iterrows():
        values = [insertion_time] + list(row)
        ptc.insert(cassandra_session, CASSANDRA_KEYSPACE, table_name, col_names, values)

    print("Successfully inserted sensors config in table : {}".format(table_name))


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
                    print(flukso, install_id)
                    if install_id not in group:
                        group += install_id + ","

            gf.write(group[:-1] + "\n")  # -1 to remove the last ","


def writeGroupsConfigCassandra(cassandra_session, table_name):
    """ 
    write groups config data in cassandra table 
    1 row = all the installations ids of a group (home ids)
    """
    groups_df = pd.read_excel(FLUKSO_TECHNICAL_FILE, sheet_name="Groups")
    nb_groups = len(set(groups_df["GroupId"]))

    cols = ["insertion_time", "group_id", "homes"]
    insertion_time = str(pd.Timestamp.now())[:19] + "Z"

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

            print(group[:-1])
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
        hid = sensors_df.loc[i, "home_ID"]
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


def createRawFluksoTable(cassandra_session, table_name):
	""" 
	compact df : home_ID,phase,flukso_id,sensor_id,token,net,con,pro
	create a cassandra table for the raw flukso data : 
		columns : flukso_sensor_id, day, timestamp, insertion_time, power_value 
	"""

	columns = ["sensor_id TEXT", 
			   "day TEXT", 
			   "ts TIMESTAMP", 
			   "insertion_time TIMESTAMP", 
			   "power FLOAT"]
	ptc.createTable(cassandra_session, CASSANDRA_KEYSPACE, table_name, columns, ["sensor_id, day"], ["ts"], {"ts":"ASC"})


def createPowerTable(cassandra_session, table_name):

    power_cols = ["home_id TEXT", 
				  "day TEXT", 
				  "ts TIMESTAMP", 
				  "P_cons FLOAT", 
				  "P_prod FLOAT", 
				  "P_tot FLOAT", 
				  "insertion_time TIMESTAMP"]
    ptc.createTable(cassandra_session, CASSANDRA_KEYSPACE, table_name, power_cols, ["home_id, day"], ["ts"], {"ts":"ASC"})


def createRawMissingTable(cassandra_session, table_name):
    """ 
    Raw missing table contains timestamps range where there is missing data
    from a specific query given a specific configuration of the sensors 
    """

    cols = ["sensor_id TEXT", 
            "sensors_config_time TIMESTAMP",
            "start_ts TIMESTAMP",
            "end_ts TIMESTAMP"]
    ptc.createTable(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, 
                    ["sensor_id, sensors_config_time"], ["start_ts"], {"start_ts":"ASC"})


# ==========================================================================


def saveHomeIds(compact_df):
    """ 
    Take the compact df of the form "home_ID,phase,flukso_id,sensor_id,token,net,con,pro"
    and save the ids for each home :
    [{hid: home_id1, phases: [flukso_id1_phase1, ..., flukso_id1_phaseN]}, 
     {hid: home_id2, phases: [...]}, ...}
    """
    ids = {}
    for hid, phase, fid, sid, t, n, c, p in compact_df.values:
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


def main():
    cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

    # > get the useful flukso sensors data in a compact csv
    compact_df = getCompactSensorDF()
    # saveToCsv(compact_df, COMPACT_SENSOR_FILE)
    # > correct phase signs
    compact_df = correctPhaseSigns(compact_df)
    print("nb sensors : ", len(compact_df))

    # > create config tables
    # createTableSensorConfig(cassandra_session, "sensors_config")
    # createTableGroupsConfig(cassandra_session, "groups_config")

    # now = pd.Timestamp.now(tz="UTC").replace(microsecond=0)   # default tz = CET, unaware timestamp
    # sensors_config_time = setInitSeconds(getTiming(input("Sensors config update time : "), now))
    # writeSensorsConfigCassandra(cassandra_session, compact_df, "sensors_config")
    # writeGroupsConfigCassandra(cassandra_session, "groups_config")

    # > setup the groups of flukso in a txt file 
    # writeGroupsFromFluksoIDs()
    # writeGroupsFromInstallationsIds()

    # > create cassandra tables 
    # createRawFluksoTable(cassandra_session, "raw")
    # createPowerTable(cassandra_session, "power")
    # createPowerTable(cassandra_session, "groups_power")
    # createRawMissingTable(cassandra_session, "raw_missing")

    # > save home ids to json
    # saveHomeIds(compact_df)

    # > save groups ids to json
    # saveGroupsIds()

if __name__ == "__main__":
    main()

