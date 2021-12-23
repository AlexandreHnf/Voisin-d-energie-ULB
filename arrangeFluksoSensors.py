import pandas as pd
import numpy as np


FLUKSO_TECHNICAL_FILE = "sensors/FluksoTechnical.xlsx"
UPDATED_FLUKSO_TECHNICAL_FILE = "sensors/updated_sensors.csv"
COMPACT_SENSOR_FILE = "sensors/sensors_technical.csv"
GROUPS_FILE = "sensors/grouped_homes_sensors.txt"
PHASE_TO_MODIF_FILE = "sensors/phases_to_modify.txt"


def getHomeIDcolumn(flukso_ids, home_ids):
    home_ids_col = []
    for fi in flukso_ids:
        home_ids_col.append(home_ids[fi])

    print(home_ids_col)
    return home_ids_col


def getFluksosDic(installation_ids_df):
    """ 
    Return a dictionary, 
    key = flukso id, value = installation id 
    ex : {'FL08000475': 'CDB001'}
    """
    fluksos = {}

    for i in range(len(installation_ids_df)):
        FlmId = installation_ids_df["FlmId"][i]
        installation_id = installation_ids_df["InstallationId"][i]
        # FluksoId = installation_ids_df["FluksoId"][i]
        print(FlmId, installation_id)

        # if str(FlmId) == "nan":
        #     FlmId = FluksoId

        fluksos[FlmId] = installation_id

    print(fluksos)
    return fluksos


def getInstallationsIds(flukso_ids, fluksos):
    """ 
    return a list of all the available installations ids
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
    """
    # sensors_df = pd.read_csv(SENSOR_FILE, header=0, index_col=1)
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

    installation_ids_df = pd.read_excel(FLUKSO_TECHNICAL_FILE, sheet_name="Flukso")
    fluksos = getFluksosDic(installation_ids_df)
    installation_ids_col = getInstallationsIds(sensors_df["FlmId"], fluksos)
    print(installation_ids_col)
    compact_df["home_ID"] = installation_ids_col

    compact_df.sort_values(by=["home_ID"])

    return compact_df


def getGroupsFromFluksoIDs():
    """
    get the list of installations IDs based on the flukso sensors ID of a group
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


def getGroupsFromInstallationsIds():
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


def saveToCsv(df):
    df.to_csv(COMPACT_SENSOR_FILE, index=None, header=True)


# ==========================================================================


def correctPhaseSigns(sensors_df=None):
    """
    functions to modify the signs of different phases (defined in a txt file of the form
    install_ID:phase1,phase2...)
    and write the corrected content in another csv file
    """
    to_modif = {}
    with open(PHASE_TO_MODIF_FILE) as f:
        for lign in f:
            l = lign.split(":")
            to_modif[l[0]] = l[1].strip().split(",")

    print(to_modif)

    # ===================================================

    # reading the csv file
    if sensors_df is None:
        sensors_df = pd.read_csv(COMPACT_SENSOR_FILE)

    print("nb of rows : ", len(sensors_df))
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
                sensors_df.loc[i, "net"] = str(-1 * float(sensors_df.loc[i, "net"]))
                # change con sign
                sensors_df.loc[i, "con"] = str(-1 * float(sensors_df.loc[i, "con"]))
                # change pro sign
                sensors_df.loc[i, "pro"] = str(-1 * float(sensors_df.loc[i, "pro"]))

    # writing into the file
    sensors_df.to_csv(UPDATED_FLUKSO_TECHNICAL_FILE, index=False)


# ===================================================


def main():
    # STEP 1 : get the useful flukso sensors data in a compact csv
    compact_df = getCompactSensorDF()
    saveToCsv(compact_df)

    # STEP 2 : setup the groups of flukso in a txt file 
    # getGroupsFromFluksoIDs()
    # getGroupsFromInstallationsIds()

    # STEP 3 : correct phase signs
    correctPhaseSigns(compact_df)  # TODO : put the sensor_df in parameter to directly correct

if __name__ == "__main__":
    main()

