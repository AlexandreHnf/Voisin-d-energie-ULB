import pandas as pd
import numpy as np

START_ID = 4
FLUKSO_TECHNICAL_FILE = "sensors/FluksoTechnical.xlsx"
COMPACT_SENSOR_FILE = "sensors/sensors_technical.csv"
GROUPS_FILE = "sensors/grouped_homes_sensors.txt"


def getHomeIDcolumn(flukso_ids, home_ids):
    home_ids_col = []
    for fi in flukso_ids:
        home_ids_col.append(home_ids[fi])

    print(home_ids_col)
    return home_ids_col


def getFluksosDic(installation_ids_df):
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
    installation_id_col = []
    for fi in flukso_ids:
        if fi in fluksos:
            installation_id_col.append(fluksos[fi])
        else:
            installation_id_col.append("unknown")

    return installation_id_col


def getStateFromPhase(phase_names):
    states = []
    for phase in phase_names:
        if phase[-1] == "-":
            states.append("-")
        else:
            states.append("+")

    return states


def getSign(x):
    if x == 1:
        return "+"
    elif x == -1:
        return "-"


def getState(cons, prod, net):
    states = []
    for i in range(len(cons)):
        if str(prod[i]) != "nan":  # if prod
            states.append("-{}".format(getSign(prod[i])))
        else:
            if str(cons[i]) != "nan":
                states.append("+{}".format(getSign(cons[i])))
            else:
                states.append("+{}".format(getSign(net[i])))
        print(cons[i], prod[i])

    print(states)

    return states



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
                                       "pro",
                                       "state"])

    compact_df["phase"] = sensors_df["Function"]
    compact_df["flukso_id"] = sensors_df["FlmId"]
    compact_df["sensor_id"] = sensors_df["SensorId"]
    compact_df["token"] = sensors_df["Token"]
    compact_df["net"] = sensors_df["Network"]
    compact_df["con"] = sensors_df["Cons"]
    compact_df["pro"] = sensors_df["Prod"]

    compact_df.fillna(0, inplace=True)

    # home_ids = getHomeIDs()
    installation_ids_df = pd.read_excel(FLUKSO_TECHNICAL_FILE, sheet_name="Flukso")
    fluksos = getFluksosDic(installation_ids_df)
    installation_ids_col = getInstallationsIds(sensors_df["FlmId"], fluksos)
    print(installation_ids_col)
    compact_df["home_ID"] = installation_ids_col

    states = getState(sensors_df["Cons"], sensors_df["Prod"], sensors_df["Network"])
    # states = getStateFromPhase(compact_df["phase"])
    compact_df["state"] = states

    compact_df.sort_values(by=["home_ID"])

    print(compact_df.head(68))

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


def main():
    compact_df = getCompactSensorDF()
    saveToCsv(compact_df)

    # getGroupsFromFluksoIDs()

    # getGroupsFromInstallationsIds()


if __name__ == "__main__":
    main()

