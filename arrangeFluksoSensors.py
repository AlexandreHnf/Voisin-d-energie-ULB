import pandas as pd
import numpy as np

START_ID = 4
SENSOR_FILE = "sensors/FluksoTechnical.csv"
SENSOR_FLUKSO_FILE = "sensors/FluksoTechnical_Flukso.csv"
COMPACT_SENSOR_FILE = "sensors/sensors_technical.csv"


def getHomeIDcolumn(flukso_ids, home_ids):
    home_ids_col = []
    for fi in flukso_ids:
        home_ids_col.append(home_ids[fi])

    print(home_ids_col)
    return home_ids_col


def getInstallationsIds(flukso_ids):
    installation_ids_df = pd.read_csv(SENSOR_FLUKSO_FILE)
    fluksos = {}

    for i in range(len(installation_ids_df)):
        FlmId = installation_ids_df["FlmId"][i]
        installation_id = installation_ids_df["InstallationId"][i]
        FluksoId = installation_ids_df["FluksoId"][i]
        print(FlmId, installation_id, FluksoId)

        if str(FlmId) == "nan":
            FlmId = FluksoId

        fluksos[FlmId] = installation_id

    installation_id_col = []
    for fi in flukso_ids:
        installation_id_col.append(fluksos[fi])
    return installation_id_col


# def getHomeIDs():
#     installation_ids_df = pd.read_csv(SENSOR_FLUKSO_FILE)
#     home_ids = {}
#     installation_indexes = {}
#     index = 0
#     for i in range(len(installation_ids_df)):
#         FlmId = installation_ids_df["FlmId"][i]
#         installation_id = installation_ids_df["InstallationId"][i]
#         FluksoId = installation_ids_df["FluksoId"][i]
#         print(FlmId, installation_id, FluksoId)
#
#         if str(FlmId) == "nan":
#             FlmId = FluksoId
#
#         if installation_id not in installation_indexes:
#             index += 1
#             installation_indexes[installation_id] = index
#
#         home_ids[FlmId] = index
#
#     print("installation_indexes : ", installation_indexes)
#     return home_ids


def getStateFromPhase(phase_names):
    states = []
    for phase in phase_names:
        if phase[-1] == "-":
            states.append("-")
        else:
            states.append("+")

    return states


def getCompactSensorDF():
    sensors_df = pd.read_csv(SENSOR_FILE, header=0, index_col=1)
    compact_df = pd.DataFrame(columns=["home_ID",
                                       "phase",
                                       "flukso_id",
                                       "sensor_id",
                                       "token",
                                       "state"])

    compact_df["phase"] = sensors_df["Function"]
    compact_df["flukso_id"] = sensors_df["FlmId"]
    compact_df["sensor_id"] = sensors_df["SensorId"]
    compact_df["token"] = sensors_df["Token"]

    # home_ids = getHomeIDs()
    installation_ids_col = getInstallationsIds(sensors_df["FlmId"])
    compact_df["home_ID"] = installation_ids_col
    # home_ids_col = getHomeIDcolumn(sensors_df["FlmId"], home_ids)
    # compact_df["home_ID"] = home_ids_col

    states = getStateFromPhase(compact_df["phase"])
    compact_df["state"] = states

    compact_df.sort_values(by=["home_ID"])

    print(compact_df.head(10))

    return compact_df


def saveToCsv(df):
    df.to_csv(COMPACT_SENSOR_FILE, index=None, header=True)


def main():
    compact_df = getCompactSensorDF()

    saveToCsv(compact_df)



if __name__ == "__main__":
    main()

