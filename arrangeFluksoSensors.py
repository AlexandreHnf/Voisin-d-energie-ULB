import pandas as pd
import numpy as np

START_ID = 4
SENSOR_FILE = "sensors/FluksoTechnical.csv"
COMPACT_SENSOR_FILE = "sensors/sensors_technical.csv"


def getHomeIDs(flukso_ids):
    home_ids = []
    current_id = None
    new_id = START_ID - 1
    for fi in flukso_ids:
        if current_id is None or fi != current_id:
            new_id += 1
            current_id = fi

        home_ids.append(new_id)

    print(home_ids)
    return home_ids


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
                                       "home_name",
                                       "sensor_id",
                                       "token",
                                       "state"])

    compact_df["phase"] = sensors_df["Function"]
    compact_df["home_name"] = sensors_df["FlmId"]
    compact_df["sensor_id"] = sensors_df["SensorId"]
    compact_df["token"] = sensors_df["Token"]

    home_ids = getHomeIDs(sensors_df["FlmId"])
    compact_df["home_ID"] = home_ids

    states = getStateFromPhase(compact_df["phase"])
    compact_df["state"] = states

    print(compact_df.head(10))

    return compact_df


def saveToCsv(df):
    df.to_csv(COMPACT_SENSOR_FILE, index=None, header=True)


def main():
    compact_df = getCompactSensorDF()

    saveToCsv(compact_df)



if __name__ == "__main__":
    main()

