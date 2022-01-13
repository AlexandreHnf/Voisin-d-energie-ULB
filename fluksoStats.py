from constants import *
import pyToCassandra as ptc

import os
import sys
import pandas as pd

# ====================================================================================


def read_sensor_info(sensor_file):
    """
    read csv file of sensors data
    """
    sensor_file
    sensors = pd.read_csv(sensor_file, header=0, index_col=1)
    return sensors


def getTiming(t, now):
    """
    get the timestamp of the "since"
    ex : the timestamp 20 min ago
    """
    print("since {}".format(t))
    timing = 0
    if t:
        if t[0] == "s":
            e = t[1:].split("-")
            timing = pd.Timestamp(year=int(e[0]), month=int(e[1]), day=int(e[2]),
                                  hour=int(e[3]), minute=int(e[4]), tz="CET")
        else:
            print("time delta : ", pd.Timedelta(t))
            print("now : ", now)
            timing = now - pd.Timedelta(t)

    print("timing : ", timing)
    return timing


def getRawData(session, since):
    """ 
    get raw flukso data from cassandra since a certain amount of time
    """
    now = pd.Timestamp.now(tz="UTC")
    timing = getTiming(since, now)

    print("timing date : ", str(timing.date()))
    print("now date : ", str(now.date()))
    dates = list(set(["'"+str(timing.date())+"'", "'"+str(now.date())+"'"]))
    print(dates)

    dates = ",".join(dates)
    where_clause = "home_id = {} and day IN ({}) AND ts > {}".format("'CDB011'", dates, "'"+str(timing)+"'")
    data = ptc.selectQuery(session, CASSANDRA_KEYSPACE, "raw_data", "*", where_clause, "LIMIT 10")

    print(data)
    return data
        

def getConsumptionProductionDF(sensors, power_df):
        """ 
        P_cons = P_tot - P_prod
        P_net = P_prod + P_cons
        """
        home_id = "CDB011"
        home_sensors = sensors.loc[sensors["home_ID"] == home_id]
        cons_prod_df = power_df[["home_id","day", "ts"]].copy()
        cons_prod_df["P_cons"] = 0
        cons_prod_df["P_prod"] = 0
        cons_prod_df["P_tot"] = 0
        # cons_prod_df = pd.DataFrame([[0, 0, 0] for _ in range(len(power_df))],
        #                             self.power_df.index,
        #                             ["P_cons", "P_prod", "P_tot"])

        for i, phase in enumerate(home_sensors.index):
            phase_i = "phase" + str(i+1)
            p = home_sensors.loc[phase]["pro"]
            n = home_sensors.loc[phase]["net"]

            cons_prod_df["P_prod"] = cons_prod_df["P_prod"] + p * power_df[phase_i]
            cons_prod_df["P_tot"] = cons_prod_df["P_tot"] + n * power_df[phase_i]

        cons_prod_df["P_cons"] = cons_prod_df["P_tot"] - cons_prod_df["P_prod"]

        cons_prod_df = cons_prod_df.round(1)  # round all column values with 2 decimals

        return cons_prod_df


def main():
    session = ptc.connectToCluster(CASSANDRA_KEYSPACE)
    since = "s2022-01-10-16-24-32"
    data = getRawData(session, since) 

    sensors = read_sensor_info(UPDATED_SENSORS_FILE)
    cons_prod_df = getConsumptionProductionDF(sensors, data)
    print(cons_prod_df)


if __name__ == "__main__":
    main()