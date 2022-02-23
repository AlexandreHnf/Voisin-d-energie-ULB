from os import times
from time import time
from constants import *
from utils import getLocalTimestampsIndex, toEpochs
from sensor import *

import pandas as pd
import numpy as np
import copy
import tmpo
        

class Home:

    def __init__(self, sensors_info, since, since_timing, to_timing, home_id, sensors):
        self.sensors_info = sensors_info.loc[sensors_info["home_ID"] == home_id]
        self.sensors = sensors
        self.since = since
        self.since_timing = since_timing
        self.to_timing = to_timing
        self.home_id = home_id
        self.columns_names = self.getColumnsNames()

        self.raw_df, self.incomplete_raw_df = self.createFluksoRawDF()
        self.cons_prod_df = self.getConsumptionProductionDF()

    def getRawDF(self):
        return self.raw_df

    def getIncompletePowerDF(self):
        # with UTC timezones
        return self.incomplete_raw_df

    def getConsProdDF(self):
        return self.cons_prod_df

    def getHomeID(self):
        return self.home_id

    def setHomeID(self, hid):
        self.home_id = hid

    def getNbFluksoSensors(self):
        return len(self.raw_df.columns)

    def getSince(self):
        return self.since

    def getSinceTiming(self):
        return self.since_timing

    def getToTiming(self):
        return self.to_timing

    def getColumnsTotal(self):
        return self.raw_df.sum(axis=0, numeric_only=True)

    def getFluksoNames(self):
        """
        ex : CDB007 :> {FL08000436: [Main, ...], FL08000437: [Phase L2, ...]}
        """
        flukso_names = {}
        for i in range(len(self.sensors_info)):
            phase = self.sensors_info.index[i]
            if self.sensors_info.flukso_id[i] not in flukso_names:
                flukso_names[self.sensors_info.flukso_id[i]] = [phase]
            else:
                flukso_names[self.sensors_info.flukso_id[i]].append(phase)
        return flukso_names

    def getAggregatedDataPerFlukso(self):
        fluksos_df = pd.DataFrame()
        flukso_names = self.getFluksoNames()
        for fid, phases in flukso_names.items():
            fluksos_df["{}:{}".format(self.home_id, fid)] = self.raw_df.loc[:, phases].sum(axis=1)
            j = 1

        return fluksos_df

    def getColumnsNames(self):
        """ 
        get columns names of the form : 
        flukso_id phase1, flukso_id phase2, ...
        """ 
        col_names = []
        # for phase, row in self.sensors_info.iterrows():
        #     name = "{} {}".format(row["flukso_id"], phase)
        #     col_names.append(name)

        for sensor in self.sensors:
            col_names.append(sensor.getSensorID())
        
        return col_names


    def createSeries(self):
        """
        create a list of time series from the sensors data
        """
        data_dfs = []

        filler = getSpecificSerie(np.nan, self.since_timing, self.to_timing)

        for sensor in self.sensors:
            data_dfs.append(sensor.getSerie())

        data_dfs.append(filler)

        return data_dfs
        

    def createFluksoRawDF(self):
        """
        create a dataframe where the colums are the phases of the Flukso and the rows are the
        data : 1 row = 1 timestamp = 1 power value
        """
        data_dfs = self.createSeries()  # list of series, one per flukso sensor
        energy_df = pd.concat(data_dfs, axis=1)  # combined series, 1 col = 1 sensor
        del data_dfs
        energy_df.columns = self.columns_names + ["fill"]
        energy_df = energy_df.drop(['fill'], axis=1) # remove the filler col

        energy_df.index = pd.DatetimeIndex(energy_df.index, name="time")
        # convert all timestamps to local timezone (CET)
        local_timestamps = getLocalTimestampsIndex(energy_df)
        energy_df.index = [tps for tps in local_timestamps]
        print("nb timestamps : ", len(energy_df.index))

        incomplete_raw_df = energy_df[energy_df.isna().any(axis=1)]  # with CET timezones
        print("nb of nan: ", energy_df.isna().sum().sum()) # count nb of nan in the entire df

        energy_df.fillna(0, inplace=True)
        
        raw_df = energy2power(energy_df, local_timestamps[0])  # cumulative energy to power conversion
        del energy_df

        raw_df.fillna(0, inplace=True)
        raw_df = raw_df.round(1)  # round with 2 decimals

        return raw_df, incomplete_raw_df

    def getConsumptionProductionDF(self):
        """ 
        P_cons = P_tot - P_prod
        P_net = P_prod + P_cons
        cons_prod_df : timestamp, P_cons, P_prod, P_tot
        """
        cons_prod_df = pd.DataFrame([[0, 0, 0] for _ in range(len(self.raw_df))],
                                    self.raw_df.index,
                                    ["P_cons", "P_prod", "P_tot"])

        for i, phase in enumerate(self.sensors_info.index):
            fluksoid_phase = self.columns_names[i]
            # c = self.sensors_info.loc[phase]["con"]
            p = self.sensors_info.loc[phase]["pro"]
            n = self.sensors_info.loc[phase]["net"]

            # cons_prod_df["P_cons"] = cons_prod_df["P_cons"] + c * self.raw_df[fluksoid_phase]
            cons_prod_df["P_prod"] = cons_prod_df["P_prod"] + p * self.raw_df[fluksoid_phase]
            cons_prod_df["P_tot"] = cons_prod_df["P_tot"] + n * self.raw_df[fluksoid_phase]

        cons_prod_df["P_cons"] = cons_prod_df["P_tot"] - cons_prod_df["P_prod"]

        cons_prod_df = cons_prod_df.round(1)  # round all column values with 2 decimals

        return cons_prod_df

    def getColNamesWithID(self, df, home_id):
        col_names = df.columns
        new_col_names = {}
        for col_name in col_names:
            new_col_names[col_name] = "{}_{}".format(home_id, col_name)

        return new_col_names

    def appendFluksoData(self, raw_df, home_id):
        # print(len(self.raw_df), len(raw_df))

        if not self.home_id in self.raw_df.columns[0]:
            self.raw_df = self.raw_df.rename(self.getColNamesWithID(self.raw_df, self.home_id), axis=1)
        raw_df = raw_df.rename(self.getColNamesWithID(raw_df, home_id), axis=1)
        self.raw_df = self.raw_df.join(raw_df)

    def addConsProd(self, cons_prod_df):
        self.cons_prod_df = self.cons_prod_df.add(cons_prod_df, fill_value=0)