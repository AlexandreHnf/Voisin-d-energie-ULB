from constants import *

import pandas as pd
import copy
import tmpo


def getLocalTimestampsIndex(df):
    """
    set timestamps to local timezone
    """

    # NAIVE
    if df.index.tzinfo is None or df.index.tzinfo.utcoffset(df.index) is None:
        # first convert to aware timestamp, then local
        return df.index.tz_localize("CET").tz_convert("CET")
    else: # if already aware timestamp
        return df.index.tz_convert("CET")
        

class Home:

    def __init__(self, session, sensors, since, since_timing, to_timing, home_id):
        self.session = session
        self.home_sensors = sensors.loc[sensors["home_ID"] == home_id]
        self.since = since
        self.since_timing = since_timing
        self.to_timing = to_timing
        self.home_id = home_id
        self.columns_names = self.getColumnsNames()

        self.power_df = self.createFluksoPowerDF()
        self.cons_prod_df = self.getConsumptionProductionDF()

    def getPowerDF(self):
        return self.power_df

    def getConsProdDF(self):
        return self.cons_prod_df

    def getHomeID(self):
        return self.home_id

    def setHomeID(self, hid):
        self.home_id = hid

    def getNbFluksoSensors(self):
        return len(self.power_df.columns)

    def getSince(self):
        return self.since

    def getSinceTiming(self):
        return self.since_timing

    def getToTiming(self):
        return self.to_timing

    def getColumnsTotal(self):
        return self.power_df.sum(axis=0, numeric_only=True)

    def getFluksoNames(self):
        """
        ex : CDB007 :> {FL08000436: [Main, ...], FL08000437: [Phase L2, ...]}
        """
        flukso_names = {}
        for i in range(len(self.home_sensors)):
            phase = self.home_sensors.index[i]
            if self.home_sensors.flukso_id[i] not in flukso_names:
                flukso_names[self.home_sensors.flukso_id[i]] = [phase]
            else:
                flukso_names[self.home_sensors.flukso_id[i]].append(phase)
        return flukso_names

    def getAggregatedDataPerFlukso(self):
        fluksos_df = pd.DataFrame()
        flukso_names = self.getFluksoNames()
        for fid, phases in flukso_names.items():
            fluksos_df["{}:{}".format(self.home_id, fid)] = self.power_df.loc[:, phases].sum(axis=1)
            j = 1

        return fluksos_df

    def getColumnsNames(self):
        """ 
        get columns names of the form : 
        flukso_id phase1, flukso_id phase2, ...
        """ 
        col_names = []
        for phase, row in self.home_sensors.iterrows():
            name = "{} {}".format(row["flukso_id"], phase)
            col_names.append(name)
        
        return col_names

    @staticmethod
    def energy2power(energy_df):
        """
        from cumulative energy to power (Watt)
        """
        power_df = energy_df.diff() * 1000
        power_df.fillna(0, inplace=True)
        power_df = power_df.resample(str(FREQ[0]) + FREQ[1]).mean()
        return power_df

    def getZeroSeries(self):
        to = pd.Timestamp.now(tz="UTC")
        if self.to_timing != 0:
            to = self.to_timing

        period = (to - self.since_timing).total_seconds() / FREQ[0]
        # print("s : {}, t : {}, to : {}, period : {}".format(self.since_timing, self.to_timing, to, period))
        zeros = pd.date_range(self.since_timing, periods=period, freq=str(FREQ[0]) + FREQ[1])
        # print("datetime range : ", zeros)
        zeros_series = pd.Series(int(period) * [0], zeros)
        # print("zeros series :", zeros_series)

        return zeros_series

    def createSeries(self):
        """
        create a list of time series from the sensors data
        """
        data_dfs = []

        for i in range(len(self.home_sensors)):
            # for id in self.home_sensors.sensor_id:
            id = self.home_sensors.sensor_id[i]
            fid = self.home_sensors.flukso_id[i]
            # print("- first timestamp : {}".format(self.session.first_timestamp(id)))
            # print("- last timestamp : {}".format(self.session.last_timestamp(id)))

            if self.to_timing == 0:
                dff = self.session.series(id, head=self.since_timing)
            else:
                dff = self.session.series(id, head=self.since_timing, tail=self.to_timing)
            print("{} - {} : {}".format(fid, id, len(dff.index)))

            if len(dff.index) == 0:
                dff = self.getZeroSeries()
            data_dfs.append(dff)

        return data_dfs

    def createFluksoPowerDF(self):
        """
        create a dataframe where the colums are the phases of the Flukso and the rows are the
        data : 1 row = 1 timestamp = 1 power value
        """
        data_dfs = self.createSeries()
        energy_df = pd.concat(data_dfs, axis=1)
        del data_dfs
        # print("nb index : ", len(energy_df.index))
        energy_df.index = pd.DatetimeIndex(energy_df.index, name="time")
        # energy_df.columns = list(self.home_sensors.index)
        energy_df.columns = self.columns_names
        power_df = self.energy2power(energy_df)
        del energy_df

        local_timestamps = getLocalTimestampsIndex(power_df)
        power_df.index = [tps for tps in local_timestamps]

        power_df.fillna(0, inplace=True)
        power_df = power_df.round(1)  # round with 2 decimals

        return power_df

    def getConsumptionProductionDF(self):
        """ 
        P_cons = P_tot - P_prod
        P_net = P_prod + P_cons
        """
        cons_prod_df = pd.DataFrame([[0, 0, 0] for _ in range(len(self.power_df))],
                                    self.power_df.index,
                                    ["P_cons", "P_prod", "P_tot"])

        for i, phase in enumerate(self.home_sensors.index):
            fluksoid_phase = self.columns_names[i]
            # c = self.home_sensors.loc[phase]["con"]
            p = self.home_sensors.loc[phase]["pro"]
            n = self.home_sensors.loc[phase]["net"]

            # cons_prod_df["P_cons"] = cons_prod_df["P_cons"] + c * self.power_df[fluksoid_phase]
            cons_prod_df["P_prod"] = cons_prod_df["P_prod"] + p * self.power_df[fluksoid_phase]
            cons_prod_df["P_tot"] = cons_prod_df["P_tot"] + n * self.power_df[fluksoid_phase]

        cons_prod_df["P_cons"] = cons_prod_df["P_tot"] - cons_prod_df["P_prod"]

        cons_prod_df = cons_prod_df.round(1)  # round all column values with 2 decimals

        return cons_prod_df

    def getColNamesWithID(self, df, home_id):
        col_names = df.columns
        new_col_names = {}
        for col_name in col_names:
            new_col_names[col_name] = "{}_{}".format(home_id, col_name)

        return new_col_names

    def appendFluksoData(self, power_df, home_id):
        # print(len(self.power_df), len(power_df))

        if not self.home_id in self.power_df.columns[0]:
            self.power_df = self.power_df.rename(self.getColNamesWithID(self.power_df, self.home_id), axis=1)
        power_df = power_df.rename(self.getColNamesWithID(power_df, home_id), axis=1)
        self.power_df = self.power_df.join(power_df)

    def addConsProd(self, cons_prod_df):
        self.cons_prod_df = self.cons_prod_df.add(cons_prod_df, fill_value=0)