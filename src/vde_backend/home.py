__title__ = "home"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"


# standard library

# 3rd party packages
import pandas as pd
import numpy as np
import logging

# local sources
from utils import energy2power, getLocalTimestampsIndex, getSpecificSerie


class Home:

	def __init__(self, sensors_info, since_timing, to_timing, home_id, sensors):
		self.sensors_config = sensors_info
		self.sensors = sensors
		self.since_timing = since_timing
		self.to_timing = to_timing
		self.home_id = home_id
		self.columns_names = self.getColumnsNames()

		self.len_raw = 0
		self.len_nan = 0
		self.nb_nan = 0

		self.energy_df = self.getEnergyRawDf()
		self.raw_df = self.createFluksoRawDF()
		self.incomplete_raw_df = self.findIncompleteRawDf()
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

	def getSinceTiming(self):
		return self.since_timing

	def getToTiming(self):
		return self.to_timing

	def getColumnsTotal(self):
		return self.raw_df.sum(axis=0, numeric_only=True)

	def showQueryInfo(self):
		""" 
		show number of rows received in total with the tmpo query
		+ the number of ligns containing NaN values + the total number of NaN values in the
		dataframe
		"""
		logging.info("     - len raw : {}, len NaN : {}, tot NaN: {}".format(
			self.len_raw,
			self.len_nan, 
			self.nb_nan
		))

	def getFluksoNames(self):
		"""
		ex : CDB007 :> {FL08000436: [Main, ...], FL08000437: [Phase L2, ...]}
		"""
		flukso_names = {}
		for i in range(len(self.sensors_config)):
			phase = self.sensors_config.index[i]
			if self.sensors_config.flukso_id[i] not in flukso_names:
				flukso_names[self.sensors_config.flukso_id[i]] = [phase]
			else:
				flukso_names[self.sensors_config.flukso_id[i]].append(phase)
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
		[flukso sensor1, flukso sensor2, ...]
		""" 
		col_names = []

		for sensor in self.sensors:
			col_names.append(sensor.getSensorID())
		
		return col_names


	def createSeries(self):
		"""
		create a list of time series from the sensors data
		"""
		data_dfs = []

		for sensor in self.sensors:
			s = sensor.getSerie()
			# if self.since_timing == 0:
			#     first_unix_ts = list(s.index)[0]
			#     self.since_timing = pd.Timestamp(datetime.datetime.fromtimestamp(first_unix_ts)).tz_localize("UTC")
			data_dfs.append(s)

		return data_dfs

	
	def getEnergyRawDf(self):
		"""
		Get a dataframe with all the raw cumulative energy series from each sensor
		of the home 
		1 column = 1 sensor
		"""
		data_dfs = self.createSeries()  # list of series, one per flukso sensor
		energy_df = pd.concat(data_dfs, axis=1)  # combined series
		del data_dfs

		# print(energy_df)

		return energy_df


	def findIncompleteRawDf(self):
		""" 
		From the cumulative energy dataframe, 
		1) fill the gaps in the timestamps with NaN values
		2) get a dataframe containing all the lines with NaN values (= incomplete rows)
		"""

		filler = getSpecificSerie(np.nan, self.since_timing, self.to_timing)
		filled_df = pd.concat([self.energy_df, filler], axis=1)
		del filler
		filled_df.columns = self.columns_names + ["fill"]
		filled_df = filled_df.drop(['fill'], axis=1) # remove the filler col

		incomplete_raw_df = filled_df[filled_df.isna().any(axis=1)]  # with CET timezones
		self.nb_nan = filled_df.isna().sum().sum()  # count nb of nan in the entire df
		self.len_nan = len(incomplete_raw_df)
		# logging.debug("len NaN : {}, tot NaN: {}".format(len(incomplete_raw_df), self.nb_nan))

		incomplete_raw_df.index = pd.DatetimeIndex(incomplete_raw_df.index, name="time")
		# convert all timestamps to local timezone (CET)
		local_timestamps = getLocalTimestampsIndex(incomplete_raw_df)
		incomplete_raw_df.index = [tps for tps in local_timestamps]
		
		# if self.home_id == "ECHASC":
		# 	print("incomplete_raw_df", incomplete_raw_df.head(30))

		return incomplete_raw_df
		

	def createFluksoRawDF(self):
		"""
		create a dataframe where the colums are the phases of the Flukso and the rows are the
		data : 
		1 row = 1 timestamp = 1 power value
		"""
		
		power_df = energy2power(self.energy_df) # cumulative energy to power conversion
		filler = getSpecificSerie(np.nan, self.since_timing, self.to_timing)
		raw_df = pd.concat([power_df, filler], axis=1)
		del power_df; del filler
		raw_df.columns = self.columns_names + ["fill"]
		raw_df = raw_df.drop(['fill'], axis=1) # remove the filler col

		# timestamps column
		raw_df.index = pd.DatetimeIndex(raw_df.index, name="time", ambiguous='NaT')
		# convert all timestamps to local timezone (CET)
		local_timestamps = getLocalTimestampsIndex(raw_df)

		raw_df.index = [tps for tps in local_timestamps]
		self.len_raw = len(raw_df.index)

		raw_df.fillna(0, inplace=True)
		
		if len(local_timestamps) > 1:
			raw_df.drop(local_timestamps[0], inplace=True)  # drop first row because NaN after conversion

			raw_df = raw_df.round(1)  # round with 2 decimals

		return raw_df


	def getConsumptionProductionDF(self):
		""" 
		P_cons = P_tot - P_prod
		P_net = P_prod + P_cons
		cons_prod_df : timestamp, P_cons, P_prod, P_tot
		"""
		cons_prod_df = pd.DataFrame([[0, 0, 0] for _ in range(len(self.raw_df))],
									self.raw_df.index,
									["P_cons", "P_prod", "P_tot"])

		for i, sid in enumerate(self.sensors_config.index):
			p = self.sensors_config.loc[sid]["pro"]
			n = self.sensors_config.loc[sid]["net"]

			cons_prod_df["P_prod"] = cons_prod_df["P_prod"] + p * self.raw_df[sid]
			cons_prod_df["P_tot"] = cons_prod_df["P_tot"] + n * self.raw_df[sid]

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