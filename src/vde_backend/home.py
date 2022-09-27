__title__ = "home"
__version__ = "2.0.0"
__author__ = "Alexandre Heneffe, and Guillaume Levasseur"
__license__ = "MIT"


# standard library

# 3rd party packages
import pandas as pd
import numpy as np
import logging

# local sources
from utils import (
	energy2power,
	resample_extend
)


def get_local_timestamps_index(df):
	"""
	set timestamps to local timezone
	"""

	# NAIVE
	if df.index.tzinfo is None or df.index.tzinfo.utcoffset(df.index) is None:
		# first convert to aware timestamp, then local
		return df.index.tz_localize("CET", ambiguous='NaT').tz_convert("CET")
	else: # if already aware timestamp
		return df.index.tz_convert("CET")


class Home:

	def __init__(self, sensors_info, since_timing, to_timing, home_id, sensors):
		self.sensors_config = sensors_info
		self.sensors = sensors

		# Convert to UTC timezone for tmpo
		self.since_timing = since_timing.tz_convert("UTC")
		self.to_timing = to_timing.tz_convert("UTC")
		self.home_id = home_id
		self.columns_names = self.get_columns_names()

		self.len_raw = 0
		self.len_nan = 0
		self.nb_nan = 0

		self.energy_df = self.get_energy_raw_df()
		self.raw_df = self.create_flukso_raw_df()
		self.incomplete_raw_df = self.find_incomplete_raw_df()
		self.cons_prod_df = self.get_consumption_production_df()

	def get_raw_df(self):
		return self.raw_df

	def get_incomplete_power_df(self):
		return self.incomplete_raw_df

	def get_cons_prod_df(self):
		return self.cons_prod_df

	def get_home_id(self):
		return self.home_id

	def set_home_id(self, hid):
		self.home_id = hid

	def get_nb_flukso_sensors(self):
		return len(self.raw_df.columns)

	def get_since_timing(self):
		return self.since_timing

	def get_to_timing(self):
		return self.to_timing

	def get_columns_total(self):
		return self.raw_df.sum(axis=0, numeric_only=True)

	def show_query_info(self):
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

	def get_flukso_names(self):
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

	def get_aggregated_data_per_flukso(self):
		fluksos_df = pd.DataFrame()
		flukso_names = self.get_flukso_names()
		for fid, phases in flukso_names.items():
			fluksos_df["{}:{}".format(self.home_id, fid)] = self.raw_df.loc[:, phases].sum(axis=1)
			j = 1

		return fluksos_df

	def get_columns_names(self):
		""" 
		get columns names of the form : 
		[flukso sensor1, flukso sensor2, ...]
		""" 
		col_names = []

		for sensor in self.sensors:
			col_names.append(sensor.get_sensor_id())
		
		return col_names


	def create_series(self):
		"""
		create a list of time series from the sensors data
		"""
		data_dfs = []

		for sensor in self.sensors:
			s = sensor.get_serie()
			data_dfs.append(s)

		return data_dfs

	
	def get_energy_raw_df(self):
		"""
		Get a dataframe with all the raw cumulative energy series from each sensor
		of the home 
		1 column = 1 sensor
		"""
		data_dfs = self.create_series()  # list of series, one per flukso sensor
		energy_df = pd.concat(data_dfs, axis=1)  # combined series
		del data_dfs

		return energy_df


	def find_incomplete_raw_df(self):
		""" 
		From the cumulative energy dataframe, 
		1) fill the gaps in the timestamps with NaN values
		2) get a dataframe containing all the lines with NaN values (= incomplete rows)
		"""

		filled_df = resample_extend(self.energy_df, self.since_timing, self.to_timing)
		filled_df.columns = self.columns_names
		incomplete_raw_df = filled_df[filled_df.isna().any(axis=1)]  # with CET timezones
		self.nb_nan = filled_df.isna().sum().sum()  # count nb of nan in the entire df
		self.len_nan = len(incomplete_raw_df)

		incomplete_raw_df.index = pd.DatetimeIndex(incomplete_raw_df.index, name="time")
		# convert all timestamps to local timezone (CET)
		incomplete_raw_df.index = get_local_timestamps_index(incomplete_raw_df)
		return incomplete_raw_df


	def create_flukso_raw_df(self):
		"""
		create a dataframe where the colums are the phases of the Flukso and the rows are the
		data : 
		1 row = 1 timestamp = 1 power value
		"""
		
		power_df = energy2power(self.energy_df) # cumulative energy to power conversion
		raw_df = resample_extend(power_df, self.since_timing, self.to_timing)
		raw_df.columns = self.columns_names
		# timestamps column
		raw_df.index = pd.DatetimeIndex(raw_df.index, name="time", ambiguous='NaT')
		# convert all timestamps to local timezone (CET)
		local_timestamps = get_local_timestamps_index(raw_df)
		raw_df.index = local_timestamps
		self.len_raw = len(raw_df.index)
		raw_df.fillna(0, inplace=True)
		if len(local_timestamps) > 1:
			raw_df.drop(local_timestamps[0], inplace=True)  # drop first row because NaN after conversion

			raw_df = raw_df.round(1)  # round with 2 decimals

		return raw_df


	def get_consumption_production_df(self):
		""" 
		P_cons = P_tot - P_prod
		P_net = P_prod + P_cons
		cons_prod_df : timestamp, P_cons, P_prod, P_tot
		"""
		cons_prod_df = pd.DataFrame(
			0,
			self.raw_df.index,
			["P_cons", "P_prod", "P_tot"]
		)

		for i, sid in enumerate(self.sensors_config.index):
			p = self.sensors_config.loc[sid]["pro"]
			n = self.sensors_config.loc[sid]["net"]

			cons_prod_df["P_prod"] = cons_prod_df["P_prod"] + p * self.raw_df[sid]
			cons_prod_df["P_tot"] = cons_prod_df["P_tot"] + n * self.raw_df[sid]

		cons_prod_df["P_cons"] = cons_prod_df["P_tot"] - cons_prod_df["P_prod"]

		cons_prod_df = cons_prod_df.round(1)  # round all column values with 2 decimals

		return cons_prod_df


	def get_col_names_with_id(self, df, home_id):
		col_names = df.columns
		new_col_names = {}
		for col_name in col_names:
			new_col_names[col_name] = "{}_{}".format(home_id, col_name)

		return new_col_names


	def append_flukso_data(self, raw_df, home_id):
		# print(len(self.raw_df), len(raw_df))

		if not self.home_id in self.raw_df.columns[0]:
			self.raw_df = self.raw_df.rename(
				self.get_col_names_with_id(self.raw_df, self.home_id), axis=1
			)
		raw_df = raw_df.rename(self.get_col_names_with_id(raw_df, home_id), axis=1)
		self.raw_df = self.raw_df.join(raw_df)


	def add_cons_prod(self, cons_prod_df):
		self.cons_prod_df = self.cons_prod_df.add(cons_prod_df, fill_value=0)
