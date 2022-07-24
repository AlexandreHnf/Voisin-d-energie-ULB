__title__ = "sensors"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"



# standard library

# 3rd party packages
import numpy as np

# local sources
from utils import(
	getSpecificSerie,
	setInitSeconds
) 


# ===============================================================================


class Sensor: 
	def __init__(self, session, flukso_id, sensor_id, since_timing, to_timing):
		self.session = session 
		self.flukso_id = flukso_id
		self.sensor_id = sensor_id

		# Convert to UTC timezone for tmpo
		self.since_timing = since_timing.tz_convert("UTC")
		self.to_timing = to_timing.tz_convert("UTC")

	def getFluksoID(self):
		return self.flukso_id
	
	def getSensorID(self):
		return self.sensor_id

	def getSerie(self):
		""" 
		# since_timing and to_timing = UTC timezone for tmpo query
		"""

		if self.to_timing == 0:
			dff = self.session.series(
				self.sensor_id, 
				head=self.since_timing
			)
		else:
			dff = self.session.series(
				self.sensor_id, 
				head=self.since_timing, 
				tail=self.to_timing
			)

		if len(dff.index) == 0:
			dff = getSpecificSerie(np.nan, self.since_timing, self.to_timing)
			
		return dff

