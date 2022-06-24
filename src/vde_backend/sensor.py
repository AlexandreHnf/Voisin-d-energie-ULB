__title__ = "sensors"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"


from constants import * 
from utils import * 
import pandas as pd
import numpy as np

import tmpo

class Sensor: 
    def __init__(self, session, flukso_id, sensor_id, since_timing, to_timing):
        self.session = session 
        self.flukso_id = flukso_id
        self.sensor_id = sensor_id

        self.since_timing = since_timing
        self.to_timing = to_timing

    def getFluksoID(self):
        return self.flukso_id
    
    def getSensorID(self):
        return self.sensor_id

    def getSerie(self):
        if self.to_timing == 0:
            # since_timing = UTC timezone for tmpo query
            dff = self.session.series(self.sensor_id, head=self.since_timing)
        else:
            dff = self.session.series(self.sensor_id, head=self.since_timing, tail=self.to_timing)

        len_dff = len(dff.index)
        if len(dff.index) == 0:
            # print("{} > 0".format(self.sensor_id))
            dff = getSpecificSerie(np.nan, self.since_timing, self.to_timing)
        # print("{} - {} : {}, {}".format(self.flukso_id, self.sensor_id, len_dff, dff.index[0]))

        # print("{} : {} sec freq".format(self.sensor_id, (dff.index[1] - dff.index[0]).seconds))

        return dff

    
