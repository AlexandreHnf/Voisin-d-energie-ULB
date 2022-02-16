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
            dff = self.session.series(self.sensor_id, head=self.since_timing)
        else:
            dff = self.session.series(self.sensor_id, head=self.since_timing, tail=self.to_timing)

        len_dff = len(dff.index)
        if len(dff.index) == 0:
            dff = getSpecificSerie(np.nan, self.since_timing, self.to_timing)
        print("{} - {} : {}, {}".format(self.flukso_id, self.sensor_id, len_dff, dff.index[0]))

        return dff

    