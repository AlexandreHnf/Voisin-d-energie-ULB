__title__ = "sensor"
__version__ = "2.0.0"
__author__ = "Alexandre Heneffe, and Guillaume Levasseur"
__license__ = "MIT"


# standard library

# 3rd party packages
import numpy as np
import pandas as pd

# local sources
from utils import (
    time_range
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

    def get_flukso_id(self):
        return self.flukso_id

    def get_sensor_id(self):
        return self.sensor_id

    def get_serie(self):
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
            dff = pd.Series(np.nan, index=time_range(self.since_timing, self.to_timing))

        return dff
