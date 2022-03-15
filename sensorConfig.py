import pandas as pd

class Configuration:
    def __init__(self, sconfig_id, sconfig_df, gconfig_df):
        self.sconfig_id = sconfig_id
        self.sconfig_df = sconfig_df
        self.gconfig_df = gconfig_df

        self.ids = self.getHomeSensors()

    def getConfigID(self):
        return self.sconfig_id

    def getFirstSensorId(self):
        """ 
        Get the first sensor id of the list of sensors
        -> useful when all sensors of a home share the same property for ex.
        """
        return self.ids[list(self.ids.keys())[0]][0]

    def getSensorsConfig(self):
        """ 
        get a dataframe with columns : 
        - home_id, phase, fluksid, sensor_id, sensor_token, net, con, pro
        """
        return self.sconfig_df

    def getGroupsConfig(self):
        """ 
        get a dataframe with columns : 
        - group_id, homes (list of home ids)
        """
        return self.gconfig_df

    def getHomeSensors(self):
        """ 
        return a dictionary with
        key : home id, value : list of sensor ids
        """
        ids = {}
        for hid, home in self.sconfig_df.groupby("home_id"):
            ids[hid] = list(home.index)
        
        return ids

    def getIds(self):
        return self.ids