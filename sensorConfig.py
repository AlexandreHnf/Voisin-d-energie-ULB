import pandas as pd

class Configuration:
    def __init__(self, sconfig_id, sconfig_df, gconfig_id, gconfig_df):
        self.sconfig_id = sconfig_id
        self.gconfig_id = gconfig_id 
        self.sconfig_df = sconfig_df
        self.gconfig_df = gconfig_df 

    def getSensorConfigID(self):
        return self.sconfig_id
    
    def getGroupsConfigID(self):
        return self.gconfig_id

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