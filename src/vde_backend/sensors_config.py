__title__ = "sensors_config"
__version__ = "2.0.0"
__author__ = "Alexandre Heneffe, and Brice Petit"
__license__ = "MIT"


class Configuration:
    def __init__(self, config_id, sconfig_df):
        self.config_id = config_id              # config insertion date
        self.sconfig_df = sconfig_df            # dataframe with the whole config

        self.ids = self.get_home_sensors()      # all home ids (installation ids)

    def __str__(self):
        """
        Display Configuration stats/information
        """

        s = "- Number of Homes :           "
        s += str(self.get_nb_homes()) + "\n"
        s += "- Number of Fluksos :         "
        s += str(len(set(self.get_sensors_config().flukso_id))) + "\n"
        s += "- Number of Fluksos sensors : "
        s += str(len(self.get_sensors_config())) + "\n"

        return s

    def get_nb_homes(self):
        return len(self.ids)

    def get_config_id(self):
        return self.config_id

    def get_first_sensor_id(self):
        """
        Get the first sensor id of the list of sensors
        -> useful when all sensors of a home share the same property for ex.
        """
        return self.ids[list(self.ids.keys())[0]][0]

    def get_sensors_config(self):
        """
        get a dataframe with columns :
        - home_id, phase, fluksid, sensor_id, sensor_token, net, con, pro
        """
        return self.sconfig_df

    def get_home_config(self, hid):
        """
        Getter for the configuration of the home hid.

        :param hid: String of the home id.

        :return:    Return a subset of the configuration with only
                    the configuration of the home hid.
        """
        return self.sconfig_df[self.sconfig_df['home_id'] == hid]

    def get_home_sensors(self):
        """
        return a dictionary with
        key : home id, value : list of sensor ids
        """
        ids = {}
        for hid, home in self.sconfig_df.groupby("home_id"):
            ids[hid] = list(home.index)

        return ids

    def get_ids(self):
        return self.ids
