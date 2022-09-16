__title__ = "preprocessFluksoSensors"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"


"""
Script to compare 2 specific config files : 
- 2 arguments : 2 config excel file paths of same format
- file path name format : 'ConfigurationNN_YYYYmmdd.xlsx'
- Show which home ids are new, which sensors ids are new
"""

# standard library
import argparse
import sys

# 3rd party packages
import pandas as pd

# local source
import pyToCassandra as ptc
from sensorConfig import Configuration
from constants import CASSANDRA_KEYSPACE
from utils import getLastRegisteredConfig
from preprocessFluksoSensors import getCompactSensorDF



def compare_configs(old_config, new_config):
	""" 
	Go through the 2 config home ids and sensors ids
	and detect new changes
	"""

	print("Old config : ")
	print(old_config)
	print("New config : ")
	print(new_config)

	for hid, sids in old_config.getHomeSensors().items():
		print("{} : ".format(hid), end="")
		if hid in new_config.getHomeSensors():
			new_sids = new_config.getHomeSensors()[hid]
			if set(sids) == set(new_sids):
				print("Same sensor ids")
			else:
				print("New sensors ids : ")
				# sensors ids from new config not present in the other config
				print([sid for sid in new_sids if sid not in sids])
		else:
			print("New home")


def get_configs(cassandra_session, config_old_path, config_new_path, now):
	""" 
	We can compare either 2 configs from 2 excel files or
	compare the last registered configuration in the Cassandra database with
	a excel config file
	"""
	old_config = None
	if not config_old_path:
		old_config = getLastRegisteredConfig(cassandra_session)
	else:
		old_config = Configuration(
			now, 
			getCompactSensorDF(config_old_path).set_index("sensor_id")
		)
	new_config = Configuration(
		now, 
		getCompactSensorDF(config_new_path).set_index("sensor_id")
	)
	
	return old_config, new_config


def process_arguments():

	argparser = argparse.ArgumentParser(
		description=__doc__,
		formatter_class=argparse.RawDescriptionHelpFormatter,
	)

	argparser.add_argument(
		"--config1", 
		type=str,
		help="Path to a config excel file. Format : ConfigurationNN_YYYYmmdd.xlsx"
	)

	argparser.add_argument(
		"--config2",
		type=str,
		help="Path to a config excel file. Format : ConfigurationNN_YYYYmmdd.xlsx"
	)

	return argparser



def main():
	# > arguments
	argparser = process_arguments()
	args = argparser.parse_args()
	config_old_path = args.config1
	config_new_path = args.config2

	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	now = pd.Timestamp.now(tz="CET")
	
	if config_new_path:
		old_config, new_config = get_configs(
			cassandra_session,
			config_old_path, 
			config_new_path, now
		)

		compare_configs(old_config, new_config)

	else:
		print("Please provide 2 valid config paths")
		exit(1)


if __name__ == "__main__":
    main()