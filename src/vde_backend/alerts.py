__title__ = "alerts"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"

"""
Script to trigger an alert whenever something went wrong in the power data
- Every X time, we query the Cassandra database (power table) and we check
    - if there is no data arriving since a certain amount of time
    - if some signs are incorrect/incoherent 
        - if we see negative consumption values
        - if we see positive production values
        - if we see photovoltaic values during the night
"""

# standard library
import argparse

# 3rd party packages
import pandas as pd
from datetime import timedelta

# local source
import pyToCassandra as ptc
from utils import getLastRegisteredConfig
from constants import (
	TBL_POWER, 
	CASSANDRA_KEYSPACE
)


SIGN_THRESHOLD = 15
MISSING_ALERT_THRESHOLD = 10


def getMailText(problem_title, threshold, legend, to_alert, date):
    """ 
    create a alert txt with specific details (to send a mail)
    """
    txt = "- Alert - {} \n".format(problem_title)
    txt += "- Threshold : {} \n".format(threshold)
    txt += "- Legend : {} \n".format(legend)
    txt += "- Date : {} \n".format(date)
    txt += "------------------------------------------------------\n"
    for hid, values in to_alert.items():
        txt += "{} > {}\n".format(hid, values)
    
    return txt 


def sendMail(mail_filename):

    # TODO : only send mail if there is an alert containing issues
    pass 


def getHomePowerDataFromCassandra(cassandra_session, home_id, date, table_name):
	""" 
	Get power data from Power table in Cassandra
	> for 1 specific home
	> for 1 specific day
	"""

	where_clause = "home_id = {} and day = '{}'".format("'"+home_id+"'", date)
	cols = ["home_id", "day", "ts", "config_id", "p_cons", "p_prod", "p_tot"]
	home_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name, cols, where_clause, "ALLOW FILTERING", "")

	return home_df


def checkMissing(cassandra_session, home_id, date, table_name):
    """ 
    Check if there are a lot of missing data in one day of power data for a home
    """
    home_df = getHomePowerDataFromCassandra(cassandra_session, home_id, date, table_name)

    count_zero = 0
    if len(home_df) > 0:

        # create column with the sum of p_cons, p_prod and p_tot columns
        # if the total is 0, then it means there is no raw data for the sensors of this home
        home_df['s'] = home_df['p_cons'] + home_df['p_prod'] + home_df['p_tot']
        count_zero = (home_df['s'] == 0).sum()  # get the number of 0 in the sum column
        
    return count_zero, len(home_df)


def checkSigns(cassandra_session, home_id, date, table_name):
    """ 
    Check if the signs are coherent in power data based on 2 criterion : 
    Signs are incorrect if :
    - negative consumption values
    - positive production values
    """
    home_df = getHomePowerDataFromCassandra(cassandra_session, home_id, date, table_name)

    ok = True
    info = {}
    if len(home_df) > 0:
        for config_id, power_df in home_df.groupby("config_id"):
            # print(power_df.head(1))
            cons_neg = (power_df["p_cons"] < SIGN_THRESHOLD).any()
            prod_pos = (power_df["p_prod"] > SIGN_THRESHOLD).any()
            maxi = home_df.max()
            mini = home_df.min()
            
            if cons_neg or prod_pos:
                # print("cons_neg : {}, prod_pos : {}".format(cons_neg, prod_pos))
                ok = False
                info[str(config_id)] = {"cons_neg": {"status": cons_neg, "max": maxi["p_cons"], "min": mini["p_cons"]}, \
                                        "prod_pos": {"status": prod_pos, "max": maxi["p_prod"], "min": mini["p_prod"]}}

    # print(info)
    return ok, info


def getHomesWithMissingData(cassandra_session, config, yesterday):
    """ 
    For each home, check if power data has a lot of missing data, 
    if the percentage of missing data is non negligeable, we send an alert by email
    """
    
    to_alert = {}
    ids = config.getIds()
    for home_id, sensor_ids in ids.items():
        # print(home_id)
        nb_zeros, tot_len = checkMissing(cassandra_session, home_id, yesterday, TBL_POWER)

        percentage = 0
        if nb_zeros > 0:
            percentage = (100 * nb_zeros) / tot_len
        # print("nb 0 : {}, tot len : {}, {}%".format(nb_zeros, tot_len, percentage))
        if percentage >= MISSING_ALERT_THRESHOLD:  # if at least 80% of the rows are 0s, then alert
            to_alert[home_id] = round(percentage, 1) 
    
    # print(to_alert)
    return to_alert


def getHomesWithIncorrectSigns(cassandra_session, config, yesterday):
    """ 
    For each home, we check if power data are correct w.r.t the signs
    If some signs are incorrect, we send an alert by email
    """
    to_alert = {}
    ids = config.getIds()
    for home_id in ids.keys():
        # print(home_id)
        ok, info = checkSigns(cassandra_session, home_id, yesterday, TBL_POWER)
        if not ok:
            to_alert[home_id] = info

    return to_alert


def getYesterday(now):
    """ 
    Given the timestamp of today, get the previous day's date.
    -> YYYY-MM-DD
    """

    yesterday = now.date()-timedelta(days=1)

    return yesterday


def writeMailToFile(mail_content, filename):
    """ 
    Write mail content to a simple txt file
    """
    with open(filename, 'w') as f:
        f.write(mail_content)


# ========================================================================================

def processArguments():
	"""
	process arguments 
	argument : what to monitor : missing data or signs
	"""
	argparser = argparse.ArgumentParser(
		description=__doc__,
		formatter_class=argparse.RawDescriptionHelpFormatter,
	)
	argparser.add_argument("--mode", type=str, default="missing",
						   help="missing : send alert when too much missing data; "
                                "sign : send alert when incorrect signs")

	return argparser


def main():

    argparser = processArguments()
    args = argparser.parse_args()
    mode = args.mode

    cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

    last_config = getLastRegisteredConfig(cassandra_session)
    now = pd.Timestamp.now()
    yesterday = getYesterday(now)  # TODO : handle multiple past dates
    print("yesterday : ", yesterday)

    if mode == "missing":
        to_alert = getHomesWithMissingData(cassandra_session, last_config, yesterday)
        if len(to_alert) > 0:
            threshold = "{} %".format(MISSING_ALERT_THRESHOLD)
            legend = "'home id' > percentage of missing data for 1 day"
            mail_content = getMailText("There is missing data", threshold, legend, to_alert, yesterday)
            print(mail_content)
            writeMailToFile(mail_content, "alert_missing.txt")
            sendMail("alert_missing.txt")
    elif mode == "sign":
        to_alert = getHomesWithIncorrectSigns(cassandra_session, last_config, yesterday)
        if len(to_alert) > 0:
            threshold = "> {} ".format(SIGN_THRESHOLD)
            legend = "'home id ' > {'config id (insertion time): ': \n"
            legend += "{'cons_neg = is there any negative consumption values ?', \n"
            legend += "'prod_pos = is there any positive production values ?'}}"
            mail_content = getMailText("There are incorrect signs", threshold, legend, to_alert, yesterday)
            print(mail_content)
            writeMailToFile(mail_content, "alert_signs.txt")
            sendMail("alert_signs.txt")



if __name__ == "__main__":
    main()
