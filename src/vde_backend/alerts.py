__title__ = "alerts"
__version__ = "2.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"

"""
Script to trigger an alert whenever something went wrong in the power data
- Every X time, we query the Cassandra database (power table) and we check
    - if there is no data arriving since a certain amount of time
    - if some signs are incorrect/incoherent 
        - if we see negative consumption values
        - if we see positive production values
"""

# standard library
import argparse

# 3rd party packages
import pandas as pd
from datetime import timedelta

# local source
from utils import(
    get_last_registered_config,
    get_home_power_data_from_cassandra
)

SIGN_THRESHOLD = 15
MISSING_ALERT_THRESHOLD = 10


def get_mail_text(problem_title, threshold, legend, to_alert, date):
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


def send_mail(mail_filename):

    # TODO : only send mail if there is an alert containing issues
    pass 



def check_missing(home_id, date):
    """ 
    Check if there are a lot of missing data in one day of power data for a home
    """
    home_df = get_home_power_data_from_cassandra(home_id, date)

    count_zero = 0
    if len(home_df) > 0:

        # create column with the sum of p_cons, p_prod and p_tot columns
        # if the total is 0, then it means there is no raw data for the sensors of this home
        home_df['s'] = home_df['p_cons'] + home_df['p_prod'] + home_df['p_tot']
        count_zero = (home_df['s'] == 0).sum()  # get the number of 0 in the sum column
        
    return count_zero, len(home_df)


def check_signs(home_id, date):
    """ 
    Check if the signs are coherent in power data based on 2 criterion : 
    Signs are incorrect if there are :
    - negative consumption values
    - positive production values
    """
    home_df = get_home_power_data_from_cassandra(home_id, date)

    ok = True
    if len(home_df) > 0:
        cons_neg = (home_df["p_cons"] < SIGN_THRESHOLD).any()
        prod_pos = (home_df["p_prod"] > SIGN_THRESHOLD).any()
        maxi = home_df.max()
        mini = home_df.min()
        if cons_neg or prod_pos:
            ok = False
            info = {
                "cons_neg": {
                    "status": cons_neg,
                    "max": maxi["p_cons"],
                    "min": mini["p_cons"]
                },
                "prod_pos": {
                    "status": prod_pos,
                    "max": maxi["p_prod"],
                    "min": mini["p_prod"]
                }
            }

    return ok, info


def get_homes_with_missing_data(config, yesterday):
    """ 
    For each home, check if power data has a lot of missing data, 
    if the percentage of missing data is non negligeable, we send an alert by email
    """
    
    to_alert = {}
    for home_id in config.get_ids().keys():
        nb_zeros, tot_len = check_missing(home_id, yesterday)

        percentage = 0
        if nb_zeros > 0:
            percentage = (100 * nb_zeros) / tot_len
        if percentage >= MISSING_ALERT_THRESHOLD:  # if at least 80% of the rows are 0s, then alert
            to_alert[home_id] = round(percentage, 1) 
    
    return to_alert


def get_homes_with_incorrect_signs(config, yesterday):
    """ 
    For each home, we check if power data are correct w.r.t the signs
    If some signs are incorrect, we send an alert by email
    """
    to_alert = {}
    for home_id in config.get_ids().keys():
        ok, info = check_signs(home_id, yesterday)
        if not ok:
            to_alert[home_id] = info

    return to_alert


def get_yesterday(now):
    """ 
    Given the timestamp of today, get the previous day's date.
    -> YYYY-MM-DD
    """

    yesterday = now.date()-timedelta(days=1)

    return yesterday


def write_mail_to_file(mail_content, filename):
    """ 
    Write mail content to a simple txt file
    """
    with open(filename, 'w') as f:
        f.write(mail_content)


# ========================================================================================

def process_arguments():
    """
    process arguments 
    argument : what to monitor : missing data or signs
    """
    argparser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    argparser.add_argument(
        "--mode", type=str, 
        default="missing",
        help="missing : send alert when too much missing data; "
            "sign : send alert when incorrect signs")

    return argparser


def main():

    argparser = process_arguments()
    args = argparser.parse_args()
    mode = args.mode

    last_config = get_last_registered_config()
    if last_config:
        now = pd.Timestamp.now(tz="CET")
        yesterday = get_yesterday(now)
        print("yesterday : ", yesterday)

        if mode == "missing":
            to_alert = get_homes_with_missing_data(last_config, yesterday)
            if len(to_alert) > 0:
                threshold = "{} %".format(MISSING_ALERT_THRESHOLD)
                legend = "'home id' > percentage of missing data for 1 day"
                mail_content = get_mail_text(
                    "There is missing data", 
                    threshold, legend, to_alert, yesterday)
                print(mail_content)
                write_mail_to_file(mail_content, "alert_missing.txt")
                send_mail("alert_missing.txt")

        elif mode == "sign":
            to_alert = get_homes_with_incorrect_signs(last_config, yesterday)
            if len(to_alert) > 0:
                threshold = "{} ".format(SIGN_THRESHOLD)
                legend = "'home id ' > \n"
                legend += "{'cons_neg = is there any negative consumption values ?', \n"
                legend += "'prod_pos = is there any positive production values ?'}}"
                mail_content = get_mail_text(
                    "There are incorrect signs", 
                    threshold, legend, to_alert, yesterday)
                print(mail_content)
                write_mail_to_file(mail_content, "alert_signs.txt")
                send_mail("alert_signs.txt")
                
    print("No registered config in db.")



if __name__ == "__main__":
    main()
