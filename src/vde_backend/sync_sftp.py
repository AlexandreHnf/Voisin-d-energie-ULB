__title__ = "sync_sftp"
__version__ = "2.0.0"
__author__ = "Alexandre Heneffe, and Guillaume Levasseur"
__license__ = "MIT"


"""
Script to send Flukso electrical data to sftp

Constraints :
    - the data is not always complete when querying at a certain time for 2 reasons:
        1) because of missing data that may be recovered after an undetermined amount
        of time by the system.
        2) if the backend system used to populate the database with data is currently
        executing a query while this script is running, then the 5 last minutes of data
        may not end up in this script's query (because the backend system query data
        every 5 minutes).

    - The script has to be executed a bit after a 'half day time':
    for example: after noon, or after midnight because the database table works by chunk
    composed of 1 day of data. It also allows to have consistent chunks of data that follow
    well. To avoid having too much delay, execute the script the closest to a half time
    as possible.

    - We save 1 csv file per home, otherwise, fusing all homes in one file can cause memory
    issues (since the number of homes can grow in time).

    - the files already sent to the sftp server in the /upload/ folder must remain in that
    folder because the system depends on those files to determine which date to query. Or
    at least, the most important files are the last ones for each home.
        - solution to become independent on the server : locally save the last sent files
        and check locally
"""


# standard library
from datetime import timedelta
import os
import os.path
import argparse

# 3rd party packages
import pandas as pd

import paramiko

# local sources
from constants import (
    PROD,
    SFTP_LOCAL_PATH,
    CASSANDRA_KEYSPACE,
    TBL_POWER
)
import py_to_cassandra as ptc

from utils import (
    logging,
    get_dates_between,
    get_last_registered_config,
    get_home_power_data_from_cassandra
)


# ==============================================================================

AM = "<="
PM = ">"

NOON = "10:00:00.000000+0000"  # in UTC = 12:00:00 in CET


def get_date_to_query(now):
    """
    Based on the timestamp of the moment the query is triggered,
    determine which day to query
    """
    midnight = pd.Timestamp("00:00:00")
    nb_hours_today = round((now - midnight).total_seconds() / (60.0 * 60.0), 1)

    # if we are in the PM part, we take data from the first half of today
    date = str(now.date())
    moment = AM  # first half : AM
    moment_now = PM  # the moment of today

    if nb_hours_today <= 12.0:  # AM
        # we are in the AM part, so we take data from the second half of the previous day
        date = str(now.date() - timedelta(days=1))  # previous day
        moment = PM  # second half : PM
        moment_now = AM

    return date, moment, moment_now


def get_csv_filename(home_id, date, moment):
    part = "AM" if moment == AM else "PM"
    return "{}_{}_{}.csv".format(home_id, date, part)


def save_data_to_csv(data_df, csv_filename):
    """
    Save to csv
    """

    outdir = SFTP_LOCAL_PATH
    if not os.path.exists(outdir):
        os.mkdir(outdir)
    filepath = os.path.join(outdir, csv_filename)

    data_df.to_csv(filepath)

    logging.debug("Successfully Saved flukso data in csv")


def get_sftp_session(sftp_info):
    """
    connect to the sftp server
    """

    transport = paramiko.Transport((
        sftp_info["host"],
        sftp_info["port"]
    ))

    transport.connect(
        username=sftp_info["username"],
        password=sftp_info["password"]
    )

    sftp = paramiko.SFTPClient.from_transport(transport)

    return sftp


def list_files_sftp(sftp_session):
    """
    return the list of
    """

    sftp_filenames = []

    for filename in sftp_session.listdir('/upload/'):
        sftp_filenames.append(filename)
        print(filename)
        logging.info(filename)

    return sftp_filenames


def get_last_date(sftp_session, home_id):
    """
    Get the last filename sent to the sftp server in order
    to know which date to start the new query from.
    """

    latest = 0
    latest_file = None
    latest_date = None

    for fileattr in sftp_session.listdir_attr('/upload/'):
        if fileattr.filename.startswith(home_id) and fileattr.st_mtime > latest:
            latest = fileattr.st_mtime
            latest_file = fileattr.filename

    if latest_file is not None:
        latest_date = pd.Timestamp(latest_file.split("_")[1])

    return latest_date


def send_file_to_sftp(sftp_session, filename, sftp_info):
    """
    Send csv file to the sftp server
    """

    dest_path = sftp_info["destination_path"] + filename
    local_path = os.path.join(SFTP_LOCAL_PATH, filename)

    sftp_session.put(local_path, dest_path)

    if PROD:
        os.remove(local_path)


def get_moments(dates, default_moment):
    """
    Given a list of dates, define the moments of each day to query
    one moment = AM or PM
    """

    moments = {}
    for i in range(len(dates)):
        date = dates[i]
        if i == len(dates) - 1:  # if last date
            if default_moment == AM:
                moments[date] = []
            elif default_moment == PM:
                moments[date] = [AM]
        else:
            moments[date] = [AM, PM]

    return moments


def get_all_history_dates(home_id, table_name, now):
    """
    For a home, get the first timestamp available in the db, and
    from that first date, return the list of dates until now.
    """

    # get first date available for this home
    where_clause = "home_id = '{}'".format(home_id)
    cols = ["day"]
    date_df = ptc.select_query(
        CASSANDRA_KEYSPACE,
        table_name,
        cols,
        where_clause,
        limit=1,
        allow_filtering=True,
        distinct=False
    )

    all_dates = []
    if len(date_df) > 0:
        first_date = pd.Timestamp(date_df.iat[0, 0])

        all_dates = get_dates_between(first_date, now)

    return all_dates


def process_all_homes(sftp_session, config, default_date, moment, moment_now, now, sftp_info):
    """
    send 1 csv file per home, per day moment (AM or PM) to the sftp server
    - if no data sent for this home yet, we send the whole history
    - otherwise, we send data from the last sent date to now
    """

    ids = config.get_ids()
    for home_id in ids.keys():
        latest_date = get_last_date(sftp_session, home_id)
        all_dates = [default_date]
        moments = {default_date: [moment]}
        if latest_date is None:  # history
            all_dates = get_all_history_dates(home_id, TBL_POWER, now)
            moments = get_moments(all_dates, moment_now)
        else:					 # realtime
            all_dates = get_dates_between(latest_date, now)
            moments = get_moments(all_dates, moment_now)

        for date in moments:
            for moment in moments[date]:
                csv_filename = get_csv_filename(home_id, date, moment)
                logging.debug(csv_filename)
                home_data = get_home_power_data_from_cassandra(
                    home_id,
                    date,
                    moment,
                    TBL_POWER,
                    ts_clause="AND ts {} '{} {}'".format(moment, date, NOON)
                )

                # first save csv locally
                save_data_to_csv(home_data.set_index("home_id"), csv_filename)
                if PROD:
                    # then, send to sftp server
                    send_file_to_sftp(sftp_session, csv_filename, sftp_info)

        logging.debug("-----------------------")


def process_arguments():
    """
    process arguments
    argument : sftp config filename
        -> contains host, port, username, password and destination path
    """
    argparser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    argparser.add_argument(
        "credentials_filename",
        type=str,
        help="sftp config file"
    )

    return argparser


def main():

    argparser = process_arguments()
    args = argparser.parse_args()
    sftp_info_filename = args.credentials_filename

    sftp_info = ptc.load_json_credentials(sftp_info_filename)
    sftp_session = get_sftp_session(sftp_info)

    config = get_last_registered_config()

    if config:
        now = pd.Timestamp.now()
        default_date, moment, moment_now = get_date_to_query(now)

        logging.debug("config id : " + str(config.get_config_id()))
        logging.debug("date : " + default_date)
        logging.debug("moment : " + moment)
        logging.debug("moment now : " + moment_now)

        process_all_homes(
            sftp_session,
            config,
            default_date,
            moment,
            moment_now,
            now,
            sftp_info
        )
    else:
        logging.debug("No registered config in db.")


if __name__ == "__main__":
    main()
