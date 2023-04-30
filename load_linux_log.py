#!/usr/bin/python3

# Script to read in linux log file and write it to a SQL database table.
#
# Log line:
# Jun 14 15:16:01 combo sshd(pam_unix)[19939]: authentication failure; logname= uid=0 euid=0 tty=NODEVssh ruser= rhost=218.188.2.4 
# Log retrieved from https://github.com/logpai/loghub

# ------------------------------------------------------------------------

# Import regex package
import re

# OS commands for parsing file/path
import os
import sys

# Sys date library
from datetime import datetime

# Import custom libraries
sys.path.append('/home/jrios/airflow/dags/my_libs/')
import dag_libs
import db_funcs

# Import application properties library and set config file variables
import configparser
config = configparser.RawConfigParser()
config.read('/home/jrios/airflow/etc/config.props') # Property file with app variables

# ------------------------------------------------------------------------

# Format date / time
# Input Example: Jun 14 15:16:01
# Output Example: 2022-06-14 15:16:01
def formatDateTime(recDate):
    # Get current year
    # going to fake it and plug last year so I dont hit future dates
    # import datetime
    # year = datetime.date.today().year # need account for new year
    year = '2022'
    month = dag_libs.getNumericMonth(recDate[0:3])
    day = recDate[4:6].replace(" ","0")
    time = recDate[7:15]

    time_date = year + "-" + month + "-" + \
            day + " " + time

    return time_date

def extract():
    try:
        # Get log file to read from config file
        linux_log = config.get('Log_Files', 'linux_log_file')

        # Open a file handle
        fh = open(linux_log, "r")

        return fh

    except Exception as Argument:
        input_log_name = config.get('Log_Files', 'linux_log_file')
        airflow_log = os.path.basename(input_log_name) + '.airflow.log'
        dag_libs.process_exception(Argument,airflow_log)

def transform(ifh):
    try:
        # Establish database connection
        conn = db_funcs.connect_db("testdb", "psql_user", "password", "127.0.0.1", "5432")

        # Get table name from config file
        tbl_name = config.get('Db_Info', 'log_table')

        # Get max db rec num
        rec_id_num = db_funcs.get_max_val(conn, tbl_name, "ID")
        if str(rec_id_num) == 'None':
            rec_id_num = 0

        # Get max app rec num
        app_rec_num = db_funcs.get_max_recnum(conn, tbl_name, "APP_REC", 'Linux')
        if str(app_rec_num) == 'None':
            app_rec_num = 0
        orig_rec_num = app_rec_num

        conn.close()

        # Init list to hold cleansed data
        clean_list = []

        # Loop through input log file
        current_rec = 0
        while True:
            # Print one line
            one_line = ifh.readline()

            # Leave loop if no more records
            if not one_line:
                break

            # Next rec if rec num already in db
            current_rec += 1
            if current_rec <= app_rec_num:
                continue

            # Increment record number
            rec_id_num += 1
            app_rec_num += 1

            # Get time/date
            rec_date =  one_line[0:15]
            time_date = formatDateTime(rec_date)

            # Get day of week
            day = "None"

            # Remove brackets from message type
            rec_split = re.split(' ', one_line)
            msg_type = rec_split[3]

            if str(msg_type.isalpha()) == 'False':
                msg_type = "None"

            # Set component - 5th element; remove colon
            component = rec_split[4].replace(":", "")

            # Get message - everything after 1st colon; limit to 250 chars
            message = one_line.split(': ', 1)[-1].replace("'", "")[:250]

            # Insert record into database table
            clean_rec_list = [rec_id_num, time_date, day, msg_type, app_rec_num, component, message]
            clean_list.append(clean_rec_list)

        # Close file handle
        ifh.close()

        return clean_list

    except Exception as Argument:
        input_log_name = config.get('Log_Files', 'linux_log_file')
        airflow_log = os.path.basename(input_log_name) + '.airflow.log'
        dag_libs.process_exception(Argument,airflow_log)

def load(clean_list):
    try:
        # Get table name from config file
        tbl_name = config.get('Db_Info', 'log_table')

        # Establish database connection
        conn = db_funcs.connect_db("testdb", "psql_user", "password", "127.0.0.1", "5432")

        for main_rec in clean_list:
            #print(main_rec)
            recNum = main_rec[0]
            timeDate = main_rec[1]
            dayx = main_rec[2]
            msgType = main_rec[3]
            app_rec = main_rec[4]
            component = main_rec[5]
            msgx = main_rec[6]
            # Insert record into database table
            db_funcs.insertTableRec(conn, recNum, 'Linux', timeDate, dayx, msgType, app_rec, component, msgx, tbl_name)

        # Close database connection
        conn.close()

    except Exception as Argument:
        input_log_name = config.get('Log_Files', 'linux_log_file')
        airflow_log = os.path.basename(input_log_name) + '.airflow.log'
        dag_libs.process_exception(Argument,airflow_log)

# Main function
def main():

    # Extract data - get open file handle
    ifh = extract()
    # one_line = ifh.readline()
    # print(one_line)

    # Clean data - get clean list
    clean_data = transform(ifh)

    # Load data to postgresql db
    loaded_data = load(clean_data)

    #print("SUCCESS")

# Call main
if __name__ == "__main__":
    main()
