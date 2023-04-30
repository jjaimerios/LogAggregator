#!/usr/bin/python3

# Script to read in hdfs log file and write it to a SQL database table.
#
# Log line:
# 081109 203615 148 INFO dfs.DataNode$PacketResponder: PacketResponder 1 for block blk_38865049064139660 terminating
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
# Input Example: 081109
# Output Example: 2008-11-09
def formatDateTime(recDate):
    time_date = "20" + recDate[0:2] + "-" + \
            recDate[2:4] + "-" + \
            recDate[4:6]

    return time_date

def extract():
    try:
        # Get log file to read from config file
        hdfs_log = config.get('Log_Files', 'hdfs_log_file')

        # Open a file handle
        fh = open(hdfs_log, "r")

        return fh

    except Exception as Argument:
        input_log_name = config.get('Log_Files', 'hdfs_log_file')
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
        app_rec_num = db_funcs.get_max_recnum(conn, tbl_name, "APP_REC", 'HDFS')
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

            # Get time/dat - first 6 characters of log line
            rec_date = one_line[0:6]
            time_date = formatDateTime(rec_date)

            # Get day of week
            day = 'None'

            # Get message type - split line on space and get 4th element
            rec_split = re.split(' ', one_line)
            msg_type = rec_split[3]

            # Set component - 5th element; remove colon
            component = rec_split[4].replace(":", "")

            # Get message - everything after 1st colon; limit to 250 chars
            message = one_line.split(': ', 1)[-1][:250]

            # Insert record into database table
            clean_rec_list = [rec_id_num, time_date, day, msg_type, app_rec_num, component, message]
            clean_list.append(clean_rec_list)

        # Close file handle
        ifh.close()

        return clean_list

    except Exception as Argument:
        input_log_name = config.get('Log_Files', 'hdfs_log_file')
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
            db_funcs.insertTableRec(conn, recNum, 'HDFS', timeDate, dayx, msgType, app_rec, component, msgx, tbl_name)

        # Close database connection
        conn.close()

    except Exception as Argument:
        input_log_name = config.get('Log_Files', 'hdfs_log_file')
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
