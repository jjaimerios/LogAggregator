#!/usr/bin/python3

# NOT QUITE WORKING - NEEDS WORK
# IMPORTS TO AIRFLOW - but does not successfully run

# Script to read in apache log file and write it to a SQL database table.
#
# Log line:
# ['[Sun', 'Dec', '04', '04:47:44', '2005]', '[notice]', 'workerEnv.init()', 'ok', '/etc/httpd/conf/workers2.properties']
# ------------------------------------------------------------------------

# Airflow libraries
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

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
# Input Example: Sun Dec 04 04:47:44 2005
# Output Example: 2005-12-04 04:47:44
def formatDateTime(recDate):
    time_date = re.split(' ', recDate)[4].replace("]","") + "-" +\
    dag_libs.getNumericMonth(re.split(' ', recDate)[1]) + "-" +\
    re.split(' ', recDate)[2] + " " +\
    re.split(' ', recDate)[3]

    return time_date

@task()
def extract():
    try:
        # Get log file to read from config file
        apache_log = config.get('Log_Files', 'apache_log_file')

        # Open a file handle
        fh = open(apache_log, "r")

        return fh

    except Exception as Argument:
        input_log_name = config.get('Log_Files', 'apache_log_file')
        airflow_log = os.path.basename(input_log_name) + '.airflow.log'
        dag_libs.process_exception(Argument,airflow_log)

@task()
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
        app_rec_num = db_funcs.get_max_val(conn, tbl_name, "APP_REC")
        if str(app_rec_num) == 'None':
            app_rec_num = 0

        conn.close()

        # Init list to hold cleansed data
        clean_list = []

        # Loop through input log file
        while True:
            # Print one line
            one_line = ifh.readline()

            # Leave loop if no more records
            if not one_line:
                break

            # Increment record number
            rec_id_num += 1
            app_rec_num += 1

            # Find any values enclosed in brackets
            in_brackets = re.findall(r'\[.*?\]', str(one_line))
            rec_date = in_brackets[0]

            # Remove brackets from message type
            msg_type = re.sub('[\]\[]', '', in_brackets[1])

            # https://stackoverflow.com/questions/17284947/regex-to-get-all-text-outside-of-brackets
            # Find elements outside any brackets
            message1 = re.findall(r'([^[\]]+)(?:$|\[)', one_line)[1].strip()
            message = message1.replace("'", "") # Remove single quotes
            #message = re.sub('[\)\(]', '', message1)

            # Get time/date
            time_date = formatDateTime(rec_date)

            # Get day of week
            day = re.split(' ', rec_date)[0].replace("[","")

            # Set component
            component = 'None'

            # Insert record into database table
            clean_rec_list = [rec_id_num, time_date, day, msg_type, app_rec_num, component, message]
            clean_list.append(clean_rec_list)

        # Close file handle
        ifh.close()

        return clean_list

    except Exception as Argument:
        input_log_name = config.get('Log_Files', 'apache_log_file')
        airflow_log = os.path.basename(input_log_name) + '.airflow.log'
        dag_libs.process_exception(Argument,airflow_log)

@task()
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
            db_funcs.insertTableRec(conn, recNum, timeDate, dayx, msgType, str(app_rec), component, msgx, tbl_name)

        # Close database connection
        conn.close()

    except Exception as Argument:
        input_log_name = config.get('Log_Files', 'apache_log_file')
        airflow_log = os.path.basename(input_log_name) + '.airflow.log'
        dag_libs.process_exception(Argument,airflow_log)

with DAG('load_apache_log_dag',
         schedule_interval='@once',
         start_date=datetime(2023, 4, 10),
         catchup=False) as dag:
    extract_data = extract()
    transform_data = transform(extract_data)
    load_data = load(transform_data)

    extract_data >> transform_data >> load_data
