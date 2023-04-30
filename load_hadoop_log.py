#!/usr/bin/python3

# Script to read in hadoop log file and write it to a SQL database table.
#
# Log line:
# 2015-10-18 18:01:47,978 INFO [main] org.apache.hadoop.mapreduce.v2.app.MRAppMaster: Created MRAppMaster for application appattempt_1445144423722_0020_000001
# Log retrieved from https://github.com/logpai/loghub

# ------------------------------------------------------------------------

# Import regex package
import re

# OS commands for parsing file/path
import sys

# Import custom libraries
sys.path.append('/home/jrios/airflow/dags/my_libs/')
import db_funcs

# Import application properties library and set config file variables
import configparser
config = configparser.RawConfigParser()
config.read('/home/jrios/airflow/etc/config.props') # Property file with app variables

# ------------------------------------------------------------------------

# Format date / time
# Input Example: 2015-10-18 18:01:47,978
# Output Example: 2015-10-18 18:01:47
def formatDateTime(recDate):
    date_split = re.split(',', recDate)
    time_date = date_split[0]

    return time_date

def extract():
    try:
        # Get log file to read from config file
        hadoop_log = config.get('Log_Files', 'hadoop_log_file')

        # Open a file handle
        fh = open(hadoop_log, "r")

        return fh

    except Exception as Argument:
        input_log_name = config.get('Log_Files', 'hadoop_log_file')
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
        app_rec_num = db_funcs.get_max_recnum(conn, tbl_name, "APP_REC", 'Hadoop')
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

            # Find any values enclosed in brackets - which is just component
            in_brackets = re.findall(r'\[.*?\]', str(one_line))

            # Remove brackets from component
            component = re.sub('[\]\[]', '', in_brackets[0])

            # Break log line on spaces
            rec_split = re.split(' ', one_line)

            # Get message type
            msg_type = rec_split[2]

            # Get message - grab everything after 4th space in line;
            # remove single quotes; limit to 250 bytes
            #message1 = rec_split[4:]
            #message = "\"" + re.sub('[\]\[]', '', str(message1)) + "\""
            message = one_line.split('] ', 1)[-1].replace("'", "")[:250]

            # Build date
            raw_date = rec_split[0] + ' ' + rec_split[1]
            time_date = formatDateTime(raw_date)

            # Get day of week
            day = "None"

            # Insert record into database table
            clean_rec_list = [rec_id_num, time_date, day, msg_type, app_rec_num, component, message]
            clean_list.append(clean_rec_list)

        # Close file handle
        ifh.close()

        return clean_list

    except Exception as Argument:
        input_log_name = config.get('Log_Files', 'hadoop_log_file')
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
            #print(msgx)
            # Insert record into database table
            db_funcs.insertTableRec(conn, recNum, 'Hadoop', timeDate, dayx, msgType, str(app_rec), component, msgx, tbl_name)

        # Close database connection
        conn.close()

    except Exception as Argument:
        input_log_name = config.get('Log_Files', 'hadoop_log_file')
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
