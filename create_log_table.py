#!/usr/bin/python3

# Script to create a new table
# ------------------------------------------------------------------------

# sys module to accept command line arguments
import sys

# Import custom libraries
sys.path.append('/home/jrios/airflow/dags/my_libs/')
import db_funcs

# Import application properties library and set config file variables
import configparser
config = configparser.RawConfigParser()
config.read('/home/jrios/airflow/etc/config.props') # Property file with app variables

# ------------------------------------------------------------------------

# Main function
def main():
    # Get command line arguments
    # tbl_name = sys.argv[1]

    # Get table name from config file
    tbl_name = config.get('Db_Info', 'log_table')
    #print(tbl_name)

    # Establish connection
    conn = db_funcs.connect_db("testdb", "psql_user", "password", "127.0.0.1", "5432")

    # Create cursor object
    cursor = conn.cursor()

    # Check if db exists
    result = db_funcs.table_exists(conn, tbl_name)
    print(result)

    if str(result) == 'False':
        db_funcs.create_log_table(conn, tbl_name)

    # Close connection
    conn.close()

    print ("Complete")

# Call main
if __name__ == "__main__":
    main()
