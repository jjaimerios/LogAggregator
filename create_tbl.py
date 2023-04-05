#!/usr/bin/python3

# Script to create a new table
# ------------------------------------------------------------------------

# sys module to accept command line arguments
import sys

# Call custom db functions module
import db_funcs

# ------------------------------------------------------------------------

# Main function
def main():
    # Get command line arguments
    tbl_name = sys.argv[1]
    #print(tbl_name)

    # Establish connection
    conn = db_funcs.connect_db("testdb", "psql_user", "password", "127.0.0.1", "5432")

    # Create cursor object
    cursor = conn.cursor()

    # Check if db exists
    result = db_funcs.table_exists(conn, tbl_name)
    #print(result)

    if result != 'True':
        print("Creating table")
        # Prep query to create table
        sql = "CREATE TABLE " + tbl_name + \
              "(ID INT PRIMARY KEY     NOT NULL, " \
              "APP           TEXT    NOT NULL, " \
              "DATE          TEXT    NOT NULL, " \
              "EVENT         TEXT     NOT NULL, " \
              "MESSAGE       CHAR(100));"
        #$print(sql)

        # Run query
        cursor.execute(sql)

    # Close connection
    conn.close()

    print ("Complete")

# Call main
if __name__ == "__main__":
    main()
