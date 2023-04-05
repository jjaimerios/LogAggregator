#!/usr/bin/python3

# Script to create a new database
# ------------------------------------------------------------------------

# sys module to accept command line arguments
import sys

# Call custom db functions module
import db_funcs

# ------------------------------------------------------------------------

# Main function
def main():
    # Get command line arguments
    #num_args = len(sys.argv)
    #arg_list = str(sys.argv)
    db_name = sys.argv[1]
    #print(db_name)

    # Establish connection
    conn = db_funcs.connect_db("testdb", "psql_user", "password", "127.0.0.1", "5432")

    # Create cursor object
    cursor = conn.cursor()

    # Check if db exists
    #result = db_funcs.db_exists(conn, 'testdb2')
    result = db_funcs.db_exists(conn, db_name)
    print(result)

    if result == 'False':
        #print("Creating db")
        # Prep query to create db
        sql = 'CREATE database ' + db_name;
        #print(sql)

        # Run query
        cursor.execute(sql)

    # Close connection
    conn.close()

    print ("Complete")

# Call main
if __name__ == "__main__":
    main()
