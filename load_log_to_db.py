#!/usr/bin/python3

# Script to load log data to db
# ------------------------------------------------------------------------

# Call custom db functions module
import db_funcs

# ------------------------------------------------------------------------

# Main function
def main():
    # Establish connection
    conn = db_funcs.connect_db("testdb", "psql_user", "password", "127.0.0.1", "5432")

    # Create cursor object
    cursor = conn.cursor()

    # Log line:
    # ['[Sun', 'Dec', '04', '04:47:44', '2005]', '[notice]', 'workerEnv.init()', 'ok', '/etc/httpd/conf/workers2.properties']

    #cur.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
    #  VALUES (1, 'Paul', 32, 'California', 20000.00 )");

    # Prep query to create db
    sql = """INSERT INTO test_tbl1 (ID,APP,DATE,EVENT,MESSAGE) 
           VALUES (1,'Apache','Sun Apr 01 2023', 'NOTICE', 'Test Message');"""
    #print(sql)

    # Run query
    cursor.execute(sql)

    conn.close()

    print ("Complete")

# Call main
if __name__ == "__main__":
    main()
