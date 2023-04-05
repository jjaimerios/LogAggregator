#!/usr/bin/python3

# Import postgresql library
import psycopg2

#-----------------------------------------------------------
# Create DB Connection
#-----------------------------------------------------------
def connect_db(dbname, usernm, passwd, hostnm, portnum):

    try:
        #print("Connecting to db name " + dbname)

        # Establish connection
        conn = psycopg2.connect(database=dbname, user=usernm, password=passwd, host=hostnm, port=portnum)

        # Set auto-commit
        conn.autocommit = True
    except psycopg2.Error as e:
        print("ERROR in connect_db: " + e)

    # Return db connection object
    return conn
#----------------------- Function END ----------------------
#-----------------------------------------------------------

#-----------------------------------------------------------
# Check if database exists
#-----------------------------------------------------------
def db_exists(con, db_str):

    exists = False
    try:
        #print("db name " + table_str)

        cur = con.cursor()
        cur.execute("select exists(SELECT datname FROM pg_catalog.pg_database WHERE datname='" + db_str + "')")
        exists = cur.fetchone()[0]
        cur.close()
    except psycopg2.Error as e:
        print("ERROR in table_exists: " + e)

    return exists
#----------------------- Function END ----------------------
#-----------------------------------------------------------

#-----------------------------------------------------------
# Check if table exists (pass in connection)
#-----------------------------------------------------------
def table_exists(con, table_str):

    exists = False
    try:
        #print("db name " + table_str)

        cur = con.cursor()
        cur.execute("select exists(select relname from pg_class where relname='" + table_str + "')")
        exists = cur.fetchone()[0]
        cur.close()
    except psycopg2.Error as e:
        print("ERROR in table_exists: " + e)

    return exists
#----------------------- Function END ----------------------
#-----------------------------------------------------------

#-----------------------------------------------------------
# Check if table exists (pass in cursor)
#-----------------------------------------------------------
def table_exists2(cur, table_str):

    exists = False
    try:
        print("db name " + table_str)
        cur.execute("select exists(select relname from pg_class where relname='" + table_str + "')")
        exists = cur.fetchone()[0]
        #cur.close()
    except psycopg2.Error as e:
        print("ERROR")
        print(e)
    return exists
#----------------------- Function END ----------------------
#-----------------------------------------------------------

#-----------------------------------------------------------
# Display table column names
#-----------------------------------------------------------
def get_col_names(con, table_str):

    col_names = []
    try:
        cur = con.cursor()
        cur.execute("select * from " + table_str + " LIMIT 0")
        for desc in cur.description:
            col_names.append(desc[0])
        cur.close()
    except psycopg2.Error as e:
        print("ERROR in get_col_names: " + e)

    return col_names
#----------------------- Function END ----------------------
#-----------------------------------------------------------
