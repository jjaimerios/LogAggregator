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
        #print("db name " + table_str)
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
# Create log table
#-----------------------------------------------------------
def create_log_table(con, table_name):

    try:
        cursor = con.cursor()

        # Prep query to create table
        sql = "CREATE TABLE " + table_name + \
              "(ID INT PRIMARY KEY     NOT NULL, " \
              "APP           TEXT    NOT NULL, " \
              "REC_DATE      DATE    NOT NULL, " \
              "DAY           TEXT    NOT NULL, " \
              "EVENT         TEXT     NOT NULL, " \
              "APP_REC       INT      NOT NULL, " \
              "COMPONENT     TEXT     NOT NULL, " \
              "MESSAGE       CHAR(250));"
        #$print(sql)

        # Run query
        cursor.execute(sql)

    except psycopg2.Error as e:
        print("ERROR in create_log_table: " + e)

    return
#----------------------- Function END ----------------------
#-----------------------------------------------------------

#-----------------------------------------------------------
# Remove table
#-----------------------------------------------------------
def drop_table(con, table_name):

    try:
        cursor = con.cursor()

        # Prep query to create table
        sql = "DROP TABLE " + table_name + ";"
        #$print(sql)

        # Run query
        cursor.execute(sql)

    except psycopg2.Error as e:
        print("ERROR in drop_table: " + e)

    return
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

#-----------------------------------------------------------
# Get max value from given field
#-----------------------------------------------------------
def get_max_val(con, table_str, field_name):

    max_id = 0
    try:
        cur = con.cursor()

        # Prep query to get max ID
        sql = "SELECT max(" + field_name + ") FROM " + table_str + ";"

        # Execute query
        cur.execute(sql)

        # Get query results
        max_id = cur.fetchone()[0]

        # Close cursor
        cur.close()
    except psycopg2.Error as e:
        print("ERROR in get_max_val: " + e)

    return max_id
#----------------------- Function END ----------------------
#-----------------------------------------------------------

#-----------------------------------------------------------
# Get max recnum for a given app log
#-----------------------------------------------------------
def get_max_recnum(con, table_str, field_name, app_name):

    max_id = 0
    try:
        cur = con.cursor()

        # Prep query to get max ID
        sql = "SELECT max(" + field_name + ") FROM " + table_str + \
                " where app = '" + app_name + "';"

        # Execute query
        cur.execute(sql)

        # Get query results
        max_id = cur.fetchone()[0]

        # Close cursor
        cur.close()
    except psycopg2.Error as e:
        print("ERROR in get_max_val: " + e)

    return max_id
#----------------------- Function END ----------------------
#-----------------------------------------------------------

#-----------------------------------------------------------
# Insert record into logs table
#-----------------------------------------------------------
def insertTableRec(db_conn, recnum, appname, timedate, weekday, msgtype, app_rec_num, component, msg, tblnm):
    try:
        # Create cursor object
        cursor = db_conn.cursor()

        # Prep query to create db
        sql = "INSERT INTO " + tblnm + " (ID,APP,REC_DATE,DAY,EVENT,APP_REC,COMPONENT,MESSAGE) \
               VALUES ( " + str(recnum) + ",\'" + \
               appname + "\',\'" + \
               timedate + "\',\'" + \
               weekday + "\',\'" + \
               msgtype + "\',\'" + \
               str(app_rec_num) + "\',\'" + \
               component + "\',\'" + \
               msg + "\');"

        #print(sql)

        # BETTER?
        # cur.execute("INSERT INTO users VALUES (%s, %s, %s, %s)", (10, 'hello@dataquest.io', 'Some Name', '123 Fake St.')) conn.commit()

        # Run query (insert record)
        cursor.execute(sql)

    except psycopg2.Error as e:
        print("ERROR in insertTableRec: " + e)

#----------------------- Function END ----------------------
#-----------------------------------------------------------
