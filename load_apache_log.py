#!/usr/bin/python3

# Script to read in log file and write it to a SQL database table.
#
# Log line:
# ['[Sun', 'Dec', '04', '04:47:44', '2005]', '[notice]', 'workerEnv.init()', 'ok', '/etc/httpd/conf/workers2.properties']
# ------------------------------------------------------------------------

# Import regex package
import re

# Call custom db functions module
import db_funcs

# ------------------------------------------------------------------------

# Convert 3 character month abbreviation to number
def getNumericMonth(alphaMonth):
    numMonth = ''

    if alphaMonth == 'Jan':
        numMonth = '01'
    elif alphaMonth == 'Feb':
        numMonth = '02'
    elif alphaMonth == 'Mar':
        numMonth = '03'
    elif alphaMonth == 'Apr':
        numMonth = '04'
    elif alphaMonth == 'May':
        numMonth = '05'
    elif alphaMonth == 'Jun':
        numMonth = '06'
    elif alphaMonth == 'Jul':
        numMonth = '07'
    elif alphaMonth == 'Aug':
        numMonth = '08'
    elif alphaMonth == 'Sep':
        numMonth = '09'
    elif alphaMonth == 'Oct':
        numMonth = '10'
    elif alphaMonth == 'Nov':
        numMonth = '11'
    elif alphaMonth == 'Dec':
        numMonth = '12'
    else:
        numMonth = ''

    return numMonth

# Format date / time
# Input Example: Sun Dec 04 04:47:44 2005
# Output Example: 2005-12-04 04:47:44
def formatDateTime(recDate):
    time_date = re.split(' ', recDate)[4].replace("]","") + "-" +\
    getNumericMonth(re.split(' ', recDate)[1]) + "-" +\
    re.split(' ', recDate)[2] + " " +\
    re.split(' ', recDate)[3]

    return time_date

# Insert record into database
def insertTableRec(db_conn, recnum, timedate, weekday, msgtype, msg):
    # Create cursor object
    cursor = db_conn.cursor()

    # Prep query to create db
    sql = "INSERT INTO test_tbl3 (ID,APP,REC_DATE,DAY,EVENT,MESSAGE) \
           VALUES ( " + str(recnum) + \
           ",'Apache',\'" + \
           timedate + "\',\'" + \
           weekday + "\',\'" + \
           msgtype + "\',\'" + \
           msg + "\');"

    print(sql)

    # BETTER?
    # cur.execute("INSERT INTO users VALUES (%s, %s, %s, %s)", (10, 'hello@dataquest.io', 'Some Name', '123 Fake St.')) conn.commit()

    # Run query
    cursor.execute(sql)

# Main function
def main():
    # Establish database connection
    conn = db_funcs.connect_db("testdb", "psql_user", "password", "127.0.0.1", "5432")

    # Open a file handle
    f = open("/mnt/documents/Regis/Final2/SampleLogs/Apache_2k.log.txt", "r")

    # Set record number
    rec_num = 0

    while True:
        # Print one line
        one_line = f.readline()

        # Leave loop if no more records
        if not one_line:
            break

        # Increment record number
        rec_num += 1
        # Find any values enclosed in brackets
        in_brackets = re.findall(r'\[.*?\]', str(one_line))
        rec_date = in_brackets[0]

        # Remove brackets from message type
        msg_type = re.sub('[\]\[]', '', in_brackets[1])

        # https://stackoverflow.com/questions/17284947/regex-to-get-all-text-outside-of-brackets
        # Find elements outside any brackets
        message = re.findall(r'([^[\]]+)(?:$|\[)', one_line)[1].strip()
        #message = re.sub('[\)\(]', '', message1)

        # Get time/date
        time_date = formatDateTime(rec_date)

        # Get day of week
        day = re.split(' ', rec_date)[0].replace("[","")

        # Insert record into database table
        insertTableRec(conn, rec_num, time_date, day, msg_type, message)

    # Close file handle
    f.close()

    # Close database connection
    conn.close()

    print("SUCCESS")

# Call main
if __name__ == "__main__":
    main()
