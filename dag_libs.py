#!/usr/bin/python3

# OS commands for parsing file/path
import os

# Import application properties library and set config file variables
import configparser
config = configparser.RawConfigParser()
config.read('/home/jrios/airflow/etc/config.props') # Property file with app variables

def say_hello():
    print("Hello")

def process_exception(arg, log_File_name):
    # Open log file
    af_log_dir = config.get('Log_Files', 'airflow_log_dir')
    f = open(af_log_dir + log_file_name, "a")

    # Write to log file
    f.write(str(arg) + "\n")

    # Close log file
    f.close()

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

