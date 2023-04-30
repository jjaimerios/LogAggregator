#!/bin/bash

# Run scripts to load log data to db

echo "Start"
echo `date`

echo "Loading Apache Log Data"
python3 load_apache_log.py

echo "Loading Hadoop Log Data"
python3 load_hadoop_log.py

echo "Loading HDFS Log Data"
python3 load_hdfs_log.py

echo "Loading Linux Log Data"
python3 load_linux_log.py

echo "Loading Windows Log Data"
python3 load_windows_log.py

echo `date`
echo "Done"
