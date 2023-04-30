# LogAggregator

Practicum Project for Regis MSDS696

Please see jrios_msde696_de_final.docx in this repo for a full write up and description of this project.

High level - the goal of this project was to create an ETL process for aggregating log data. These scripts were created to take in log data from different applications and to load that data to a common table where it could be queried and visualized.

The log data used was sourced from the Loghub project (https://github.com/logpai/loghub).

Here is a list of the scripts in this repo with a quick description of what each is:

- config.props – property/config file with shared variable names
- create_db.py – creates a new PostgreSQL db; takes db name as a command line argument
- create_log_table.py – creates log table if it does not exist; gets log name from config file
- drop_log_table.py – drops log table; came in very handy for deleting and rebuilding table
- db_funcs.py – database functions that other scripts could call
- dag_libs.py – a few functions for other scripts to call
- cron_log_runs.sh – called by cron; calls all the below load scripts
- load_apache_log.py – loads Apache log data to PostgreSQL db table
- load_hadoop_log.py – loads Hadoop log data to PostgreSQL db table
- load_hdfs_log.py – loads HDFS log data to PostgreSQL db table
- load_linux_log.py – loads Linux log data to PostgreSQL db table
- load_windows_log.py – loads Windows log data to PostgreSQL db table

Again - please see jrios_msde696_de_final.docx for a much more thorough write up of this project.
