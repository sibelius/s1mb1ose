# Simbiose Challenge

task: sync elasticsearch (es) and cassandra (cs) databases using a python daemon

# How to use
You should create a virtualenv
Then install the requirements (pip install -r requirements.txt)

# Simulation of data insertion
./simulate.py
new data (y/n), clear data (c): 

This script is used to simulate data insertion into es and cs.
it will generate a random data to be inserted into a document or collumnfamily into a index or keyspace.
it will randomly choose three options:
- insert only into es
- insert only into cs
- insert into both
    - first es then cs
    - first cs then es

# The Daemon
./simbiose.py or ./simbiose.py &

This script reads the config.json and sleep per "period" seconds of time defined in that file.

if you run in foreground you can see what the daemon is doing, for instance:

> es ==> cs syncing...
> es_to_cs_insert: 14cb6bcd-1950-4526-879d-9bb20b50f9cc
> finished!
