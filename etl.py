# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv

# checking current working directory
print(os.getcwd())
# Get folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
# initiating an empty list of rows that will be generated from each file
    full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
# extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line)

# creating event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

# check number of rows in csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))

from cassandra.cluster import Cluster
try: 
    cluster = Cluster(['127.0.0.1'])
    
# Connection and begin executing queries, need a session
    session = cluster.connect()
except Exception as e:
    print(e)
    
# Create a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)
    
# Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

## Query 1:  First we need create the table 
query = "CREATE TABLE IF NOT EXISTS song_length " 
query = query + "(sessionId int, itemInSession int, artist text, song text, length float, PRIMARY KEY(sessionId, itemInSession))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# Now We have provided part of the code to set up the CSV file and read CSV for insert data
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## INSERT statements into the song_length
        query = "INSERT INTO song_length (sessionId, itemInSession, artist, song, length)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, ( int(line[8]), int(line[3]), line[0], line[9], float(line[5])))

## check results        
query = "SELECT * FROM song_length WHERE sessionId = 338 and itemInSession = 4"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)   
for row in rows:
    print (row.artist, row.song, row.length)

    
## Query 2:  First we need create the table 
query = "CREATE TABLE IF NOT EXISTS name_song_user " 
query = query + "(userId int, sessionId int, itemInSession int, artist text, song text, firstName text, lastName text, PRIMARY KEY((userId, sessionId), itemInSession))"
try:
    session.execute(query)
except Exception as e:
    print(e)
# Read CSV and insert data
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## INSERT statements into the name_song_user
        query = "INSERT INTO name_song_user (userId, sessionId, itemInSession, artist, song, firstName, lastName)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, ( int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))
        
## check results 
query = "SELECT * FROM name_song_user WHERE userId = 10 AND sessionId = 182"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)   
for row in rows:
    print (row.artist, row.song, row.firstname, row.lastname)
    
## Query 3:  First we need create the table 
query = "CREATE TABLE IF NOT EXISTS first_last_name " 
query = query + "(song text, userId int, firstName text, lastName text, PRIMARY KEY(song, userId))"
try:
    session.execute(query)
except Exception as e:
    print(e)
    
# Read CSV and insert data
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## INSERT statements into the `query` variable
        query = "INSERT INTO first_last_name (song, userId, firstName, lastName)"
        query = query + "VALUES (%s, %s, %s, %s)"
        session.execute(query, ( line[9], int(line[10]), line[1], line[4]))

## check results         
query = "SELECT * FROM first_last_name WHERE song = 'All Hands Against His Own' "
try:
    rows = session.execute(query)
except Exception as e:
    print(e)   
for row in rows:
    print (row.firstname, row.lastname)

#Close session and cluster
session.shutdown()
cluster.shutdown()