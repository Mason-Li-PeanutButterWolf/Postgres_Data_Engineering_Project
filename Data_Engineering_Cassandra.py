# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv

# Make connection to the local cluster.

from cassandra.cluster import Cluster
cluster = Cluster(['127.0.0.1'])

# Create a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS songplayevent
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)

# To establish connection and begin executing queries, need a session
session = cluster.connect()

# Create a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS songplayevent
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


#Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('songplayevent')
except Exception as e:
    print(e)


# Data Modeling for :  SELECT artist, songTitle, songLength, sessionId, itemInSession  FROM session_info WHERE sessionId = 338 AND itemInSession = 4

'''
The table session_info is created to collect key information related to each session that the app users created.
Given the qury includes 'WHERE' clause that filters by sessionId and itemInSession, sessionId is chosen to be the
partition key and itemInSession becomes the clustering column. Together, they make up the primary key for the table
that uniquely identifies each row.
'''


query = "CREATE TABLE IF NOT EXISTS session_info "
query = query + "(sessionId int, itemInSession int, artist text, songTitle text, songLength decimal, PRIMARY KEY (sessionId, itemInSession))"
try:
    session.execute(query)
except Exception as e:
    print(e)    

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:

        query = "INSERT INTO session_info (sessionId, itemInSession, artist, songTitle, songLength)"
        query = query + " VALUES (%s, %s, %s, %s, %s)"
        
        session.execute(query, (int(line[8]),int(line[3]),line[0],line[9],float(line[5])))

# SELECT statement to verify the data was entered into the table

query_sessionInfo = '''SELECT artist, songTitle, songLength, sessionId, itemInSession  FROM session_info WHERE sessionId = 338 AND itemInSession = 4'''
try:
    rows = session.execute(query_sessionInfo)
except Exception as e:
    print(e)
    
for row in rows:
    print(row)

## Data Modeling for : SELECT artist, songTitle, userFirstName, userLastName FROM user_info WHERE userId = 10 AND sessionId = 182

'''
The table user_info is created to collect key information related to each user of the app. Given the qury includes 
'WHERE' clause that filters by userId and sessionId, userId is chosen to be the partition key and sessionId and 
itemInSession become the clustering column (to sort by itemInSession). Together, they make up the primary key for
the table that uniquely identifies each row.
'''

query = "CREATE TABLE IF NOT EXISTS user_info "
query = query + "( userId int, userFirstName text, userLastName text, artist text, songTitle text, sessionId int, itemInSession int, PRIMARY KEY (userId, sessionId, itemInSession))"
try:
    session.execute(query)
except Exception as e:
    print(e)   

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:

        query = "INSERT INTO user_info ( userId, userFirstName, userLastName, artist, songTitle, sessionId, itemInSession)"
        query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"

        session.execute(query, (int(line[10]),line[1],line[4],line[0],line[9],int(line[8]),int(line[3])))

query_userInfo = '''SELECT artist, songTitle, userFirstName, userLastName FROM user_info WHERE userId = 10 AND sessionId = 182'''
try:
    rows = session.execute(query_userInfo)
except Exception as e:
    print(e)
    
for row in rows:
    print(row)

## Data Model for query: SELECT songTitle, userFirstName, userLastName FROM user_by_song WHERE songTitle = 'All Hands Against His Own'

'''
The table user_by_song is created to organize user's listening history by song tittle. Given the qury includes 
'WHERE' clause that filters by songTitle only, and songTitle is chosen to be the partition key and userFirstName and 
userLastName become the clustering column. Together, they make up the primary key for the table that uniquely identifies each row.
'''

query = "CREATE TABLE IF NOT EXISTS user_by_song "
query = query + "(songTitle text, userFirstName text, userLastName text, PRIMARY KEY (songTitle, userFirstName, userLastName))"
try:
    session.execute(query)
except Exception as e:
    print(e)                      


file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:

        query = "INSERT INTO user_by_song (songTitle, userFirstName, userLastName)"
        query = query + " VALUES (%s, %s, %s)"
        session.execute(query, (line[9],line[1],line[4]))

query_userInfo = '''SELECT songTitle, userFirstName, userLastName FROM user_by_song WHERE songTitle = 'All Hands Against His Own' '''
try:
    rows = session.execute(query_userInfo)
except Exception as e:
    print(e)
    
for row in rows:
    print(row)

query = "drop table session_info"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "drop table user_info"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

query = "drop table user_by_song"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


session.shutdown()
cluster.shutdown()