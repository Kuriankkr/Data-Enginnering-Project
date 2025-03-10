###  Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. This project thus requires script setting up a Data Warehouse on a Redshift cluster. 

During this project a database and its relevant tables were setup. Before the final tables are setup the data is shifted into intermediate staging tables. From which data is moved to the final redshift database.

![Udacity_TablesJPG](https://github.com/Kuriankkr/Udacity-Nanodegree-Data-Engineering/blob/master/Cloud%20Data%20Warehouses/Project%20Data%20Warehouse/Udacity_TablesJPG.JPG)

## Datasets

### Log Dataset

{"artist":"Pavement", "auth":"Logged In", "firstName":"Sylvie", "gender", "F", "itemInSession":0, "lastName":"Cruz", "length":99.16036, "level":"free", "location":"Klamath Falls, OR", "method":"PUT", "page":"NextSong", "registration":"1.541078e+12", "sessionId":345, "song":"Mercy:The Laundromat", "status":200, "ts":1541990258796, "userAgent":"Mozilla/5.0(Macintosh; Intel Mac OS X 10_9_4...)", "userId":10}

### Song Dataset

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}


### Staging Tables

#### staging_songs
num_songs ,artist_id,artist_latitude,artist_longitude,artist_location,artist_name,song_id,title,duration,year

#### staging_events
event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, 
registration, sessionId, song, status, ts, userAgent, userId

### Fact and Dimensions table

#### Fact table : songplays
songplay_id,start_time,user_id,level,song_id,artist_id,session_id,location,user_agent

#### Dimension Tables: users, songs, artists, time

- users: user_id, first_name,last_name,gender, level
- song: song_id,title,artist_id,year,duration
- artists: artist_id,name,location, latitude, longitude
- time_table: start_time,hour,day,week,month,year,weekday


## Project pipeline
This project include 5 scripts. The following are the steps to complete the project
- dwh.cgf: Fill in the details of the dwh.cfg 
- create_cluster.py: This script creates the Redshift cluster
- create_tables.py: This script drop the tables if any existed and creates the tables mentioned above
- sql_queries.py: This has all the queries for creation,insertion and dropping of tables
- test.py: This is to test of if the data has been loaded correctly

## Steps
- Create the tables for both staging and fact and dimensions
- Load the data from S3 buckets (json filees for song and log data) to staging tables and from there to to the fact and dimensionss
- Delete the cluster

